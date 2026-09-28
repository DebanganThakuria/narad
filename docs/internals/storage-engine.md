# Storage Engine

Every partition is a directory owned by exactly one node. The engine underneath is a compressed, CRC-checked, segmented log with a visibility watermark and a retention reaper.

## On disk

```
topics/orders/
├── incarnation                  ← the topic incarnation this directory belongs to
└── p00003/
    ├── 00000000000000000000.log     ← sealed segment (starts at offset 0)
    ├── 00000000000000450832.log     ← active segment (starts at offset 450832)
    ├── hwm                          ← 8-byte high-watermark written at close (emptied by the first commit after an open)
    ├── consumer.offset              ← 8-byte committed consumer frontier
    └── consumer.ahead               ← offsets acked out of order above it, plus the frontier (two checksummed 4 KiB slots)
topics/orders.stale-3f9a1c0e7b2d4a61/   ← quarantined: a deleted incarnation's leftover
```

Everything under the data directory that the engine creates is private to the broker's user: directories `0700`, files `0600` (segments, `hwm`, `consumer.offset`, fan-out cursors, the incarnation marker, transferred segments, and the ingress WAL's directory and segments). Segments carry every message payload, so they get the same protection `fsm.db` (password hashes) already had. Modes are applied at creation only; a file or directory created by an older binary keeps the mode it was created with, so tighten those by hand if the host is shared.

### The incarnation marker

Topic directories are keyed by name, and a name outlives the topic: delete `orders`, recreate `orders`, and the new topic's partition logs open exactly where the old one's segments, high-watermark and consumer offset sit on any node that missed the purge (it was down, or the purge lost the race with the recreate). Served as-is, the recreated topic would hand consumers the deleted topic's messages and append new produce after them.

So every topic record carries an **incarnation id** (`id`, 16 hex characters, minted by the proposer at create time and preserved by every update), and every topic directory a marker file, `topics/<name>/incarnation`, holding the id of the incarnation it belongs to. The marker is stamped the first time a node opens a partition log for the topic, or installs a moved partition, and every open compares it with the metastore record before serving the directory:

- **match**: served;
- **no marker**: the directory predates markers and is adopted (stamped with the current id), the one-time upgrade path; a record without an id keeps name-based bookkeeping and stamps nothing;
- **different marker**: the directory is a deleted incarnation's leftover. Any open logs under the name are closed, the whole directory is renamed to `topics/<name>.stale-<oldid>` (quarantined, never served), an error is logged with both ids, and a fresh directory is opened for the current id.

An already-open log is re-checked whenever the topic's metadata version moves, so a delete plus recreate applied while the log was open retires it rather than serving the old data. The transfer listing a move source serves reads the directory without opening a log, so it performs the same comparison itself and refuses a stale directory.

A quarantined directory is reclaimed only after the **leader** confirms that incarnation is gone (the record is absent, or carries a different id): by the purge for that incarnation if it still arrives, by the startup orphan sweep, or by the periodic sweep the move runner performs. Older binaries never read the marker and ignore the file.

```mermaid
flowchart LR
    subgraph segment file
        F1["frame: header + CRC<br/>zstd(records 0..k)"]
        F2["frame: header + CRC<br/>zstd(records k+1..m)"]
        F3[...]
    end
    F1 --- F2 --- F3
```

- **Segments** are capped at 64 MiB. A frame that pushes the active segment past the cap marks it full and fsyncs it; the roll itself (a new segment named by its first offset) happens at the end of the commit that filled it, at Close, or right before the next frame write, whichever comes first, and a segment also rolls before the first write that finds its oldest record older than the retention roll age (see Retention). Sealed segments are immutable: the unit of retention deletion.
- **Frames** are the write unit: all records drained in one flush become one frame: length-prefixed records, compressed together (zstd by default), CRC over the stored bytes. Frame size therefore tracks batch size: trickle traffic gives per-record frames (~40% compression on JSON-ish payloads); busy traffic gives multi-hundred-record frames (~95%+, since similar records compress against each other).
- **Records** carry the keyed envelope: `[version][key][commit-time][payload]`. Commit time is stamped when the envelope is built, just before the commit takes the partition's produce lock, and under that lock it is raised to the newest commit time the partition has seen if it is lower (a batch that lost the race for the lock, or a wall-clock step back). So it never decreases along a partition, the property the [delay gate](fanout-engine.md) relies on. The floor is kept in memory, so it starts over after a restart or after the topic is forgotten on delete.

## Write path: buffer → flush → sync

Appends go into an in-memory buffer; a flusher goroutine drains it into frames and writes them out; fsync policy is configurable (per-write or batched). The **commit path bypasses the leniency**: `Log.CommitDurable` forces drain + fsync synchronously on the flusher goroutine, re-reads and CRC-verifies the new frames, and only then advances the high-watermark in memory. That is one fsync per commit: the boundary itself is not written to disk on the way (see [below](#the-high-watermark-and-the-hidden-tail) for why a restart still finds it). Buffered data lost in a crash was, by construction, never acked to anyone.

Records drained out of the buffer sit in a **flushing snapshot** until an fsync proves their frame durable; a failed segment write is retried from the unwritten suffix on the next drain, and the in-memory copy is released only after the sync. On the commit path the sync is part of the same drain, so the snapshot never outlives a commit.

### When a commit fails

A commit can fail at the write (`ENOSPC`, `EIO`), the fsync, the CRC read-back, the segment roll, or the release of the `hwm` file (below), which each commit attempts until it has succeeded once since the log was opened. The records are acked to the producer only by the ingress WAL, which re-commits any batch whose `CommitDurable` did not return success by appending the same records again. So a failed commit must leave **nothing** of the batch behind: if the first copy stayed in the log (in the snapshot, or already written and fsynced), the retry would append a second copy at fresh offsets and its commit would advance the high-watermark past both, delivering every record of the batch twice, permanently, without any crash.

Before the error reaches the caller, the flusher discards the uncommitted tail: everything above the high-watermark is dropped from the buffer and the snapshot, the active segment is truncated back to the first frame at or above the high-watermark (and the truncate fsynced), the sparse index and the caches forget the cut frames, and the next append is assigned the offset the failed batch had. The retry lands exactly one copy at the same offsets. The lazy roll above is what makes this always possible: a commit's frames are never sealed into an immutable segment before the commit has returned.

### When fsync fails

An fsync failure is final for the log. After a failed `fdatasync` the kernel may already have dropped the dirty pages (Linux marks them clean and reports the error once), so a second fsync that "succeeds" proves nothing about the bytes that failed, and the tail of the segment past the last good sync is of unknown content. This is the PostgreSQL fsyncgate lesson: the only sound recovery is the one a crash would get. Narad does not crash the node (other partitions on it are fine), but it treats the log as crashed: the error is **latched** (`storage.ErrLogPoisoned`, wrapping the original error), every later append and commit on that log fails with it, the failure is logged at error level and counted (`errors_total{component="storage",kind="fsync_poisoned"}`), and the state clears only when the log is reopened and its files rescanned. Reads of committed records keep working. Nothing acked is lost: records above the last durable tail are still in the ingress WAL, which reroutes them to a sibling partition after a few failed passes and re-commits them here after the reopen.

## The high-watermark and the hidden tail

The **HWM** is the exclusive bound of what consumers may see. An open log keeps it in memory; the `hwm` file holds a boundary only while the log is **closed**:

- **Close writes it.** Once the flusher has stopped, `Close` writes the exact HWM (8 bytes overwritten in place, a single-sector atomic write + fdatasync) unless the file already holds it. Readers that answer without opening the log (the topic describe, the consume pump's backlog estimate, fan-out reads of a closed parent, the partition transfer listing, the metrics poller) read that value, and for a cleanly closed log it is exact.
- **Open trusts it, clamped.** An 8-byte file recovers as `min(file, record tail)`. Opening writes nothing.
- **The first advance empties it.** Before the first HWM advance of a log's life (inside the first commit after the log was opened) the file is truncated to empty and fsynced, once. No later commit touches it. An empty or missing file tells recovery to take the boundary from the CRC-verified record tail.

The record tail never hides a visible record, because a commit fsyncs and verifies its frames before it advances the boundary. It can hold more than the committed records, though: an open reads it through the page cache, so after a process crash it includes frames the dead process wrote but never saw fsynced. An open that takes the boundary from the tail therefore fsyncs the active segment first (sealed segments were fsynced when they rolled) and fails if that fsync fails, so nothing it exposes can be taken back by a later power loss; that is one fsync per such open, never one on the commit path. So a crash while the log is open leaves an empty file and every committed record visible, and a commit pays one fsync (the segment) where it used to pay a second, serial one for the boundary file under the produce lock. Emptying the file rather than letting it lag the commits is what keeps a rollback safe: releases before this one persisted the file on every commit, and recover an 8-byte file as `min(file, tail)` and an empty one as the tail. A file that lagged the acked commits would make such a binary, started after a crash, hide acked records, and its failed-commit discard would then truncate them.

After a crash the file stays empty until the log is opened and closed again, and a reader of the closed partition finds no boundary on disk. Startup opens every partition the node owns before it reports ready (one whose assignment lookup or open fails at that point is skipped; up to min(8, GOMAXPROCS) topics open at once, each topic's partitions one after another), so in practice every owned partition is open until idle eviction closes it and writes the exact boundary.

Records can sit above the HWM, which is a deliberate artifact:

```mermaid
flowchart LR
    A["offsets 0 .. H-1<br/>visible"] --> B["offsets H .. T-1<br/>hidden tail"]
    B --> C["offset T = next append"]
```

They were written but never exposed: a crash landed after a commit wrote them and before it advanced the boundary (before or after its fsync), or a failed commit could not truncate them (the log is then poisoned, see above). For produce-path records the ingress WAL still owns them and **re-commits them at fresh offsets** (its checkpoint never passes a batch whose commit did not return), so the hidden copy is a duplicate in waiting. A clean `Close` writes the boundary below them, so they stay hidden across a clean restart until a new commit appends past them and advances the HWM over them. After a crash the empty file exposes them at reopen instead, once the open has fsynced them. Either way the outcome is duplicates, never loss, and only around crashes and poisoned logs.

## Recovery

Opening a log reads only its **active** (last) segment. That segment is walked frame by frame, CRC included, and indexed:

- A **torn tail** in the active segment (crash mid-write, including a crash in the middle of a commit's frame write) is truncated and the truncate fsynced; those bytes were never acked by this log, and the ingress WAL re-commits the batch at the offset it had. A last frame whose bytes are all present but do not check out (a zero-filled or scrambled final sector: the file size reached the disk, the data did not) is a torn tail too, as long as no valid frame follows it; left in place, its intact-looking header would shadow the frames the next commits write at the same offsets.
- **Mid-file corruption** under valid later frames is *not* truncated (that would destroy acked data and regress offsets): the walk resyncs to the next frame that passes its CRC and carries on, and the bad frame's offsets become a permanent gap.

A **sealed** segment is opened only to stat it, and its handle is released at once. Its range ends at its successor's base offset, taken from the file name, and that is exact rather than an estimate: a roll fsyncs the active segment before it creates the next one, named by the offset the sealed one ended at, and nothing writes to a sealed segment again. So an open costs O(active segment), at most about one 64 MiB segment, instead of O(retained bytes): on 256 MiB of sealed 4 KiB frames an open went from about 94 ms and 287 MiB allocated to about 0.5 ms and 13 KiB (macOS, warm page cache). The active walk streams each frame through one reused buffer rather than a frame-sized allocation per frame, about 20% faster (a 60 MiB active segment of 4 KiB frames: 22.4 ms to 17.9 ms).

That moves where damage in a sealed segment is found: **at the read that lands on it, never at open**, and it is never served. Every read CRC-checks the frame it serves, so a record in a bad sealed frame reads as corrupt or not found: a queue consume skips it with `narad_consumer_corrupt_skipped_total` and a warning (`skipped permanently-unreadable record`), a replay answers `410`, and a fan-out cursor skips it and counts it in `narad_fanout_child_dropped_messages`. The open never did anything more with it: the old full scan resynced past a bad sealed frame in silence. An I/O error in a sealed segment fails the reads that need it (a queue consume that reaches such an offset gives the record back and answers `500`) instead of failing the whole partition open. The bound assumes every sealed segment's frames end at or below its successor's base, which every roll this code has written keeps; frames of a hand-edited or misnamed file past that point would be unreachable.

### What a lying fsync costs

Every durability decision above assumes `fdatasync` tells the truth. A drive (or a virtualised disk) that acknowledges a flush it never performed can lose, at a power cut, any record whose commit was acked on the strength of the lie: the frames are gone (the segment sync lied), or the sync that empties the `hwm` file before a log's first advance lied, so the previous `Close`'s boundary is still on disk and the records above it sit in the hidden tail until a later commit advances the boundary over them. Commits never sync that file otherwise, so the second exposure is once per log open, not once per commit. That is the hardware's loss, not the engine's, and it is bounded to exactly those records. A sealed segment whose last sync lied keeps its range up to its successor's base, so its lost offsets read as not found or corrupt and are skipped as recorded loss, which is what happened to them before too. What must never happen, and what the disk-fault tests in `storage` and `wal` pin under a lying-fsync simulation (`fault_test.go`, driven by the `syncfile` fault seam that fails or skips a chosen syscall for tests only): a torn or fabricated record served, a crash loop at open, a sparse index that disagrees with the file, a record from a fully honest commit missing, or a high-watermark that grew past what the crash left.

## Retention

One process-wide reaper loop (a single goroutine, not one per partition) sweeps every open partition log with an age bound about once a minute and deletes **sealed segments whose last write is older than the topic's `retention_ms`**. Granularity is the segment, and two rules keep a segment from outliving retention on a partition that never fills one:

- **Time-based roll.** The flusher rolls the active segment before the first write that finds the segment's oldest record older than the roll age (`RetentionConfig.MaxSegmentAge`, which defaults to `MaxAge`). A partition writing 1 MiB a day with a 7-day retention used to keep its oldest records for months (until the 64 MiB segment filled, then one more retention period); now the segment is sealed after at most one retention period of writes.
- **Rotation of an idle active segment.** A partition that stops writing never triggers the roll, so the reaper asks the flusher to seal an active segment whose last write is older than `MaxAge` (every record in it has expired) and deletes it in the same sweep. Only a fully committed segment is rotated; one that still holds records above the high-watermark waits for the ingress WAL to re-commit them first, so the failed-commit discard above always finds them in the active segment.

Together these bound a record's lifetime by `MaxSegmentAge + MaxAge + CheckInterval`, so with the defaults a record is gone within about twice the retention age of its write. For a segment recovered from disk the roll age counts from the file's mtime (its last write), the only write time the inode keeps, so such a segment can live up to one write-span longer.

The reaper's bookkeeping is cheap: every segment caches its first and last write time (the reaper never stats a file per sweep), a sweep detaches expired segments under the write lock but unlinks the files after releasing it (a restart with a backlog of expired segments no longer stalls the partition's readers and flusher for the whole batch of unlinks), and a failed unlink is logged and counted (`errors_total{component="storage",kind="retention_unlink"}`) rather than silently dropped while the file keeps consuming disk. Sealed segments do not pin a file descriptor: recovery opens a sealed segment only to stat it and releases the handle, the first read reopens it lazily, and the handle goes away with the segment's sparse index when it leaves the two-segment hot set, so a partition with days of history holds a handful of descriptors, not one per segment. Deletions export bytes/messages counters (`reason="age"`).

Two things keep retention running when the happy path does not. The shared loop is supervised: a sweep that panics is logged and skipped for that partition rather than taking retention down for every partition on the node, and a loop that stops ticking for a minute is replaced (`narad_reaper_restarts` counts the replacements; past a cap the log line is the alarm). And the loop only sees *open* logs, so a partition closed by idle eviction, or never reopened since a restart, would keep its expired segments until something touched it. The cold-retention walk (`storage.cold_retention_walk_ms`, see [Configuration](../operate/configuration.md)) lists the partition directories on disk every few minutes, stats the closed ones without opening them, and for one holding an expired segment opens the log, runs a single sweep through the same roll, detach and unlink path, and closes it again (`narad_cold_retention_swept_total`).

The consumer frontier is persisted every `storage.consumer_offset_commit_interval_ms` (100ms by default, or back to back with a warning when a node's disk cannot sync every changed partition within that; see [Consume Path](consume-path.md#the-numbers)) with one data sync per partition that changed, one partition at a time: to `consumer.ahead` when the out-of-order ack set changed (its record carries the frontier too), otherwise to `consumer.offset` (8 bytes overwritten in place as a single-sector atomic write + fdatasync; an empty file left by a crash between create and first write reads as "no offset"). So while acks arrive out of order, `consumer.offset` can trail the frontier in `consumer.ahead`; a graceful `Close` brings it level, and a persisted frontier never moves backwards. It is recovered lazily when a partition's queue state is first touched, from both files on disk (the larger frontier wins), deliberately *not* from a boot-time metastore scan, so a stale replica at startup can't misplace consumption progress.

## Opening and closing partition logs

A node's partition logs live in one map (`runtime.Logs`), opened lazily on first use and closed by idle eviction, a retention change, a partition reclaim or move, a topic delete, and shutdown. Opening a log means recovery (a CRC read of its active segment, bounded by the 64 MiB segment size) and closing one means a final flush plus fsyncs, so neither runs under the map's lock. The lock order is: the partition's produce mutex, then the topic's guard, then the map lock. The map lock is held only to look up, claim, install or drop an entry; the metastore lookup, the incarnation check, the open, the close and a purge's unlink all run under the per-topic guard. Opening or closing one topic's logs therefore stalls callers of that topic and nobody else, where it used to stall produce and consume of every topic on the node for the length of the I/O.

Every close of a live log (retention change, reclaim, move install, shutdown, idle eviction) takes the partition's produce mutex first, so it lands between two commits, never between a commit's append and its `CommitDurable`. Otherwise the commit could fail with `ErrLogClosed` after the close's final drain had already written its records: the ingress WAL would re-commit them, and the first copy would surface as duplicates once a later commit advanced past it. Only a purge and the retirement of a stale incarnation skip the produce mutex, since the topic they close has no valid commit left to protect. A peek at a partition that is being closed (`Peek`, `PeekHighWatermark`) waits for the close to finish rather than reading a half-written `hwm` file.

## The frame format, byte by byte

From `storage/format.go`, and yes, the magic is `0xCAFE`:

```
offset 0   magic         2B   0xCA 0xFE
offset 2   flags         1B   bits 0-2: codec (0=none, 1=zstd)
offset 3   recordCount   4B   big-endian int32
offset 7   baseOffset    8B   big-endian int64, first record's offset
offset 15  uncompressed  4B   payload size before codec
offset 19  compressed    4B   payload size on disk
offset 23  crc32c        4B   Castagnoli over header[2:23] + payload
offset 27  payload            [len:4BE][record bytes] × recordCount, codec-encoded
```

The CRC deliberately excludes the magic so the recovery scanner can hunt for `0xCAFE` cheaply when resynchronizing past a torn region. A 256 MiB `maxFrameBytes` bound is enforced on both write and read, so a corrupt header can never talk recovery into allocating the moon.

Inside each record sits the **keyed envelope** (`storage/keyed_record.go`):

```
[version:1B = 0x02][keyLen:uvarint][key][committedAtUnixMs:8B BE][payload]
```

The commit timestamp is written when the envelope is built and raised under the partition produce lock if an earlier commit on the partition carried a later one; that's the per-partition monotonicity the delay gate stands on.

## The flusher pipeline, with its actual knobs

```mermaid
flowchart LR
    A["Append/AppendBatch<br/>(in-memory buffer)"] --> D["drain: flush_bytes 1MiB<br/>flush_records 1000<br/>flush_interval 100ms"]
    D --> F["one frame per drain<br/>(codec applied here)"]
    F --> W[write to active segment]
    W --> S["fsync: batched mode syncs on<br/>sync_interval 1s / sync_bytes 8MiB /<br/>segment roll / Close / explicit Sync()"]
```

Three things make this safe rather than sloppy:

- **The commit path doesn't negotiate.** `Log.CommitDurable` (called by `commitDurable` on every produce commit) synchronously drains, writes, fsyncs, verifies, and advances the high-watermark; the lazy timers above only govern data nobody has been promised yet.
- **`flush_interval` is not a heartbeat.** The flusher holds a timer only while a pass is actually owed: records sitting in the buffer, bytes written but not yet fsynced, or a failed write waiting to retry. A high-watermark advance owes nothing (the boundary is written only at `Close`). An idle partition holds no timer and burns no CPU; an append into an empty buffer re-arms it. That matters at scale, because the cost is per open partition: measured on 500 idle logs, an always-armed 100ms timer cost 2.7% of a core doing nothing, and the runtime's timer heap was most of it. It also means any new "do it later, the timer will pick it up" path needs a matching condition in `flusher.needsTimer`, or it is silently never scheduled once the partition goes quiet.
- **Reads are self-verifying.** Every frame decode re-checks the CRC; the commit path re-reads the just-written frames *before* the high-watermark moves, streamed through a buffer from a process-wide free list (at most one per P) rather than one pinned per partition. Decoded frames and frame positions are cached (`frameCache`, `navCache`). When something has read the partition since the previous write, a frame of up to 1 MiB goes into `frameCache` as it is written, so a consumer at the tail is served without a disk read or a decode; a failed commit's truncate drops those entries, and both caches are invalidated under the write lock when retention deletes a segment. Decoded records alias the frame's decode buffer rather than being copied one by one, which is why a codec's `Decode` must not retain or reuse its output.

## Long-poll wiring, since everyone asks

An idle consumer isn't polling: it parks on the partition log's broadcast channel (`Log.NotifyC`). The channel is *closed* to broadcast: a commit's high-watermark advance, a lease expiry, a nack, and an ack that frees a cap slot or ends an ahead-full stall all `notifyAll()`; every parked waiter wakes, re-checks, and either grabs a message or parks on the fresh channel. Buffering or flushing a record does not broadcast (waiters gate on the high-watermark), and an ack that relieves nothing wakes nobody. Zero timers, zero missed wakeups (a waiter that raced the close sees the already-closed channel immediately).

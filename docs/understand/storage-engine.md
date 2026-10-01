---
description: "Learn how Narad stores a partition on disk: segments of CRC-checked frames, the high watermark that bounds what consumers see, crash recovery and retention."
search:
  boost: 0.5
---

# Storage engine

Learn how Narad stores a partition on disk: segments of CRC-checked frames, the high watermark that bounds what consumers see, crash recovery and retention.

!!! abstract "In short"
    - Every partition is a directory on its owner's volume: a segmented log of 64 MiB segments, written in CRC-checked frames.
    - A commit fsyncs its frames and reads them back before the high watermark moves over them. Consumers never see a record above the high watermark.
    - A failed commit leaves nothing behind, and a failed fsync poisons the log until it is reopened, so the ingress WAL can always commit the records again.
    - Recovery scans only the active segment. A torn tail is truncated; damage in the middle is skipped and recorded, never served.
    - Retention deletes whole sealed segments once they are older than the topic's `retention_ms`.

Every [partition](../reference/glossary.md#partition) is a directory owned by exactly one node. The engine underneath is a segmented log of checksummed frames, with a visibility boundary (the [high watermark](../reference/glossary.md#high-watermark)) and a [retention](../reference/glossary.md#retention) reaper.

## On disk {#on-disk}

```
topics/orders/
├── incarnation        <- the topic incarnation this directory belongs to
└── p00003/
    ├── 00000000000000000000.log   <- sealed segment (starts at offset 0)
    ├── 00000000000000450832.log   <- active segment (starts at 450832)
    ├── hwm                <- 8-byte high watermark written at close
    │                         (emptied by the first commit after an open)
    ├── consumer.offset    <- 8-byte committed consumer frontier
    │                         (levelled with consumer.ahead at most every
    │                         30s, and at close)
    └── consumer.ahead     <- the frontier plus the offsets acked out of
                              order above it (two checksummed 4 KiB
                              slots: a durable anchor and a window)
topics/orders.stale-3f9a1c0e7b2d4a61/   <- quarantined: a deleted
                                           incarnation's leftover
```

How acks reach `consumer.offset` and `consumer.ahead`: [Ack persistence](consume-path.md#how-acks-reach-the-disk).

Everything the engine creates under the data directory is private to the broker's user: directories `0700`, files `0600`. That covers segments, `hwm`, `consumer.offset`, fan-out cursors, the incarnation marker, transferred segments, and the ingress WAL's directory and segments. Segments carry every message payload, so they get the same protection `fsm.db` (password hashes) already had. Modes are applied at creation only, so a file or directory that an older binary created keeps the mode it was created with.

### The incarnation marker {#incarnation-marker}

Topic directories are keyed by name, and a name outlives the topic. Delete `orders` and recreate it, and on any node that missed the purge (it was down, or the purge lost the race with the recreate) the new topic's partition logs would open exactly where the old one's segments, high watermark and consumer offset sit. Served as they are, the recreated topic would hand consumers the deleted topic's messages and append new produce after them.

So every topic record carries an [incarnation](../reference/glossary.md#incarnation) id (`id`, 16 hex characters, created by the proposer at create time and kept by every update), and every topic directory has a marker file, `topics/<name>/incarnation`, holding the id of the incarnation it belongs to. The marker is written the first time a node opens a partition log for the topic, or installs a moved partition, and every open compares it with the metastore record before serving the directory:

- **match**: served;
- **no marker**: the directory predates markers and is adopted (stamped with the current id), the one-time upgrade path; a record without an id keeps name-based bookkeeping and stamps nothing;
- **different marker**: the directory is a deleted incarnation's leftover. Any open logs under the name are closed, the whole directory is renamed to `topics/<name>.stale-<oldid>` (quarantined, never served), an error is logged with both ids, and a fresh directory is opened for the current id.

In that last case, the deleted incarnation's in-memory consumer state (reservations, committed frontiers, acked-ahead sets) is dropped right after the rename, before the fresh directory exists. So none of it is ever written into the new directory. The [offset committer](consume-path.md#how-acks-reach-the-disk) writes by path; if the state were dropped any later, an ack of the deleted incarnation could write its frontier there, and the recreated topic would skip its own first records.

An already-open log is checked again whenever the topic's metadata version moves, so a delete and recreate applied while the log was open retires it rather than serving the old data. The transfer listing a move source serves reads the directory without opening a log, so it makes the same comparison itself and refuses a stale directory.

A [quarantined](../reference/glossary.md#quarantine) directory is reclaimed only after the **leader** confirms that incarnation is gone (the record is absent, or carries a different id). That happens in the purge for that incarnation if it still arrives, in the startup orphan sweep, or in the periodic sweep the move runner performs. Older binaries never read the marker and ignore the file.

### Segments, frames and records {#segments-frames-records}

<figure class="nr-dia nr-dia--doc" id="fig-storage-segments">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--mint">
--8<-- "diagrams/storage-segments.html"
</div>
<figcaption>A segment is the unit of retention, a frame the unit of writing, and a record the unit of reading.</figcaption>
</figure>

- **Segments** are capped at 64 MiB. A frame that pushes the active segment past the cap marks it full and fsyncs it. The roll itself (a new segment named by its first offset) happens at the end of the commit that filled the segment, at close, or right before the next frame write, whichever comes first. A segment also rolls before the first write that finds its oldest record older than the retention roll age (see [Retention](#retention)). Sealed segments are immutable, and they are the unit of retention deletion.
- **Frames** are the write unit: all records drained in one flush become one frame of length-prefixed records, compressed together with the configured codec and covered by a CRC over the stored bytes. The codec is `storage.codec`: `none` by default, or `zstd`. With zstd, frame size tracks batch size: trickle traffic gives one record per frame (about 40% compression on JSON-like payloads), while busy traffic gives frames of hundreds of records (95% or more, since similar records compress against each other).
- **Records** carry the keyed envelope: `[version][key][commit-time][payload]`. Commit time is stamped when the envelope is built, just before the commit takes the partition's produce lock. Under that lock it is raised to the newest commit time the partition has seen, if it is lower (a batch that lost the race for the lock, or a wall-clock step back). So it never decreases along a partition, the property the [delay gate](fanout-engine.md#the-delay-gate) relies on. The floor is kept in memory, so it starts over after a restart, or after the topic is forgotten on delete.

## Write path {#write-path}

Appends go into an in-memory buffer; a flusher goroutine drains the buffer into frames and writes them out. The fsync policy is an internal setting (batched by default), not something a config file can change. The **commit path does not wait for the flusher's timers**: `Log.CommitDurable` forces the drain and the fsync synchronously on the flusher goroutine, reads back and CRC-checks the new frames, and only then advances the high watermark in memory. That is one fsync per commit: the boundary itself is not written to disk on the way (see [High watermark and hidden tail](#the-high-watermark-and-the-hidden-tail) for how a restart still finds it). Buffered data lost in a crash was, by construction, never acked to anyone.

Records drained out of the buffer sit in a **flushing snapshot** until an fsync proves their frame durable. A failed segment write is retried from the unwritten suffix on the next drain, and the in-memory copy is released only after the sync. On the commit path the sync is part of the same drain, so the snapshot never outlives a commit.

### Commit failure {#commit-failure}

A commit can fail at the write (`ENOSPC`, `EIO`), the fsync, the CRC read-back, the segment roll, or the release of the `hwm` file (below), which each commit attempts until it has succeeded once since the log was opened. The records are acked to the producer only by the ingress WAL, which commits any batch again, by appending the same records, when its `CommitDurable` did not return success.

So a failed commit must leave **nothing** of the batch behind. If the first copy stayed in the log (in the snapshot, or already written and fsynced), the retry would append a second copy at fresh offsets. Its commit would then advance the high watermark past both, delivering every record of the batch twice, permanently, without any crash.

Before the error reaches the caller, the flusher discards the uncommitted tail. Everything above the high watermark is dropped from the buffer and the snapshot, the active segment is truncated back to the first frame at or above the high watermark (and the truncate fsynced), the sparse index and the caches forget the cut frames, and the next append gets the offset the failed batch had. The retry lands exactly one copy at the same offsets. The lazy roll above is what makes this always possible: a commit's frames are never sealed into an immutable segment before the commit has returned.

### Fsync failure {#fsync-failure}

An fsync failure is final for the log. After a failed `fdatasync` the kernel may already have dropped the dirty pages (Linux marks them clean and reports the error once). A second fsync that "succeeds" proves nothing about the bytes that failed, and the tail of the segment past the last good sync is of unknown content. This is the PostgreSQL fsyncgate lesson: the only sound recovery is the one a crash would get.

Narad does not crash the node, since other partitions on it are fine, but it treats the log as crashed:

- the error is **latched** (`storage.ErrLogPoisoned`, wrapping the original error), and every later append and commit on that log fails with it;
- the failure is logged at error level and counted in `errors_total{component="storage",kind="fsync_poisoned"}`;
- the state clears only when the log is reopened and its files scanned again.

Reads of committed records keep working. Nothing acked is lost: records above the last durable tail are still in the ingress WAL, which reroutes them to a sibling partition after its commits keep failing, and commits them here again after the reopen. What to do is in [Troubleshooting](../operate/troubleshooting.md#log-fsync-poisoned).

## High watermark and hidden tail {#the-high-watermark-and-the-hidden-tail}

**Unreleased:** in master, not in v3.0.1. v3.0.1 writes the `hwm` file as commits advance the boundary.

The high watermark is the exclusive bound of what consumers may see. An open log keeps it in memory; the `hwm` file holds a boundary only while the log is **closed**:

- **Close writes it.** Once the flusher has stopped, `Close` writes the exact high watermark (8 bytes overwritten in place, a single-sector atomic write plus `fdatasync`) unless the file already holds it. Readers that answer without opening the log (the topic describe, the consume pump's backlog estimate, fan-out reads of a closed parent, the partition transfer listing, the metrics poller) read that value, and for a cleanly closed log it is exact.
- **Open trusts it, clamped.** An 8-byte file recovers as `min(file, record tail)`. Opening writes nothing.
- **The first advance empties it.** Before the first high-watermark advance of a log's life (inside the first commit after the log was opened), the file is truncated to empty and fsynced, once. No later commit touches it. An empty or missing file tells recovery to take the boundary from the CRC-verified record tail.

The record tail never hides a visible record, because a commit fsyncs and verifies its frames before it advances the boundary. It can hold more than the committed records, though. An open reads it through the page cache, so after a process crash it includes frames the dead process wrote but never saw fsynced. An open that takes the boundary from the tail therefore fsyncs the active segment first (sealed segments were fsynced when they rolled), and fails if that fsync fails, so nothing it exposes can be taken back by a later power loss. That is one fsync per such open, never one on the commit path.

So a crash while the log is open leaves an empty file and every committed record visible, and a commit pays one fsync (the segment) where it used to pay a second one, in series, for the boundary file under the produce lock.

Emptying the file, rather than letting it lag the commits, is what keeps a rollback from hiding acked records. Releases before this one persisted the file on every commit, and recover an 8-byte file as `min(file, tail)` and an empty one as the tail. A file that lagged the acked commits would make such a binary, started after a crash, hide acked records, and its failed-commit discard would then truncate them. Those releases take the tail without the fsync, though, so a rollback started right after a crash leaves a narrow window in which a power loss can take back records the older binary already served. A clean stop before the rollback closes it (see [Upgrade Narad](../operate/upgrade.md#roll-back)).

After a crash the file stays empty until the log is opened and closed again, and a reader of the closed partition finds no boundary on disk. Startup opens every partition the node owns before it reports ready: up to min(8, GOMAXPROCS) topics at once, each topic's partitions one after another, skipping, with a warning, one whose assignment lookup or open fails at that point. So in practice every owned partition is open until idle eviction closes it and writes the exact boundary.

The readers that act on the boundary do not take such an empty file as 0. The transfer listing and a fan-out read of a closed parent open the log (the fan-out read to drain the backlog behind it). The children listing, which never opens a log, leaves the partition out, and the metrics poller exports no per-partition series for it. When the segments hold no record bytes, the first three need no open: the boundary is the offset the newest segment is named for.

Records can sit above the high watermark, deliberately:

<figure class="nr-dia nr-dia--doc" id="fig-storage-hwm-tail">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--mint">
--8<-- "diagrams/storage-hwm-tail.html"
</div>
<figcaption>A batch is written and checked above the high watermark and becomes visible only in step 4; a crash before then leaves a hidden copy that the ingress WAL makes a duplicate, never a loss.</figcaption>
</figure>

These records in the [hidden tail](../reference/glossary.md#hidden-tail) were written but never exposed. A crash landed after a commit wrote them and before it advanced the boundary (before or after its fsync), or a failed commit could not truncate them (the log is then poisoned, see above). For produce-path records the ingress WAL still owns them and **commits them again at fresh offsets**, since its checkpoint never passes a batch whose commit did not return. So the hidden copy is a duplicate in waiting.

A clean `Close` writes the boundary below them, so they stay hidden across a clean restart until a new commit appends past them and advances the high watermark over them. After a crash, the empty file exposes them at reopen instead, once the open has fsynced them. Either way the outcome is duplicates, never loss, and only around crashes and poisoned logs.

## Recovery {#recovery}

Opening a log reads only its **active** (last) segment (unreleased; v3.0.1 scans every segment at open). That segment is walked frame by frame, CRC included, and indexed:

- A **torn tail** in the active segment (a crash in the middle of a write, including a commit's frame write) is truncated and the truncate fsynced. Those bytes were never acked by this log, and the ingress WAL commits the batch again at the offset it had. A last frame whose bytes are all present but do not check out (a zero-filled or scrambled final sector: the file size reached the disk, the data did not) is a torn tail too, as long as no valid frame follows it. Left in place, its intact-looking header would shadow the frames the next commits write at the same offsets.
- **Corruption in the middle of the file**, under valid later frames, is *not* truncated, because that would destroy acked data and move offsets backwards. The walk resyncs to the next frame that passes its CRC and carries on, and the bad frame's offsets become a permanent gap.

A **sealed** segment is opened only to stat it, and its handle is released at once. Its range ends at its successor's base offset, taken from the file name. That is exact rather than an estimate: a roll fsyncs the active segment before it creates the next one, named by the offset the sealed one ended at, and nothing writes to a sealed segment again.

So an open costs O(active segment), at most about one 64 MiB segment, instead of O(retained bytes). On 256 MiB of sealed 4 KiB frames an open went from about 94 ms and 287 MiB allocated to about 0.5 ms and 13 KiB (macOS, warm page cache). The active walk streams each frame through one reused buffer rather than allocating one per frame, about 20% faster (a 60 MiB active segment of 4 KiB frames: 22.4 ms to 17.9 ms).

That moves where damage in a sealed segment is found: **at the read that lands on it, never at open**, and it is never served. Every read CRC-checks the frame it serves, so a record in a bad sealed frame reads as corrupt or not found:

- a queue consume skips it, counts it in `narad_consumer_corrupt_skipped_total` and logs a warning (`skipped permanently-unreadable record`);
- a replay answers `410`;
- a fan-out cursor skips it and counts it in `narad_fanout_child_dropped_messages`.

The open never did more than that with such a frame: the old full scan resynced past a bad sealed frame without a word. An I/O error in a sealed segment fails the reads that need it (a queue consume that reaches such an offset gives the record back and answers `500`) instead of failing the whole partition open. The bound assumes every sealed segment's frames end at or below its successor's base, which every roll this code has written keeps; frames of a hand-edited or misnamed file past that point would be unreachable.

### Drives that lie about fsync {#lying-fsync}

Every durability decision above assumes `fdatasync` tells the truth. A drive (or a virtual disk) that acknowledges a flush it never performed can lose, at a power cut, any record whose commit was acked on the strength of the lie. Either the frames are gone (the segment sync lied), or the sync that empties the `hwm` file before a log's first advance lied: then the previous `Close`'s boundary is still on disk, and the records above it sit in the hidden tail until a later commit advances the boundary over them. Commits never sync that file otherwise, so the second exposure happens once per log open, not once per commit.

That is the hardware's loss, not the engine's, and it is bounded to exactly those records. A sealed segment whose last sync lied keeps its range up to its successor's base, so its lost offsets read as not found or corrupt and are skipped as recorded loss.

What must never happen, and what the disk-fault tests in `storage` and `wal` pin under a lying-fsync simulation (`fault_test.go`, driven by the `syncfile` fault seam that fails or skips a chosen syscall, for tests only):

- a torn or fabricated record served;
- a crash loop at open;
- a sparse index that disagrees with the file;
- a record from a fully honest commit missing;
- a high watermark that grew past what the crash left.

## Retention {#retention}

One process-wide reaper loop (a single goroutine, not one per partition) sweeps every open partition log that has an age bound about once a minute. It deletes **sealed segments whose last write is older than the topic's `retention_ms`**. The unit is the segment, and two rules keep a segment from outliving retention on a partition that never fills one:

- **Time-based roll.** The flusher rolls the active segment before the first write that finds the segment's oldest record older than the roll age (`RetentionConfig.MaxSegmentAge`, which defaults to `MaxAge`). A partition writing 1 MiB a day with a 7-day retention used to keep its oldest records for months (until the 64 MiB segment filled, then one more retention period). Now the segment is sealed after at most one retention period of writes.
- **Rotation of an idle active segment.** A partition that stops writing never triggers the roll, so the reaper asks the flusher to seal an active segment whose last write is older than `MaxAge` (every record in it has expired), and deletes it in the same sweep. Only a fully committed segment is rotated. One that still holds records above the high watermark waits for the ingress WAL to commit them again first, so the failed-commit discard above always finds them in the active segment.

<figure class="nr-dia nr-dia--doc" id="fig-storage-retention">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--mint">
--8<-- "diagrams/storage-retention.html"
</div>
<figcaption>Retention deletes whole segments by their last write, so a record can outlive <code>retention_ms</code>: the oldest record in segment B is already past it and stays until B goes.</figcaption>
</figure>

Together these bound a record's lifetime by `MaxSegmentAge + MaxAge + CheckInterval`, so with the defaults a record is gone within about twice the retention age of its write. For a segment recovered from disk, the roll age counts from the file's mtime (its last write), the only write time the inode keeps, so such a segment can live up to one write-span longer.

The reaper's bookkeeping is cheap:

- Every segment caches its first and last write time, so the reaper never stats a file per sweep.
- A sweep detaches expired segments under the write lock, but unlinks the files after releasing it. A restart with a backlog of expired segments therefore does not stall the partition's readers and flusher for the whole batch of unlinks.
- A failed unlink is logged and counted (`errors_total{component="storage",kind="retention_unlink"}`) rather than dropped in silence while the file keeps using disk.
- Sealed segments do not pin a file descriptor. Recovery opens a sealed segment only to stat it and releases the handle, the first read reopens it lazily, and the handle goes away with the segment's sparse index when it leaves the two-segment hot set. So a partition with days of history holds a handful of descriptors, not one per segment.
- Deletions export byte and message counters (`reason="age"`).

Two things keep retention running when the happy path does not. The shared loop is supervised: a sweep that panics is logged and skipped for that partition rather than taking retention down for every partition on the node, and a loop that stops ticking for a minute is replaced (`narad_reaper_restarts` counts the replacements; past a cap the log line is the alarm). And the loop only sees *open* logs, so a partition closed by idle eviction, or not reopened since a restart, would keep its expired segments until something touched it. The cold-retention walk (`storage.cold_retention_walk_ms`, see [Configuration reference](../reference/configuration.md#storage)) lists the partition directories on disk every few minutes and stats the closed ones without opening them. For one holding an expired segment, it opens the log, runs a single sweep through the same roll, detach and unlink path, and closes it again (`narad_cold_retention_swept_total`).

The consumer frontier stored next to the segments is written at two cadences (unreleased; see [Consume path](consume-path.md#how-acks-reach-the-disk)). Every 100 ms (or every durability interval, when that is shorter), each partition acked since the last tick has its `consumer.ahead` record, the frontier plus the out-of-order ack set, written into the page cache through a held descriptor. Once per `storage.consumer_offset_commit_interval_ms` (1 s by default), each partition written since is written out, one partition at a time, with one device flush per tick on macOS. `consumer.ahead`'s two slots are an anchor, the newest record known durable, which is never written while it is the anchor, and a window the ticks overwrite; a writeout flips them.

`consumer.offset` (8 bytes overwritten in place as a single-sector atomic write; an empty file left by a crash between create and first write reads as "no offset") is brought level with that frontier at most every 30 s while the broker runs, so it can trail `consumer.ahead`. A graceful `Close` levels it and writes both files out. It is recovered lazily when a partition's queue state is first touched, from both files on disk (the larger frontier wins), deliberately *not* from a metastore scan at boot, so a stale replica at startup cannot misplace consumption progress. Both files keep the formats of v3.0.1, which recovers them the same way.

## Opening and closing logs {#open-close}

A node's partition logs live in one map (`runtime.Logs`). They are opened lazily on first use, and closed by idle eviction, a retention change, a partition reclaim or move, a topic delete, and shutdown. Opening a log means recovery (a CRC read of its active segment, bounded by the 64 MiB segment size), and closing one means a final flush plus fsyncs, so neither runs under the map's lock.

The lock order is: the partition's produce mutex, then the topic's guard, then the map lock. The map lock is held only to look up, claim, install or drop an entry; the metastore lookup, the incarnation check, the open, the close and a purge's unlink all run under the per-topic guard. Opening or closing one topic's logs therefore stalls callers of that topic and nobody else, where it used to stall produce and consume of every topic on the node for the length of the I/O.

Every close of a live log (retention change, reclaim, move install, shutdown, idle eviction) takes the partition's produce mutex first, so it lands between two commits, never between a commit's append and its `CommitDurable`. Otherwise the commit could fail with `ErrLogClosed` after the close's final drain had already written its records: the ingress WAL would commit them again, and the first copy would surface as duplicates once a later commit advanced past it. Only a purge and the retirement of a stale incarnation skip the produce mutex, since the topic they close has no valid commit left to protect.

A peek at a partition that is being closed (`Peek`, `PeekHighWatermark`) waits for the close to finish rather than reading a half-written `hwm` file. Reading a record from a closed log answers `ErrLogClosed` and nothing else, never "not found" or "corrupt": a reader still holding a retired incarnation's log would otherwise read those as a gap in the successor (see [Consume path](consume-path.md#reserve-deliver-settle)).

## Frame format {#frame-format}

From `storage/format.go`:

```
offset 0   magic         2B   0xCA 0xFE
offset 2   flags         1B   bits 0-2: codec (0=none, 1=zstd)
offset 3   recordCount   4B   big-endian int32
offset 7   baseOffset    8B   big-endian int64, first record's offset
offset 15  uncompressed  4B   payload size before codec
offset 19  compressed    4B   payload size on disk
offset 23  crc32c        4B   Castagnoli over header[2:23] + payload
offset 27  payload            [len:4BE][record bytes] x recordCount,
                              codec-encoded
```

The CRC leaves out the magic, so the recovery scanner can search for `0xCAFE` cheaply when it resyncs past a torn region. A 256 MiB `maxFrameBytes` bound is enforced on both write and read, so a corrupt header can never make recovery allocate an absurd amount of memory.

Inside each record sits the **keyed envelope** (`storage/keyed_record.go`):

```
[version:1B = 0x02][keyLen:uvarint][key][committedAtUnixMs:8B BE][payload]
```

The commit timestamp is written when the envelope is built, and raised under the partition produce lock if an earlier commit on the partition carried a later one. That is the per-partition monotonic order the delay gate depends on.

## Flusher pipeline {#flusher}

<figure class="nr-dia nr-dia--doc" id="fig-storage-flusher">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--mint">
--8<-- "diagrams/storage-flusher.html"
</div>
<figcaption>Timers bound how long unpromised data sits in memory. A commit never waits for them: it forces its own drain and fsync before the high watermark moves.</figcaption>
</figure>

These are internal defaults, not settings a config file can change. Three properties keep the pipeline safe:

- **The commit path does not wait for the timers.** `Log.CommitDurable` (called by `commitDurable` on every produce commit) synchronously drains, writes, fsyncs, verifies, and advances the high watermark. The timers above only govern data nobody has been promised yet.
- **`flush_interval` is not a heartbeat.** The flusher holds a timer only while a pass is actually owed: records sitting in the buffer, bytes written but not yet fsynced, or a failed write waiting to retry. A high-watermark advance owes nothing (the boundary is written only at `Close`). An idle partition holds no timer and uses no CPU, and an append into an empty buffer arms it again. That matters at scale, because the cost is per open partition: measured on 500 idle logs, an always-armed 100ms timer cost 2.7% of a core doing nothing, most of it in the runtime's timer heap. It also means any new "do it later, the timer will pick it up" path needs a matching condition in `flusher.needsTimer`, or it is never scheduled once the partition goes quiet.
- **Reads verify themselves.** Every frame decode checks the CRC again. The commit path reads back the frames it just wrote *before* the high watermark moves, streamed through a buffer from a process-wide free list (at most one per P) rather than one pinned per partition. Decoded frames and frame positions are cached (`frameCache`, `navCache`). When something has read the partition since the previous write, a frame of up to 1 MiB goes into `frameCache` as it is written, so a consumer at the tail is served without a disk read or a decode. A failed commit's truncate drops those entries, and both caches are invalidated under the write lock when retention deletes a segment. Decoded records point into the frame's decode buffer rather than being copied one by one, which is why a codec's `Decode` must not retain or reuse its output.

## Wake-ups for waiting readers {#wakeups}

A partition log announces that records may have become deliverable: when the high watermark advances, when something calls `Wake` (a lease expiry, a nack, a freed cap slot), and when the log closes. Buffering or flushing a record announces nothing, because every reader gates on the high watermark and would only wake to find nothing.

The announcement reaches two kinds of reader:

- **Fan-out slab readers** wait on the log's broadcast channel, `Log.NotifyC`. The channel is closed to wake every waiter at once, and each waiter checks again and fetches the fresh channel before it waits again. A waiter fetches the channel before its last check for data, so a commit that lands between the check and the wait still wakes it. When nobody fetched the channel since the last broadcast, a broadcast does nothing, which keeps appends allocation-free.
- **Queue consumers** do not park on the log at all. Each log carries a wake notifier (`SetWakeNotifier`) that tells the consume dispatcher the topic may have records, at the cost of two atomic loads unless the topic has a parked consumer. The dispatcher's single pump then hands each record to exactly one waiting consumer (see [Consume path](consume-path.md#reserve-deliver-settle)).

## Next steps

- [Consume path](consume-path.md): how consumers lease and settle the records this engine stores.
- [Troubleshooting](../operate/troubleshooting.md#log-fsync-poisoned): what to do when a log is poisoned by a failed fsync.

// Package syncfile provides SyncData: the cheapest kernel primitive
// that makes an append-only file's DATA durable, chosen per OS at
// build time.
//
// The contract is deliberately narrower than os.File.Sync: SyncData
// guarantees the file's contents — including the size, where that is
// needed to read appended data back — survive power loss. It does NOT
// guarantee timestamps or other inode metadata, and it does not sync
// the parent directory (a newly created file still needs its directory
// fsynced to survive; see the WAL's syncDir).
//
// Per kernel:
//
//	Linux   fdatasync(2) — data + size, skips the metadata journal
//	        write that fsync(2) pays on every group commit.
//	macOS   fcntl(F_FULLFSYNC) via os.File.Sync — Darwin has no
//	        fdatasync, and its plain fsync does not force the drive
//	        cache; F_FULLFSYNC is the only durable option.
//	Windows FlushFileBuffers via os.File.Sync — NT has no data-only
//	        variant.
//	other   os.File.Sync — correct everywhere Go runs.
//
// Use it only on append-only data files (WAL segments, log segments).
// Directory handles and rename-based checkpoint files keep full Sync:
// their durability IS metadata.
package syncfile

// Package cluster holds scripted multi-process cluster scenarios: each
// test builds the narad binary, starts a 3-node cluster on localhost
// (ports 34000-34099), drives it with the tests/integration chaos driver,
// and kills, restarts or re-certifies nodes underneath it while asserting
// nothing is lost:
//
//   - TestSnapshotRestoreUnderLoad: a node killed under load, the
//     survivors compacted past its log, comes back by installing the
//     leader's snapshot and converges on the same metadata.
//   - TestTLSCertRenewalRollingRestart, TestTLSCARotation,
//     TestTLSUntrustedCertFailsLoudly: Raft mTLS certificate renewal and
//     CA rotation by rolling restart, and the failure mode of a
//     certificate the peers do not trust.
//
// The scenarios take minutes and spawn processes, so they are behind the
// "cluster" build tag and do not run with a plain go test ./...:
//
//	go test -tags cluster -count=1 -timeout 30m ./tests/cluster/...
//
// Environment:
//
//	NARAD_CLUSTER_TEST_NORACE=1    build the server without -race (faster)
//	NARAD_CLUSTER_TEST_ARTIFACTS=d copy node logs of failed tests under d
//	NARAD_CLUSTER_TEST_KEEP_LOGS=1  copy them for passing tests too
package cluster

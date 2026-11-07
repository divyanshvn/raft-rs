# raft-rs

A work-in-progress Rust implementation of Raft for learning distributed consensus.
Built with Tokio and tarpc. It includes leader election, heartbeats, log replication,
follower catch-up and local persistence.

## Code structure

| Path                  | Responsibility                                               |
| --------------------- | ------------------------------------------------------------ |
| `src/lib.rs`          | Library module exports                                       |
| `src/node/mod.rs`     | Node lifecycle, elections, replication and follower catch-up |
| `src/raft_rpc.rs`     | Peer RPC definitions and vote / append-entry handlers        |
| `src/client_rpc.rs`   | Client RPC for submitting string log entries                 |
| `src/types.rs`        | Node roles, IDs, log metadata and shared state               |
| `src/persistence.rs`  | JSON state validation, saving, recovery and storage tests    |
| `src/utils.rs`        | Timing, quorum and RPC deadline helpers                      |
| `tests/raft_tests.rs` | Cluster election and repeated leader-disconnection scenarios |
| `tests/common/`       | Cluster setup, simulated network partitions and tracing      |

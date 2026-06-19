# Porting Raft Tests to Rust (`raft-rs`)

## Background Context
Currently, the Raft implementation in Go (`6.5840`) uses a massive, battle-tested framework (`tester1` and `labrpc`). The Go tests spin up actual OS processes (`DaemonClnt`) for each node to ensure strict memory isolation, and they use Unix sockets communicating via Go's specific `encoding/gob` serialization format. 

Your Rust implementation (`raft-rs`) uses `tokio` and `tarpc` for RPCs, but currently only has a basic `test_append_entries` happy-path test using in-memory channels without any simulated failures.

To "port" the tests to Rust, we have to bridge the gap between rigorous simulated failures and the Rust ecosystem.

## Proposed Approaches

### Approach 1: Native Rust Rewrite (Recommended)
We rewrite the testing framework and test cases completely in Rust within the `raft-rs/tests` directory.
* **Mechanism**: 
  * We build a custom Network Emulator in Rust that wraps `tarpc`'s channels. It will randomly delay, drop, and reorder packets.
  * We build a `Config` struct (equivalent to `test.go`) to manage node lifecycles (start, crash, isolate, reconnect) and verify system state (e.g., `check_one_leader()`).
  * We translate the test logic from `raft_test.go` directly into Rust async tests.
* **Pros**: 100% idiomatic Rust. Easy to debug using standard Rust tools (like `tokio-console`). No dual-language overhead.
* **Cons**: We have to port thousands of lines of sophisticated testing logic, which could introduce test bugs.

### Approach 2: Go Adapter Daemon (Reuse the Go tests)
Instead of porting the tests to Rust, we run the Rust nodes *under* the existing Go tests.
* **Mechanism**: The Go tester uses `encoding/gob` over Unix sockets, which is practically impossible to parse natively in Rust. To bypass this, we write a small "Adapter" program in Go. The MIT Tester spawns the Go Adapter (thinking it's the node). The Go Adapter speaks Gob to the Tester, and translates those calls into standard JSON or gRPC/tarpc to a child Rust process.
* **Pros**: We use the exact, rigorous MIT 6.5840 test suite without changing it.
* **Cons**: Messy dual-language architecture. Requires maintaining an adapter layer.

### Approach 3: JSON-RPC over Unix Sockets
* **Mechanism**: Modify the `6.5840` lab framework to use `encoding/json` instead of `labgob`. The Rust node then implements a Unix socket listener to parse the JSON RPCs directly from the Go tester.
* **Pros**: No adapter binary needed.
* **Cons**: Modifies the core grading framework (`labrpc`, `sockrpc`), which might break other labs.

---

> [!IMPORTANT]
> **User Review Required**
> I strongly recommend **Approach 1 (Native Rust Rewrite)** because it aligns best with the goal of creating a standalone, high-quality Rust project. It will result in an idiomatic Rust codebase that doesn't rely on the legacy Go framework. 
> 
> However, if your primary goal is to simply validate that your Rust code is 100% compliant with the MIT labs as fast as possible, **Approach 2** is the safer bet.
> 
> **Which approach would you prefer to proceed with?**

## Implementation Plan for Approach 1 (Native Rust Rewrite)

If we proceed with Approach 1, here is the technical breakdown:

### 1. Build the Network Emulator (The `labrpc` equivalent)
We will create a `Network` struct that acts as an intermediary for all `tarpc` communication between nodes.
* **Connection State**: Maintain a matrix of boolean flags indicating if Node A can talk to Node B (to simulate partitions).
* **Interception**: Instead of using `tarpc::transport::channel::unbounded()` directly between nodes, nodes will communicate with the `Network` router. The router will inspect the partition matrix, apply randomized `tokio::time::sleep` (to simulate latency), or randomly drop the future (to simulate packet loss).

### 2. Build the Cluster Config Manager (The `test.go` equivalent)
We will build a `TestConfig` struct.
* **Node Lifecycle**: Methods like `start_node(id)`, `crash_node(id)`. This requires the Rust `Node` to support a graceful shutdown mechanism (likely via a `CancellationToken`).
* **Validation**: Port the complex validation functions:
  * `check_one_leader()`: Queries all nodes, asserts exactly one node claims leadership with the highest term.
  * `check_terms()`: Verifies terms are monotonically increasing.
  * `check_logs()`: Queries the committed logs of all nodes and ensures they are identical at given indices.

### 3. Port the Test Cases (from `raft_test.go`)
We will create async tests in `raft-rs/tests/raft_tests.rs`. We will start with the 2A (Elections) tests:
* `test_initial_election`: Start 3 nodes, wait, verify 1 leader.
* `test_re_election`: Start 3 nodes, find leader, disconnect leader. Wait, verify new leader. Reconnect old leader, verify it steps down.

## Open Questions

1. **Which Approach?** Do you prefer Approach 1 (Native Rust), Approach 2 (Go Adapter), or Approach 3 (JSON)?
2. **Current Rust Node Completeness:** I noticed `Node` currently drops locks and uses `unimplemented!()` in a few places (like error handling in `request_vote`). If we port the tests natively, we will likely expose these panics. Do you want to fix the Node implementation as we go, or just focus on getting the tests ported first?
3. **Persistence:** The Go tests heavily test persistence (saving State to disk). Does your Rust implementation currently have a disk persistence layer, or should we mock an in-memory `Storage` trait for the tests?

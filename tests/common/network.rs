use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

use anyhow::Result;

use raft_rs::raft_rpc::{RaftRpc, RaftRpcClient};
use raft_rs::types1::{Log, LogMetadata, NodeId};

pub struct Network {
    pub reliable: AtomicBool,
    pub long_delays: AtomicBool,
    pub long_reordering: AtomicBool,
    pub connected: Mutex<HashMap<(usize, usize), bool>>,
}

impl Network {
    pub fn new() -> Self {
        Self {
            reliable: AtomicBool::new(true),
            long_delays: AtomicBool::new(false),
            long_reordering: AtomicBool::new(false),
            connected: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_reliable(&self, reliable: bool) {
        self.reliable.store(reliable, Ordering::SeqCst);
    }

    pub fn set_partition(&self, from: usize, to: usize, connected: bool) {
        let mut map = self.connected.lock().unwrap();
        map.insert((from, to), connected);
        map.insert((to, from), connected);
    }

    pub fn is_connected(&self, from: usize, to: usize) -> bool {
        let map = self.connected.lock().unwrap();
        *map.get(&(from, to)).unwrap_or(&true)
    }

    pub async fn simulate_network_delay(&self, from: usize, to: usize) -> Result<()> {
        if !self.is_connected(from, to) {
            // sleep(Duration::from_millis(50)).await;
            return Err(anyhow::anyhow!("Network partitioned"));
        }

        let reliable = self.reliable.load(Ordering::SeqCst);

        if !reliable {
            let short_delay = rand::random::<u32>() % 27;
            sleep(Duration::from_millis(short_delay as u64)).await;

            if rand::random::<u32>() % 1000 < 100 {
                return Err(anyhow::anyhow!("Packet dropped"));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ProxyNode {
    pub node_id: usize,
    pub target: RaftRpcClient,
    pub network: Arc<Network>,
}

impl RaftRpc for ProxyNode {
    async fn append_entries(
        self,
        context: tarpc::context::Context,
        term: u32,
        leader_id: NodeId,
        prev_log_metadata: LogMetadata,
        logs: Vec<Log>,
        leader_commit: LogMetadata,
    ) -> Result<(u32, bool)> {
        let to = self.node_id;
        let from = leader_id.0;
        if self.network.simulate_network_delay(from, to).await.is_err() {
            // Sleeping till Deadline to correctly stimulate network partition.
            sleep(Duration::from_secs(10)).await;
            return Err(anyhow::anyhow!("Network partitioned OH NOOOO"));
        };
        let mut req_ctx = tarpc::context::current();
        req_ctx.deadline = context.deadline;
        let res = self
            .target
            .append_entries(
                req_ctx,
                term,
                leader_id,
                prev_log_metadata,
                logs,
                leader_commit,
            )
            .await?;
        res.map_err(|e| anyhow::anyhow!(e))
    }

    async fn request_vote(
        self,
        context: tarpc::context::Context,
        term: u32,
        candidate_id: NodeId,
        last_log_metadata: LogMetadata,
    ) -> Result<(u32, bool)> {
        let from = candidate_id.0;
        let to = self.node_id;
        self.network.simulate_network_delay(from, to).await?;
        let mut req_ctx = tarpc::context::current();
        req_ctx.deadline = context.deadline;
        let res = self
            .target
            .request_vote(req_ctx, term, candidate_id, last_log_metadata)
            .await?;
        res.map_err(|e| anyhow::anyhow!(e))
    }
}

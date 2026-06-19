use std::{
    any::Any,
    collections::VecDeque,
    sync::{atomic::AtomicU32, Arc, Mutex},
};

use anyhow::Result;

use crate::{
    node::Node,
    types1::{LeaderState, Log, LogMetadata, NodeState, NodeType},
};

#[tarpc::service]
pub trait ClientRpc {
    // 05/24/2026 09:04 AM IST - TODO: should i use vector of logs instead?
    async fn reply_log(log: String) -> Result<()>;
}

impl ClientRpc for Node {
    async fn reply_log(mut self, context: tarpc::context::Context, log: String) -> Result<()> {
        // println!(
        //     "append at node {:?}: {}",
        //     self.state.lock().unwrap().id,
        //     log
        // );
        self.call_append_entries(vec![log]).await?;
        Ok(())
    }
}

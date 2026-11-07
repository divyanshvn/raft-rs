//! Whole-file persistence for small, Linux-based learning clusters.
use crate::types::{Log, LogMetadata, NodeId, NodeState, NodeType, START};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
};

#[derive(Serialize, Deserialize)]
pub(crate) struct SavedState {
    pub id: NodeId,
    pub current_term: u32,
    pub voted_for: Option<(NodeId, u32)>,
    pub logs: Vec<Log>,
}
impl SavedState {
    pub fn validate(&self, id: usize, count: usize) -> Result<()> {
        ensure!(self.id.0 == id, "state belongs to another node");
        if let Some((candidate, _)) = self.voted_for {
            ensure!(candidate.0 < count, "invalid saved vote candidate");
        }
        ensure!(!self.logs.is_empty(), "missing initial entry");
        ensure!(
            self.logs[0].metadata == LogMetadata::default() && self.logs[0].log == START,
            "invalid initial entry"
        );
        for (i, log) in self.logs.iter().enumerate() {
            ensure!(log.metadata.index as usize == i, "invalid saved log index");
        }
        Ok(())
    }
}
impl NodeState {
    pub(crate) fn healthy(&self) -> Result<()> {
        ensure!(!self.failed, "node stopped after storage failure");
        Ok(())
    }

    pub(crate) fn persist(&mut self) -> Result<()> {
        self.healthy()?;
        let Some(path) = self.state_path.as_ref() else {
            return Ok(());
        };
        let result = (|| -> Result<()> {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."));
            let mut name = path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("missing file name"))?
                .to_os_string();
            name.push(".tmp");
            let temporary = parent.join(name);
            let bytes = serde_json::to_vec(&SavedState {
                id: self.id,
                current_term: self.current_term,
                voted_for: self.voted_for,
                logs: self.logs.clone(),
            })?;
            let mut file = File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
            self.current_type = NodeType::Follower;
            self.leader = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{node::Node, raft_rpc::RaftRpcClient};
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "raft-persistence-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn file(&self) -> std::path::PathBuf {
            self.0.join("state.json")
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn peers() -> Vec<RaftRpcClient> {
        (0..3)
            .map(|_| {
                let (tx, _rx) = tarpc::transport::channel::unbounded();
                RaftRpcClient::new(tarpc::client::Config::default(), tx).spawn()
            })
            .collect()
    }
    #[tokio::test]
    async fn restores_saved_local_state_as_follower() {
        let dir = Directory::new();
        let peers = peers();
        let node = Node::open(0, &peers, dir.file()).unwrap();
        {
            let mut state = node.state.lock().await;
            state.current_term = 7;
            state.voted_for = Some((NodeId(1), 7));
            state.current_type = NodeType::Leader;
            state.logs.push(Log {
                log: "saved".into(),
                metadata: LogMetadata { term: 7, index: 1 },
            });
            state.persist().unwrap();
        }
        drop(node);
        let node = Node::open(0, &peers, dir.file()).unwrap();
        let state = node.state.lock().await;
        assert_eq!(state.current_term, 7);
        assert_eq!(state.voted_for, Some((NodeId(1), 7)));
        assert_eq!(state.current_type, NodeType::Follower);
        assert!(state.leader.is_none());
        assert_eq!(state.logs[1].log, "saved");
        assert_eq!(state.latest_log, state.logs[1].metadata);
        assert_eq!(state.commit_index.index, 0);
        assert_eq!(state.last_applied, 0);
    }
    #[tokio::test]
    async fn rejects_corrupt_or_wrong_node_state() {
        let dir = Directory::new();
        let peers = peers();
        Node::open(0, &peers, dir.file()).unwrap();
        assert!(Node::open(1, &peers, dir.file()).is_err());
        fs::write(dir.file(), b"broken").unwrap();
        assert!(Node::open(0, &peers, dir.file()).is_err());
        assert_eq!(fs::read(dir.file()).unwrap(), b"broken");
    }
    #[tokio::test]
    async fn ignores_partial_temporary_and_stops_on_write_error() {
        let dir = Directory::new();
        let peers = peers();
        Node::open(0, &peers, dir.file()).unwrap();
        let temporary = dir.0.join("state.json.tmp");
        fs::write(&temporary, b"partial").unwrap();
        let node = Node::open(0, &peers, dir.file()).unwrap();
        fs::remove_file(&temporary).unwrap();
        fs::create_dir(&temporary).unwrap();
        let mut state = node.state.lock().await;
        state.current_term = 1;
        assert!(state.persist().is_err());
        assert!(state.healthy().is_err());
        assert!(
            !Node::open(0, &peers, dir.file())
                .unwrap()
                .state
                .try_lock()
                .unwrap()
                .failed
        );
    }
}

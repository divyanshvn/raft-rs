use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Notify;

use crate::raft_rpc::RaftRpcClient;
use crate::utils::get_current_time;

pub struct VoteRequest {}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Hash)]
pub struct NodeId(pub usize);

pub const START: &str = "START";

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Log {
    pub log: String,
    pub metadata: LogMetadata,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default, Copy)]
pub struct LogMetadata {
    pub term: u32,
    pub index: u32,
}

impl PartialOrd for LogMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.term > other.term {
            return Some(Ordering::Greater);
        }
        if self.term < other.term {
            return Some(Ordering::Less);
        }

        if self.index > other.index {
            return Some(Ordering::Greater);
        }

        if self.index < other.index {
            return Some(Ordering::Less);
        }

        return Some(Ordering::Equal);
    }
}

#[derive(Clone)]
pub struct NodeState {
    pub(crate) current_type: NodeType,
    pub(crate) logs: Vec<Log>,
    pub(crate) current_term: u32,
    pub(crate) voted_for: Option<(NodeId, u32)>,
    pub(crate) commit_index: LogMetadata,
    pub(crate) latest_log: LogMetadata,
    pub(crate) leader: Option<NodeId>,
    pub(crate) id: NodeId,
    pub(crate) timeout: u64,
    pub(crate) peers: Vec<RaftRpcClient>,
    pub(crate) last_applied: usize, // index into log_entries. MAKE SURE TO CHANGE WHEN DUMPING LOGS
    pub(crate) last_leader_contact: u64,
}

impl NodeState {
    /// finds the matching entry to match_log in self.logs, returns the index in logs[] if found, else None.
    pub fn find_matching_log_entry(&self, match_log: &LogMetadata) -> Option<usize> {
        for i in (0..self.logs.len()).rev() {
            let log_meta = self.logs.get(i).unwrap().metadata;
            if log_meta.eq(match_log) {
                return Some(i);
            }

            if log_meta.lt(match_log) {
                return None;
            }
        }

        return None;
    }

    /// returns true if timed out.
    pub fn check_timeout(&self) -> bool {
        if self.last_leader_contact + self.timeout >= get_current_time() {
            return true;
        }
        return false;
    }
}

pub struct LeaderState {
    // 05/25/2026 12:13 PM IST - WARNING: i am not actually using match_index because i am sending all the logs together (iteratively), but i probably should later.
    pub(crate) match_index: Vec<u32>, // index of the highest log known to be replicated on the follower
    pub(crate) next_log_index: u32,   // next index of the log in the leader's logs[] array
    pub(crate) catchup_tracker: HashMap<(NodeId, u32), Arc<Notify>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    Leader,
    Follower,
    Candidate,
}

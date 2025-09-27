use std::marker::PhantomData;

pub enum NodeState {
    Leader,
    Follower,
    Candidate,
}

pub type NodeId = PhantomData<String>;

pub struct RaftNode {
    state: NodeState,
    committed_logs: Vec<Log>,
    current_term: u32,
    remaining_commits: Vec<Log>,
    commit_length: u32, // usable from Leader
    current_log_index: u32,
    leader: NodeId,
    id: NodeId,
    election_timeout: u32,
    servers: Vec<NodeId>, // contains list of nodes (apart from self)
                          // TODO: also will need to track acks through a datastructure
}

pub struct Log {
    log_text: String,
    log_term: u32,
    log_index: u32, // NOTE: might not be needed
}

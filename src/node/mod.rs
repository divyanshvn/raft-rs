use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
    u64,
};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tarpc::context;
use tokio::{
    sync::{Mutex, Notify},
    time::sleep,
};
use tracing::{debug, info, warn};

use crate::{
    raft_rpc::{RaftRpc, RaftRpcClient},
    types::{LeaderState, Log, LogMetadata, NodeId, NodeState, NodeType, START},
    utils::{
        context_with_deadline, get_current_time, is_majority, just_majority, rand_election_timeout,
        MIN_TIMEOUT,
    },
};

#[derive(Clone)]
pub struct Node {
    pub(crate) state: Arc<Mutex<NodeState>>,
    pub(crate) leader_state: Arc<Mutex<Option<LeaderState>>>, // might not need if i use this state within leader's function, but what if leader goes down for just a bit, need to store this in that case, in case leader remains the leader.
}

struct AppendEntriesDetails {
    leader_id: NodeId,
    current_term: u32,
    logs: Vec<Log>,
    commit_index: LogMetadata,
    peer: RaftRpcClient,
    follower_id: usize,
}

impl Node {
    pub fn new(id: usize, peers: &[RaftRpcClient]) -> Self {
        let zero_log_metadata = LogMetadata { term: 0, index: 0 };
        let mut leader_state = None;
        let mut current_type = NodeType::Follower;

        let node_state = NodeState {
            current_type,
            logs: vec![Log {
                metadata: zero_log_metadata,
                log: START.to_string(),
            }],
            current_term: 0, // A hack. if this node is leader, then current_term = 1, else 0
            voted_for: None,
            commit_index: zero_log_metadata,
            latest_log: zero_log_metadata,
            leader: None,
            id: NodeId(id),
            timeout: rand_election_timeout(MIN_TIMEOUT),
            peers: peers.to_vec(),
            last_applied: 0,
            last_leader_contact: get_current_time(),
            state_path: None,
            failed: false,
        };

        Self {
            state: Arc::new(Mutex::new(node_state)),
            leader_state: Arc::new(Mutex::new(leader_state)),
        }
    }

    /// Open a local JSON state file. Callers must supply the same node identity.
    pub fn open(
        id: usize,
        peers: &[RaftRpcClient],
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        anyhow::ensure!(id < peers.len(), "node ID must be a peer vector position");
        let node = Self::new(id, peers);
        {
            let mut state = node.state.try_lock().unwrap();
            state.state_path = Some(path.as_ref().to_path_buf());
            match std::fs::read(path.as_ref()) {
                Ok(bytes) => {
                    let saved: crate::persistence::SavedState = serde_json::from_slice(&bytes)?;
                    saved.validate(id, peers.len())?;
                    state.current_term = saved.current_term;
                    state.voted_for = saved.voted_for;
                    state.logs = saved.logs;
                    state.latest_log = state.logs.last().unwrap().metadata;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => state.persist()?,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(node)
    }

    pub async fn get_state(&self) -> (u32, bool) {
        let state = self.state.lock().await;
        (state.current_term, state.current_type == NodeType::Leader)
    }

    pub async fn get_leader(&self) -> Option<usize> {
        let state = self.state.lock().await;
        let leader = state.leader?;
        Some(leader.0)
    }

    pub async fn start(mut self, node_type: Option<NodeType>) -> Result<()> {
        {
            let state = self.state.lock().await;
            state.healthy()?;
            anyhow::ensure!(
                state.state_path.is_none()
                    || node_type.is_none()
                    || node_type == Some(NodeType::Follower),
                "persistent nodes must start as followers"
            );
        }
        self.initialize_states(node_type).await;

        loop {
            let (node_type, id) = {
                let state = self.state.lock().await;
                {
                    state.healthy()?;
                    (state.current_type.clone(), state.id)
                }
            };
            match node_type {
                NodeType::Leader => {
                    self.initialize_leader().await;
                    // TODO: I Shouldn't be not unwrapping here.
                    // More Importantly, What should i be doing when call_append_entries returns Err?
                    self.call_append_entries(vec![]).await;

                    // NOTE: made this half of MIN_TIMEOUT to be on the safer side.
                    // need to spin up a task, that awaits for the tasks in task_queue
                    self.loop_leader(MIN_TIMEOUT / 2, &id).await;

                    debug!(node = id.0, "got out of leader loop");
                }
                NodeType::Follower => {
                    // election timeout monitor (maybe a loop based approach)
                    let timeout = rand_election_timeout(MIN_TIMEOUT);
                    debug!(node = id.0, timeout, "becoming follower now");
                    self.loop_follower(timeout, id.0).await;
                }
                NodeType::Candidate => {
                    // 05/24/2026 08:34 AM IST - TODO: how should i check the timer here
                    let notify = Arc::new(Notify::new());
                    let num_votes = Arc::new(AtomicU32::new(1));

                    let (id, current_term, peers, latest_log, timeout) = {
                        let state = self.state.lock().await;
                        info!(
                            node = state.id.0,
                            term = state.current_term,
                            "node became candidate"
                        );
                        let current_term = state.current_term;
                        let peers = state.peers.clone();

                        (
                            state.id,
                            current_term,
                            peers,
                            state.latest_log,
                            state.timeout,
                        )
                    };
                    let num_peers = peers.len();

                    for i in 0..peers.len() {
                        if i == id.0 {
                            continue;
                        }

                        let notify_cl = notify.clone();
                        let num_votes_cl = num_votes.clone();
                        let peer_cl = peers[i].clone();

                        tokio::spawn(async move {
                            let resp = peer_cl
                                .request_vote(context::current(), current_term, id, latest_log)
                                .await;

                            if let Ok(Ok((term, voted))) = resp {
                                if voted {
                                    let num_votes = num_votes_cl.fetch_add(1, Ordering::SeqCst) + 1;
                                    if num_votes == ((num_peers + 1).div_ceil(2) as u32) {
                                        notify_cl.notify_one();
                                    }
                                    return;
                                }

                                // when voted is false and current_term is lagging behind
                                if term > current_term {
                                    // change back to follower
                                    // change num_votes to 0 , to signal this.
                                    // 06/17/2026 12:26 PM IST - TODO: When should this be turned back to follower?
                                    num_votes_cl.store(0, Ordering::SeqCst);
                                    notify_cl.notify_one();
                                }
                            }
                        });
                    }

                    let notify_cl = notify.clone();
                    let start_time = get_current_time();
                    let election_timeout = rand_election_timeout(MIN_TIMEOUT);
                    // spawn an election timout
                    tokio::spawn(async move {
                        // 05/26/2026 02:24 PM IST - WARN: need to check whether this wakes up exactly after election_timeout secs.
                        tokio::time::sleep(Duration::from_millis(election_timeout)).await;
                        notify_cl.notify_one();
                    });

                    notify.notified().await;

                    let num_votes = num_votes.load(Ordering::SeqCst);
                    if num_votes == 0 {
                        // outdated term signal
                        // TODO: have a term AtomicUint that can be used to get the updated value.
                        // Convert to Follower
                        continue;
                    }

                    if is_majority(num_votes, num_peers) {
                        // convert to leader
                        let (node_id, term) = {
                            let state = self.state.lock().await;
                            (state.id.0, state.current_term)
                        };
                        info!(node = node_id, term, num_votes, "election won");
                        {
                            let mut state = self.state.lock().await;
                            state.current_type = NodeType::Leader;
                        }
                        continue;
                    }

                    if get_current_time() - start_time >= election_timeout {
                        // check current NodeType
                        let mut state_locked = self.state.lock().await;
                        if state_locked.current_type == NodeType::Follower {
                            continue;
                        }

                        // Election to be conducted again
                        state_locked.current_term += 1;
                        state_locked.voted_for = Some((state_locked.id, state_locked.current_term));
                        state_locked.persist()?;
                        continue;
                    }
                }
            }
        }
    }

    pub async fn call_append_entries(&mut self, logs_str: Vec<String>) -> Result<()> {
        let (peers, leader_id, logs, commit_index, current_term, prev_log) = {
            let mut state = self.state.lock().await;
            state.healthy()?;

            if state.current_type != NodeType::Leader {
                return Err(anyhow!("Current_node : {:?}, current_state: {:?}, call_append_entries called by non-leader", state.id, state.current_type));
            }
            let mut leader_state = self.leader_state.lock().await;

            let mut logs = Vec::new();
            let mut next_log_index = leader_state.as_ref().unwrap().next_log_index;

            let mut log_metadata = state.latest_log;
            for log_str in logs_str {
                log_metadata = LogMetadata {
                    term: state.current_term,
                    index: next_log_index,
                };
                next_log_index += 1;
                logs.push(Log {
                    log: log_str,
                    metadata: log_metadata,
                });
            }
            leader_state.as_mut().unwrap().next_log_index = next_log_index;

            let prev_log = state.latest_log;
            state.logs.append(&mut logs.clone());

            state.latest_log = log_metadata;
            state.persist()?;
            state.last_leader_contact = get_current_time();
            (
                state.peers.clone(),
                state.id,
                logs,
                state.commit_index,
                state.current_term,
                prev_log,
            )
        };
        // NOTE: Locks DROPPED here
        let count = ConsensusCount {
            accepted: Arc::new(AtomicU32::new(1)),
            failed: Arc::new(AtomicU32::new(0)),
        };
        let notify = Arc::new(Notify::new());
        let mut tasks = Vec::new();

        for (i, peer) in peers.iter().enumerate() {
            // skip the leader ofcourse
            if i == leader_id.0 {
                continue;
            }

            let count_cl = count.clone();
            let peer_c = peer.clone();
            let state_c = self.state.clone();
            let leader_state_c = self.leader_state.clone();
            // 06/14/2026 02:27 PM IST - TODO: can just pass Arc of logs
            let logs_cl = logs.clone();

            let notify_cl = notify.clone();
            let task = tokio::spawn(async move {
                debug!("spawning append_entries task node={}", i);
                append_entry_task(
                    AppendEntriesDetails {
                        leader_id,
                        current_term,
                        logs: logs_cl,
                        commit_index,
                        peer: peer_c,
                        follower_id: i,
                    },
                    count_cl,
                    prev_log,
                    state_c,
                    leader_state_c,
                    notify_cl,
                )
                .await;
                debug!("append_entries task finished node={}", i);
            });

            tasks.push(task);
        }

        notify.notified().await;

        self.state.lock().await.healthy()?;
        if is_majority(count.accepted.load(Ordering::SeqCst), peers.len()) {
            Ok(())
        } else {
            Err(anyhow!("Majority of append_entries failed"))
        }
    }

    async fn initialize_states(&self, node_type: Option<NodeType>) {
        let mut state = self.state.lock().await;

        state.current_type = node_type.unwrap_or(NodeType::Follower);
        if state.current_type == NodeType::Leader {
            let mut mu = self.leader_state.lock().await;
            state.current_term = 1;
            *mu = Some(LeaderState {
                match_index: vec![0; state.peers.len()],
                next_log_index: 1,
                catchup_tracker: HashMap::new(),
            });
        }
    }

    async fn initialize_leader(&self) {
        let mut state = self.state.lock().await;
        state.last_leader_contact = get_current_time();
        debug!(
            node = state.id.0,
            term = state.current_term,
            latest_log_index = state.latest_log.index,
            "node is leader"
        );
        *(self.leader_state.lock().await) = Some(LeaderState {
            match_index: vec![0; state.peers.len()],
            next_log_index: state.latest_log.index + 1,
            catchup_tracker: HashMap::new(),
        });
    }

    async fn loop_leader(&mut self, mut timeout: u64, leader_id: &NodeId) {
        loop {
            let task = tokio::spawn(async move {
                sleep(Duration::from_millis(timeout)).await;
            });
            task.await.unwrap();
            debug!(node = leader_id.0, "leader timer up");

            let (last_leader_contact, current_type) = {
                let state = self.state.lock().await;
                (state.last_leader_contact, state.current_type.clone())
            };
            if current_type != NodeType::Leader {
                debug!(
                    node = leader_id.0,
                    state = format!("{:?}", current_type),
                    "no longer the leader"
                );
                break;
            }
            let current_time = get_current_time();
            let time_since_leader_contact = current_time - last_leader_contact;

            timeout = if time_since_leader_contact >= timeout {
                info!(
                    node = leader_id.0,
                    state = format!("{:?}", current_type),
                    "calling empty append_entries"
                );
                debug!(node = leader_id.0, "calling append_entries");
                // TODO: I Shouldn't be not unwrapping here.
                self.call_append_entries(vec![]).await;
                debug!(node = leader_id.0, "append_entries done");
                timeout
            } else {
                timeout - time_since_leader_contact
            };
        }
    }

    async fn loop_follower(&mut self, mut timeout: u64, follower_id: usize) {
        loop {
            debug!(timeout, node = follower_id, "going to sleep");
            let current_time = Instant::now();
            let task = tokio::spawn(async move {
                sleep(Duration::from_millis(timeout)).await;
            });
            task.await.unwrap();
            debug!(
                timeout,
                node = follower_id,
                "got out of sleep after {} ms",
                current_time.elapsed().as_millis()
            );

            let mut state = self.state.lock().await;
            let current_time = get_current_time();
            let time_since_leader_contact = current_time - state.last_leader_contact;
            let new_election_timeout =
                rand_election_timeout(MIN_TIMEOUT) as i64 - time_since_leader_contact as i64;

            // if time since the last leader contact > the timeout, then it needs to become a candidate.
            if time_since_leader_contact > timeout || new_election_timeout <= 0 {
                state.current_type = NodeType::Candidate;
                state.current_term += 1;
                state.voted_for = Some((state.id, state.current_term));
                if state.persist().is_err() {
                    return;
                }
                info!(
                    node_id = state.id.0,
                    term = state.current_term,
                    timeout_ms = timeout,
                    "election timeout elapsed"
                );
                break;
            }

            // While, if the leader has contacted in between, then timeout restarts. But it should be such that the timer restarted when the new leader contacted it, not when the old timer finishes. Therefore, it will make more sense to deduct the time since last leader contact. Is that guaranteed to be > 0, might not be since this a distribution from timeout to 2*timeout, therefore if it is greater than such, in that case one should become a candidate.
            timeout = new_election_timeout as u64;
            state.timeout = timeout;
        }
    }
}

#[derive(Clone)]
struct ConsensusCount {
    accepted: Arc<AtomicU32>,
    failed: Arc<AtomicU32>,
}

// called by the leader, handles sending appendEntries RPC to follower
// AI_AGENT: suggest some ergonomic minimal way to reduce the number of arguments
async fn append_entry_task(
    append_details: AppendEntriesDetails,
    count: ConsensusCount,
    prev_log: LogMetadata,
    state: Arc<Mutex<NodeState>>,
    leader_state: Arc<Mutex<Option<LeaderState>>>,
    notify: Arc<Notify>,
) {
    let leader_id = append_details.leader_id;
    let current_term = append_details.current_term;
    let logs = append_details.logs;
    let peer = append_details.peer;
    let commit_index = append_details.commit_index;
    let follower_id = append_details.follower_id;
    let latest_metadata = if logs.is_empty() {
        prev_log
    } else {
        logs.last().unwrap().metadata
    };

    debug!(
        node = follower_id,
        logs = format!("{:?}", logs),
        "sending append_entries"
    );
    let response = peer
        .append_entries(
            context_with_deadline(Duration::from_millis(MIN_TIMEOUT)),
            current_term,
            leader_id,
            prev_log,
            logs.clone(),
            commit_index,
        )
        .await;

    debug!(
        node = follower_id,
        logs = format!("{:?}", logs),
        "append_entries sent"
    );

    let response = match response {
        Ok(Ok(resp)) => resp,
        Err(err) => {
            handle_rpc_failure(count.failed, notify, state).await;
            debug!(
                node = follower_id,
                logs = format!("{:?}", logs),
                term = current_term,
                leader = leader_id.0,
                error = %err,
                "append_entries rpc failed"
            );
            return;
        }
        Ok(Err(err)) => {
            handle_rpc_failure(count.failed, notify, state).await;
            debug!(
                node = follower_id,
                logs = format!("{:?}", logs),
                term = current_term,
                leader = leader_id.0,
                error = %err,
                "append_entries rejected by service"
            );
            return;
        }
    };
    // 05/24/2026 06:28 PM IST - TODO: might not want to unwrap here.

    if response.1 {
        // append_entries is successful
        handle_successful_append(state, leader_state, count.accepted, latest_metadata, notify)
            .await;
        return;
    }

    // Handling for the scenarion wherin the response's term > current_term, which means that this node is no longer the leader.
    if response.0 > current_term {
        warn!(
            leader_id = leader_id.0,
            id = follower_id,
            current_term,
            peer_term = response.0,
            "leader observed higher term"
        );
        handle_leader_removal(
            state.clone(),
            leader_state.clone(),
            count,
            notify,
            response.0,
        )
        .await;
        return;
    }

    if !node_is_leader(&state).await {
        handle_rpc_failure(count.failed, notify, state).await;
        debug!(
            node = follower_id,
            logs = format!("{:?}", logs),
            "node is not leader, aborting append_entries"
        );
        return;
    }

    // checking if the catchup is already undergoing
    if let Some(catchup_notify) = check_catchup(&leader_state, leader_id, latest_metadata).await {
        catchup_notify.notified().await;
        handle_successful_append(state, leader_state, count.accepted, latest_metadata, notify)
            .await;
        return;
    };

    // handle failed append_entries, i.e. the follower needs to catch up
    handle_follower_catchup(
        state,
        leader_state,
        AppendEntriesDetails {
            leader_id,
            current_term,
            logs,
            commit_index,
            peer,
            follower_id,
        },
        count,
        notify,
        latest_metadata,
    )
    .await;
}

async fn handle_follower_catchup(
    state: Arc<Mutex<NodeState>>,
    leader_state: Arc<Mutex<Option<LeaderState>>>,
    append_details: AppendEntriesDetails,
    count: ConsensusCount,
    notify: Arc<Notify>,
    latest_metadata: LogMetadata,
) {
    let mut entries = VecDeque::from(append_details.logs);
    let catch_notify = Arc::new(Notify::new());
    insert_catchup_tracker(
        &leader_state,
        append_details.follower_id,
        latest_metadata,
        catch_notify.clone(),
    )
    .await;

    let logs = { state.lock().await.logs.clone() };
    // iteration starts from 1 because at initialization all logs contain the "START" log.
    for j in (1..(logs.len() - 1)).rev() {
        // check_state_and_handle_rpc_failure(count.failed, notify.clone(), state.clone());
        let prev_log = logs[j - 1].metadata;
        entries.push_front(logs[j].clone());

        let response = append_details
            .peer
            .append_entries(
                context::current(),
                append_details.current_term,
                append_details.leader_id,
                prev_log,
                entries.clone().into(),
                append_details.commit_index,
            )
            .await;

        // NOTE: I am not sure about the case where the node isn't the leader anymore, but this iteration keeps on going.

        let response = match response {
            Ok(Ok(resp)) => resp,
            _ => return,
        };
        if response.1 {
            // append_entries is successful
            handle_successful_append(state, leader_state, count.accepted, latest_metadata, notify)
                .await;
            catch_notify.notify_waiters();
            return;
        }
    }
}

async fn handle_leader_removal(
    state: Arc<Mutex<NodeState>>,
    leader_state: Arc<Mutex<Option<LeaderState>>>,
    count: ConsensusCount,
    notify: Arc<Notify>,
    current_term: u32,
) {
    let mut state_locked = state.lock().await;
    state_locked.current_type = NodeType::Follower;
    state_locked.current_term = current_term;
    if state_locked.persist().is_err() {
        notify.notify_one();
        return;
    }
    let failed = count
        .failed
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        + 1;
    if is_majority(failed, state_locked.peers.len()) {
        notify.notify_one();
    }
    let mut leader_state_locked = leader_state.lock().await;
    // TODO: what happens to the heap allocation when i make it None?
    *leader_state_locked = None;
}

async fn node_is_leader(state: &Mutex<NodeState>) -> bool {
    let state_locked = state.lock().await;
    state_locked.current_type == NodeType::Leader
}

async fn insert_catchup_tracker(
    leader_state: &Mutex<Option<LeaderState>>,
    id: usize,
    latest_metadata: LogMetadata,
    catchup_notify: Arc<Notify>,
) {
    let mut leader_state_locked = leader_state.lock().await;
    leader_state_locked
        .as_mut()
        .unwrap()
        .catchup_tracker
        .insert((NodeId(id), latest_metadata.index), catchup_notify);
}

async fn check_catchup(
    leader_state: &Mutex<Option<LeaderState>>,
    id: NodeId,
    latest_metadata: LogMetadata,
) -> Option<Arc<Notify>> {
    let leader_state_locked = leader_state.lock().await;
    let catchup_tracker = &leader_state_locked.as_ref().unwrap().catchup_tracker;
    catchup_tracker.get(&(id, latest_metadata.index)).cloned()
}

async fn check_state_and_handle_rpc_failure(
    failed: Arc<AtomicU32>,
    notify: Arc<Notify>,
    state: Arc<Mutex<NodeState>>,
) {
    let state_lock = state.lock().await;
    if state_lock.current_type == NodeType::Leader {
        return;
    }
    let num_nodes = state_lock.peers.len();
    let failed = failed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    if is_majority(failed, num_nodes) {
        notify.notify_one();
    }
}

async fn handle_rpc_failure(
    failed: Arc<AtomicU32>,
    notify: Arc<Notify>,
    state: Arc<Mutex<NodeState>>,
) {
    let num_nodes = { state.lock().await.peers.len() };
    let failed = failed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    if is_majority(failed, num_nodes) {
        notify.notify_one();
    }
}

// called by leader
async fn handle_successful_append(
    state: Arc<Mutex<NodeState>>,
    leader_state: Arc<Mutex<Option<LeaderState>>>,
    accepted_count: Arc<AtomicU32>,
    metadata: LogMetadata,
    notify: Arc<Notify>,
) {
    let mut state_lock = state.lock().await;
    // TODO: why is this causing issue?
    if state_lock.current_type != NodeType::Leader {
        // debug!(
        //     node = state_lock.id.0,
        //     "not leader, should be ignoring successful append",
        // );
        return;
    }
    let count = accepted_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

    if just_majority(count, state_lock.peers.len()) {
        // signal that majority is achieved
        // TODO: might not need to mutate state every time
        if metadata.gt(&state_lock.commit_index) {
            state_lock.commit_index = metadata;
        }
        notify.notify_one();
    }

    drop(state_lock);

    // let mut binding = leader_state.lock().await;
    // 06/19/2026 09:32 PM IST - TODO: fix this bug of leader_state becoming None

    // let leader_state = binding.as_mut().unwrap();
    // leader_state.match_index[i] = metadata.index;
    // drop(binding)
}

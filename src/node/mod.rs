use core::range;
use std::{
    collections::VecDeque,
    ops::DerefMut,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tarpc::context;
use tokio::{
    sync::Notify,
    task::JoinHandle,
    time::{sleep, Sleep},
};
use tracing::{event, Level};

use crate::{
    client_rpc::ClientRpc,
    raft_rpc::{RaftRpc, RaftRpcClient},
    types1::{LeaderState, Log, LogMetadata, NodeId, NodeState, NodeType, START},
    utils::{exceeds_majority, get_current_time, rand_election_timeout},
};

#[derive(Clone)]
pub struct Node {
    pub(crate) state: Arc<Mutex<NodeState>>,
    pub(crate) leader_state: Arc<Mutex<Option<LeaderState>>>, // might not need if i use this state within leader's function, but what if leader goes down for just a bit, need to store this in that case, in case leader remains the leader.
}

impl Node {
    pub fn new(id: usize, min_timeout: u64, peers: &[RaftRpcClient]) -> Self {
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
            min_timeout,
            peers: peers.to_vec(),
            last_applied: 0,
            last_leader_contact: get_current_time(),
        };

        Self {
            state: Arc::new(Mutex::new(node_state)),
            leader_state: Arc::new(Mutex::new(leader_state)),
        }
    }

    pub fn get_state(&self) -> (u32, bool) {
        let state = self.state.lock().unwrap();
        (state.current_term, state.current_type == NodeType::Leader)
    }

    pub fn get_leader(&self) -> Option<usize> {
        let state = self.state.lock().unwrap();
        let leader = state.leader?;
        Some(leader.0)
    }

    pub async fn start(mut self, node_type: Option<NodeType>) -> Result<()> {
        let mut min_timeout;
        {
            let mut state = self.state.lock().unwrap();

            state.current_type = node_type.unwrap_or(NodeType::Follower);
            if state.current_type == NodeType::Leader {
                let mut mu = self.leader_state.lock().unwrap();
                state.current_term = 1;
                *mu = Some(LeaderState {
                    match_index: vec![0; state.peers.len()],
                    next_log_index: 1,
                });
            }
            min_timeout = state.min_timeout;
        }

        loop {
            let node_type = { self.state.lock().unwrap().current_type.clone() };
            match node_type {
                NodeType::Leader => {
                    {
                        let mut state = self.state.lock().unwrap();
                        state.last_leader_contact = get_current_time();
                        event!(
                            Level::DEBUG,
                            "node {} is the leader now with term: {}",
                            state.id.0,
                            state.current_term
                        );
                        *(self.leader_state.lock().as_deref_mut().unwrap()) = Some(LeaderState {
                            match_index: vec![0; state.peers.len()],
                            next_log_index: state.latest_log.index + 1,
                        });
                    }
                    // initialize (or even re-initialize in some state vars case) the state variables accordingly
                    self.call_append_entries(vec![]).await.unwrap();
                    // call appendEntries for all. (maybe if possible call its own reply_log)

                    // need to spin up a task, that awaits for the tasks in task_queue
                    let mut timeout = min_timeout;
                    loop {
                        let task = tokio::spawn(async move {
                            sleep(Duration::from_millis(timeout)).await;
                        });
                        task.await.unwrap();

                        let (last_leader_contact, current_type) = {
                            let state = self.state.lock().unwrap();
                            (state.last_leader_contact, state.current_type.clone())
                        };
                        if current_type != NodeType::Leader {
                            break;
                        }
                        let current_time = get_current_time();
                        let time_since_leader_contact = current_time - last_leader_contact;

                        timeout = if time_since_leader_contact >= min_timeout {
                            self.call_append_entries(vec![]).await.unwrap();
                            min_timeout
                        } else {
                            min_timeout - time_since_leader_contact
                        }
                    }
                }
                NodeType::Follower => {
                    // election timeout monitor (maybe a loop based approach)
                    let mut timeout = rand_election_timeout(min_timeout);
                    loop {
                        let task = tokio::spawn(async move {
                            sleep(Duration::from_millis(timeout)).await;
                        });

                        task.await.unwrap();

                        let mut state = self.state.lock().unwrap();
                        let current_time = get_current_time();
                        let time_since_leader_contact = current_time - state.last_leader_contact;

                        if time_since_leader_contact > timeout {
                            state.current_type = NodeType::Candidate;
                            state.current_term += 1;
                            state.voted_for = Some((state.id, state.current_term));
                            break;
                        }

                        // otherwise, restart timer
                        let new_election_timeout = rand_election_timeout(min_timeout);
                        event!(
                            Level::INFO,
                            "new election timeout {}, old {}, time_since_leader_contact {}",
                            new_election_timeout,
                            timeout,
                            time_since_leader_contact
                        );
                        timeout = new_election_timeout - (time_since_leader_contact - timeout);
                    }
                }
                NodeType::Candidate => {
                    // 05/24/2026 08:34 AM IST - TODO: how should i check the timer here
                    let notify = Arc::new(Notify::new());
                    let num_votes = Arc::new(AtomicU32::new(1));

                    let (id, current_term, peers, latest_log, min_timeout) = {
                        let state = self.state.lock().unwrap();
                        event!(
                            Level::DEBUG,
                            "node {} is candidate for term: {}",
                            state.id.0,
                            state.current_term
                        );
                        let current_term = state.current_term;
                        let peers = state.peers.clone();

                        (
                            state.id,
                            current_term,
                            peers,
                            state.latest_log,
                            state.min_timeout,
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
                    let election_timeout = rand_election_timeout(min_timeout);
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

                    if exceeds_majority(num_votes, num_peers) {
                        // convert to leader
                        let (node_id, term) = {
                            let state = self.state.lock().unwrap();
                            (state.id.0, state.current_term)
                        };
                        {
                            let mut state = self.state.lock().unwrap();
                            state.current_type = NodeType::Leader;
                        }
                        continue;
                    }

                    if get_current_time() - start_time >= election_timeout {
                        // check current NodeType
                        let mut state_locked = self.state.lock().unwrap();
                        if state_locked.current_type == NodeType::Follower {
                            continue;
                        }

                        // Election to be conducted again
                        state_locked.current_term += 1;
                        state_locked.voted_for = Some((state_locked.id, state_locked.current_term));
                        continue;
                    }
                }
            }
        }
    }

    pub async fn call_append_entries(&mut self, logs_str: Vec<String>) -> Result<()> {
        let (peers, id, logs, commit_index, current_term, prev_log) = {
            let mut state = self.state.lock().unwrap();

            if state.current_type != NodeType::Leader {
                return Err(anyhow!("Current_node : {:?}, current_state: {:?}, call_append_entries called by non-leader", state.id, state.current_type));
            }
            let mut leader_state = self.leader_state.lock().unwrap();

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
        let replica_count = Arc::new(AtomicU32::new(1));
        let notify = Arc::new(Notify::new());
        let mut tasks = Vec::new();

        for (i, peer) in peers.iter().enumerate() {
            // skip the leader ofcourse
            if i == id.0 {
                continue;
            }

            let rc_cl = replica_count.clone();
            let peer_c = peer.clone();
            let state_c = self.state.clone();
            let leader_state_c = self.leader_state.clone();
            // 06/14/2026 02:27 PM IST - TODO: can just pass Arc of logs
            let logs_cl = logs.clone();

            let notify_cl = notify.clone();
            let task = tokio::spawn(async move {
                append_entry_task(
                    logs_cl,
                    peer_c,
                    rc_cl,
                    id,
                    commit_index,
                    current_term,
                    prev_log,
                    i,
                    state_c,
                    leader_state_c,
                    notify_cl,
                )
                .await;
            });

            tasks.push(task);
        }

        notify.notified().await;

        Ok(())
    }
}

// called by the leader, handles sending appendEntries RPC to follower
async fn append_entry_task(
    logs: Vec<Log>,
    peer: RaftRpcClient,
    rc_cl: Arc<AtomicU32>,
    id: NodeId,
    commit_index: LogMetadata,
    current_term: u32,
    prev_log: LogMetadata,
    peer_index: usize,
    state: Arc<Mutex<NodeState>>,
    leader_state: Arc<Mutex<Option<LeaderState>>>,
    notify: Arc<Notify>,
) {
    // WARNING: there are too many arguments in this function call. I did this initially just so that i don't have to lock and read state again. I need to rethink this strategy later.
    let latest_metadata = if logs.is_empty() {
        prev_log
    } else {
        logs.last().unwrap().metadata
    };
    let response = peer
        .append_entries(
            context::current(),
            current_term,
            id,
            prev_log,
            logs.clone(),
            commit_index,
        )
        .await;

    let response = match response {
        Ok(Ok(resp)) => resp,
        _ => return,
    };
    // 05/24/2026 06:28 PM IST - TODO: might not want to unwrap here.

    if response.1 {
        // append_entries is successful
        handle_successful_append(state, leader_state, rc_cl, latest_metadata, id.0, notify);
        return;
    }

    // Handling for the scenarion wherin the response's term > current_term, which means that this node is no longer the leader.
    if response.0 > current_term {
        let mut state_locked = state.lock().unwrap();
        state_locked.current_type = NodeType::Follower;
        state_locked.current_term = response.0;
        let mut leader_state_locked = leader_state.lock().unwrap();
        *leader_state_locked = None;
        return;
    }

    // handle failed append_entries, i.e. the follower needs to catch up
    let mut entries = VecDeque::from(logs);
    let logs = { state.lock().unwrap().logs.clone() };
    // iteration starts from 1 because at initialization all logs contain the "START" log.
    for j in (1..(logs.len() - 1)).rev() {
        let prev_log = logs[j - 1].metadata;
        entries.push_front(logs[j].clone());
        // 05/25/2026 12:34 PM IST - TODO: This is slightly inefficient. Scenario: node j is already being caught up through another task, but here, i am doing this iteration again.
        // 05/25/2026 12:35 PM IST - TODO: MAJOR!! THIS MIGHT EVEN LEAD TO AN ISSUE , where in-between of these iterations, another task updates to a more updated version of logs, but then this decreases them.

        // update one by one?
        let response = peer
            .append_entries(
                context::current(),
                current_term,
                id,
                prev_log,
                entries.clone().into(),
                commit_index,
            )
            .await;

        let response = match response {
            Ok(Ok(resp)) => resp,
            _ => return,
        };
        if response.1 {
            // append_entries is successful
            handle_successful_append(
                state,
                leader_state,
                rc_cl,
                latest_metadata,
                peer_index,
                notify,
            );
            return;
        }
    }
}

// called by leader
fn handle_successful_append(
    state: Arc<Mutex<NodeState>>,
    leader_state: Arc<Mutex<Option<LeaderState>>>,
    replica_count: Arc<AtomicU32>,
    metadata: LogMetadata,
    i: usize,
    notify: Arc<Notify>,
) {
    let mut state_lock = state.lock().unwrap();
    let count = replica_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

    if count == ((state_lock.peers.len() + 1).div_ceil(2) as u32) {
        // signal that majority is achieved
        if metadata.gt(&state_lock.commit_index) {
            state_lock.commit_index = metadata;
        }
        notify.notify_one();
    }

    drop(state_lock);

    let mut binding = leader_state.lock().unwrap();
    // 06/19/2026 09:32 PM IST - TODO: fix this bug of leader_state becoming None

    // let leader_state = binding.as_mut().unwrap();
    // leader_state.match_index[i] = metadata.index;
    drop(binding)
}

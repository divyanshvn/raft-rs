use anyhow::Result;
use tracing::{debug, info, warn};

use crate::{
    node::Node,
    types1::{Log, LogMetadata, NodeId, NodeType},
    utils::{get_current_time, MIN_TIMEOUT},
};

#[tarpc::service]
pub trait RaftRpc {
    // leader sends to follower
    async fn append_entries(
        term: u32,
        leader_id: NodeId,
        prev_log_metadata: LogMetadata,
        logs: Vec<Log>,
        leader_commit: LogMetadata,
    ) -> Result<(u32, bool)>; // returns (current_term, bool for success)

    // candidate sends to peers
    async fn request_vote(
        term: u32,
        candidate_id: NodeId,
        last_log_metadata: LogMetadata,
    ) -> Result<(u32, bool)>; // returns (current_term, bool if vote granted)
}

impl RaftRpc for Node {
    async fn request_vote(
        self,
        context: tarpc::context::Context,
        term: u32,
        candidate_id: NodeId,
        latest_log_metadata: LogMetadata,
    ) -> anyhow::Result<(u32, bool)> {
        let mut state = self.state.lock().unwrap();
        if term <= state.current_term {
            debug!(
                node = state.id.0,
                candidate_id = candidate_id.0,
                requested_term = term,
                current_term = state.current_term,
                "rejecting vote for stale term"
            );
            return Ok((state.current_term, false));
        }

        // 05/23/2026 02:37 PM IST - TODO: need to add LeaderStickiness part here. Not yet added.
        // Leader stickiness, wherein requestToVote is rejected if the follower thinks that a leader still exists.
        if state.last_leader_contact + MIN_TIMEOUT >= get_current_time() {
            debug!(
                node = state.id.0,
                candidate_id = candidate_id.0,
                requested_term = term,
                "rejecting vote while leader contact is fresh"
            );
            return Ok((state.current_term, false));
        }

        if state.current_type == NodeType::Leader {
            // revert to follower
            info!(
                node = state.id.0,
                old_term = state.current_term,
                new_term = term,
                "leader stepping down after vote request"
            );
            state.current_type = NodeType::Follower;
            state.current_term = term;
            // 05/25/2026 11:29 PM IST - TODO: should leader start its own election timer after this?

            // TODO: fix this bug, where leader_state will be None , but then it reflects

            // let mut leader_state = self.leader_state.lock().unwrap();
            // *leader_state = None;
        }

        if let Some(vote) = state.voted_for {
            // 05/23/2026 02:34 PM IST - NOTE: what if the term that it had voted for had been a previous one ? and that election didn't reach a majority vote. That is covered here.
            let vote_term = vote.1;

            // candidate's term not greater than the voted term, hence we have already voted.
            if vote_term >= term {
                debug!(
                    node = state.id.0,
                    candidate_id = candidate_id.0,
                    requested_term = term,
                    voted_term = vote_term,
                    "rejecting vote because node already voted"
                );
                return Ok((state.current_term, false));
            }
        }

        // log up-to-date check
        if state.latest_log.gt(&latest_log_metadata) {
            debug!(
                node = state.id.0,
                candidate_id = candidate_id.0,
                requested_term = term,
                candidate_log_index = latest_log_metadata.index,
                candidate_log_term = latest_log_metadata.term,
                latest_log_index = state.latest_log.index,
                latest_log_term = state.latest_log.term,
                "rejecting vote because candidate log is behind"
            );
            return Ok((state.current_term, false));
        }

        state.voted_for = Some((candidate_id, term));
        info!(
            node = state.id.0,
            candidate_id = candidate_id.0,
            term,
            "vote granted"
        );

        return Ok((state.current_term, true));
    }

    async fn append_entries(
        self,
        context: tarpc::context::Context,
        term: u32,
        leader_id: NodeId,
        prev_log_metadata: LogMetadata,
        logs: Vec<Log>,
        leader_commit: LogMetadata,
    ) -> anyhow::Result<(u32, bool)> {
        let mut state = self.state.lock().unwrap();

        // TODO: remove this later
        if state.current_term == term && state.current_type == NodeType::Leader {
            warn!(
                sender = leader_id.0,
                sender_term = term,
                current_term = state.current_term,
                receiver = state.id.0,
                // current_type = state.current_type,
                "something is wrong here. append_entries from invalid node",
            );
        }
        if state.current_term > term {
            debug!(
                node = state.id.0,
                leader_id = leader_id.0,
                requested_term = term,
                current_term = state.current_term,
                "rejecting append_entries for stale term"
            );
            return Ok((state.current_term, false));
        }
        // 05/25/2026 02:04 PM IST - TODO: reset election timer
        if state.current_type == NodeType::Leader {
            debug!(node = state.id.0, "Outdated leader");
        }

        // Handles both outdated-term followers and competing candidates who lost.
        if state.current_type != NodeType::Follower
            || state.leader.map(|leader| leader.0) != Some(leader_id.0)
        {
            info!(
                node = state.id.0,
                leader_id = leader_id.0,
                term,
                "accepted leader"
            );
        }
        state.voted_for = None;
        state.current_term = term;
        state.leader = Some(leader_id);
        state.current_type = NodeType::Follower;

        state.last_leader_contact = get_current_time();

        if state.latest_log != prev_log_metadata {
            let match_index = state.find_matching_log_entry(&prev_log_metadata);

            if match_index.is_none() {
                // NOTE: Wrong hypothesis: there is no conflicting entry , but the node lacks latest entries.
                // It's just that there might be conflicting entries but they can only be found when we get to know more(preceding) log entries
                debug!(
                    node = state.id.0,
                    leader_id = leader_id.0,
                    prev_log_index = prev_log_metadata.index,
                    prev_log_term = prev_log_metadata.term,
                    "rejecting append_entries because previous log was not found"
                );
                return Ok((state.current_term, false));
            }

            // prune all logs after match_index
            debug!(
                node = state.id.0,
                leader_id = leader_id.0,
                match_index = match_index.unwrap(),
                "truncating conflicting log entries"
            );
            state.logs.truncate(match_index.unwrap() + 1);
        }

        // append new entries
        let appended = logs.len();
        state.logs.append(&mut logs.clone());
        // NOTE: assuming that the logs[] arrray isn't ever empty
        state.latest_log = state.logs.last().unwrap().metadata;
        debug!(
            node = state.id.0,
            leader_id = leader_id.0,
            appended,
            latest_log_index = state.latest_log.index,
            latest_log_term = state.latest_log.term,
            logs = format!("{:?}", logs),
            state = format!("{:?}", state.current_type),
            "append_entries accepted"
        );

        // 05/23/2026 04:11 PM IST - TODO: RESET TIMER OF ELECTION

        // update commit_index and apply the unapplied committed entries.
        // NOTE: i know that when appendEntries is used for catching up , if leader passes its original commit_index , then it migth not be yet present in the follower node. And therefore it might be better to update it to the latest log entry in logs[] ... but even if it's set to leader_commit , then too it will keep applying the entries till it reaches end of logs[] which has the same effect.
        state.commit_index = leader_commit;
        if state.logs[state.last_applied].metadata != state.commit_index {
            for i in state.last_applied + 1..state.logs.len() {
                // 05/23/2026 04:14 PM IST - TODO: Call the "Apply log" method here
                if state.logs[i].metadata == state.commit_index {
                    break;
                }
            }
        }

        return Ok((state.current_term, true));
    }
}

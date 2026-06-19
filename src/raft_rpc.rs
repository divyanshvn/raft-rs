use std::time::{self, UNIX_EPOCH};

use anyhow::Result;

use crate::{
    node::Node,
    types1::{Log, LogMetadata, NodeId, NodeType},
    utils::get_current_time,
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
        println!(
            "received request_to_vote at node {} from node {}",
            state.id.0, candidate_id.0
        );
        if term <= state.current_term {
            return Ok((state.current_term, false));
        }

        // 05/23/2026 02:37 PM IST - TODO: need to add LeaderStickiness part here. Not yet added.
        // Leader stickiness, wherein requestToVote is rejected if the follower thinks that a leader still exists.

        let current_time = get_current_time();
        if state.last_leader_contact + state.min_timeout > current_time {
            return Ok((state.current_term, false));
        }

        if state.current_type == NodeType::Leader {
            // revert to follower
            state.current_type = NodeType::Follower;
            state.current_term = term;
            // 05/25/2026 11:29 PM IST - TODO: should leader start its own election timer after this?

            let mut leader_state = self.leader_state.lock().unwrap();
            *leader_state = None;
        }

        if let Some(vote) = state.voted_for {
            // 05/23/2026 02:34 PM IST - NOTE: what if the term that it had voted for had been a previous one ? and that election didn't reach a majority vote. That is covered here.
            let vote_term = vote.1;

            // candidate's term not greater than the voted term, hence we have already voted.
            if vote_term >= term {
                return Ok((state.current_term, false));
            }
        }

        // log up-to-date check
        if state.latest_log.gt(&latest_log_metadata) {
            return Ok((state.current_term, false));
        }

        state.voted_for = Some((candidate_id, term));

        return Ok((state.current_term, true));
    }

    async fn append_entries(
        self,
        context: tarpc::context::Context,
        term: u32,
        leader_id: NodeId,
        prev_log_metadata: LogMetadata,
        mut logs: Vec<Log>,
        leader_commit: LogMetadata,
    ) -> anyhow::Result<(u32, bool)> {
        let mut state = self.state.lock().unwrap();

        // println!("append_entries called of id {:?}, leader term: {term}, prev_log_metadata: {prev_log_metadata:?}, new logs: {logs :?}", state.id);
        if state.current_term > term {
            // println!("append_entries failed 2, for peer {:?}", state.id);
            return Ok((state.current_term, false));
        }
        // 05/25/2026 02:04 PM IST - TODO: reset election timer

        // Handles both outdated-term followers and competing candidates who lost.
        if state.current_term <= term {
            state.voted_for = None;
            state.current_term = term;
            state.leader = Some(leader_id);
            state.current_type = NodeType::Follower;
        }

        state.last_leader_contact = get_current_time();

        if state.latest_log.ne(&prev_log_metadata) {
            let match_index = state.find_matching_log_entry(&prev_log_metadata);

            if match_index.is_none() {
                // NOTE: Wrong hypothesis: there is no conflicting entry , but the node lacks latest entries.
                // It's just that there might be conflicting entries but they can only be found when we get to know more(preceding) log entries
                // println!("append_entries failed 1, for peer {:?}. Latest log: {:?} vs prev_log_metada: {:?}", state.id, state.latest_log, prev_log_metadata);
                return Ok((state.current_term, false));
            }

            // prune all logs after match_index
            state.logs.truncate(match_index.unwrap() + 1);
        }

        // append new entries
        state.logs.append(&mut logs);
        // NOTE: assuming that the logs[] arrray isn't ever empty
        state.latest_log = state.logs.last().unwrap().metadata;

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
        // println!(
        //     "append_entries successful, for peer {:?}, metadata: {:?}",
        //     state.id, state.latest_log
        // );

        return Ok((state.current_term, true));
    }
}

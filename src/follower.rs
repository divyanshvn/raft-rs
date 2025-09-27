use crate::{
    node::NodeId,
    types::{AppendEntry, VoteRequest},
};

trait Follower {
    fn vote(node_id: NodeId, vote_request: VoteRequest); // vote!

    fn replicate_log(append_entry: AppendEntry);
    // handle appendEntry from leader.
    // If this is for catching up, accordingly replace/add logs
    // in both cases , send an ack if successful after valiate_log
    // This also gets the to-be-committed logs and then commits them

    fn validate_log(append_entry: AppendEntry);
    // validate w.r.t. prev log (term + index)

    fn commit_log(log_index: u32);
    // commits and applies
}

use crate::node::NodeId;

trait Candidate {
    fn become_candidate();
    // happens when election timeout happens. increase #term and requestVote

    fn request_vote();
    // sends vote request to every node with its bio-data (current term, log index, prev(log term +
    // log index))

    fn track_vote(node_id: NodeId, in_favour: bool);
    // if # votes > 50% , then declare leadership

    fn declare_leader();
    // candidate -> leader... call declare_self
}

use crate::node::NodeId;

trait Leader {
    fn declare_self(); // exhibition of power on being elected, and time to time

    fn append_entries(log_entry: String);
    // adds to itself and spreads the entry, listening
    // for log is done separately

    fn append_entry(node_id: NodeId, log_index: u32);
    // used to append individual entry to node
    // that is catching up

    fn receive_ack(node_id: NodeId, log_index: u32);
    // receives ack for a particular log , from a node. Also checks if the #acks > 50%

    fn commit_log(commit_index: u32);
    // supply index of the log in
    // remaining_commits. Commits log, applies it and adds the co. And then ack is send to
    // the client
}

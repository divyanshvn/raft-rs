pub mod client_rpc;
pub mod node;
pub mod raft_rpc;
pub mod types1;
pub(crate) mod utils;

fn main() {
    println!("Hello, world!");
}

pub mod tests {

    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use anyhow::Result;
    use tarpc::{
        client, context,
        server::{self, BaseChannel, Channel},
        transport,
    };
    use tokio::sync::mpsc;

    use crate::{
        client_rpc::{ClientRpc, ClientRpcClient},
        node::Node,
        raft_rpc::{RaftRpc, RaftRpcClient},
        types1::{Log, LogMetadata, NodeId, NodeType},
    };

    use futures::StreamExt;

    const MIN_TIMEOUT: u64 = 1000;

    #[tokio::test]
    async fn test_append_entries() {
        // set of five nodes
        let mut raft_clients = Vec::new();
        let mut raft_nodes = Vec::new();

        let mut node_rx = VecDeque::new();
        let mut client_rx = VecDeque::new();

        for i in 0..5 {
            let (tx1, rx1) = transport::channel::unbounded();
            let node_raft = RaftRpcClient::new(client::Config::default(), tx1).spawn();
            node_rx.push_back(rx1);
            raft_nodes.push(node_raft);

            let (tx2, rx2) = transport::channel::unbounded();
            let node_client = ClientRpcClient::new(client::Config::default(), tx2).spawn();
            client_rx.push_back(rx2);
            raft_clients.push(node_client);
        }

        let mut node_objs = Vec::new();
        for i in 0..5 {
            // 06/14/2026 04:50 PM IST - TODO: shouldn't nodes be passed as Arc, to save memory? since they aren't gonna be edited.
            let raft_node = Node::new(i, MIN_TIMEOUT, &raft_nodes);
            node_objs.push(raft_node.clone());

            let server_node = server::BaseChannel::with_defaults(node_rx.pop_front().unwrap());
            let raft_node_c = raft_node.clone();
            tokio::spawn(server_node.execute(RaftRpc::serve(raft_node_c)).for_each(
                |future| async move {
                    tokio::spawn(future);
                },
            ));

            let raft_node_c = raft_node.clone();
            let server_client = server::BaseChannel::with_defaults(client_rx.pop_front().unwrap());
            tokio::spawn(
                server_client
                    .execute(ClientRpc::serve(raft_node_c))
                    .for_each(|future| async move {
                        tokio::spawn(future);
                    }),
            );
        }
        let leader_i = 2;
        for i in 0..5 {
            // 06/14/2026 04:50 PM IST - TODO: shouldn't nodes be passed as Arc, to save memory? since they aren't gonna be edited.
            let mut node_type = NodeType::Follower;
            if i == leader_i {
                node_type = NodeType::Leader;
            }
            let node_obj = node_objs[i].clone();

            tokio::spawn(async move {
                node_obj.clone().start(Some(node_type)).await.unwrap();
            });
        }

        // println!("hello");
        raft_clients[leader_i]
            .reply_log(context::current(), "hello".to_string())
            .await
            .unwrap()
            .unwrap();
        // println!("world");
    }
}

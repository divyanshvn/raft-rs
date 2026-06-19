use futures::StreamExt;
use raft_rs::client_rpc::{ClientRpc, ClientRpcClient};
use raft_rs::types1::NodeType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tarpc::{client, server, server::Channel};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::network::{Network, ProxyNode};
use raft_rs::node::Node;
use raft_rs::raft_rpc::{RaftRpc, RaftRpcClient};

pub struct Config {
    pub n: usize,
    pub network: Arc<Network>,
    pub nodes: Arc<HashMap<usize, Mutex<Node>>>,
    pub clients: Arc<HashMap<usize, ClientRpcClient>>,
    pub tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Config {
    pub async fn new(n: usize) -> Self {
        let network = Arc::new(Network::new());
        let mut cfg = Self {
            n,
            network: network.clone(),
            nodes: Arc::new(HashMap::new()),
            clients: Arc::new(HashMap::new()),
            tasks: Arc::new(Mutex::new(Vec::new())),
        };

        cfg.start_all(Some(1)).await;
        cfg
    }

    pub async fn start_all(&mut self, leader_id: Option<usize>) {
        let mut node_real_rxs = HashMap::new();
        let mut real_clients = HashMap::new();
        let mut rpc_clients = HashMap::new();
        let mut nodes = HashMap::new();

        for i in 0..self.n {
            let (tx, rx) = tarpc::transport::channel::unbounded();
            let client = RaftRpcClient::new(client::Config::default(), tx).spawn();
            node_real_rxs.insert(i, rx);
            real_clients.insert(i, client);
        }

        let mut proxy_clients = Vec::new();

        for i in 0..self.n {
            let (tx, rx) = tarpc::transport::channel::unbounded();
            let client = RaftRpcClient::new(client::Config::default(), tx).spawn();
            proxy_clients.push(client);

            let server_proxy = server::BaseChannel::with_defaults(rx);
            let proxy_node = ProxyNode {
                node_id: i,
                target: real_clients.get(&i).unwrap().clone(),
                network: self.network.clone(),
            };
            let h = tokio::spawn(server_proxy.execute(proxy_node.serve()).for_each(
                |future| async move {
                    tokio::spawn(future);
                },
            ));

            self.tasks.lock().unwrap().push(h);
        }

        for i in 0..self.n {
            let node = Node::new(i, 150, &proxy_clients);
            nodes.insert(i, Mutex::new(node.clone()));

            // Serve the real node
            let rx = node_real_rxs.remove(&i).unwrap();
            let server_node = server::BaseChannel::with_defaults(rx);
            let n_cl = node.clone();
            let h = tokio::spawn(server_node.execute(RaftRpc::serve(n_cl)).for_each(
                |future| async move {
                    tokio::spawn(future);
                },
            ));
            self.tasks.lock().unwrap().push(h);

            // Start the node's main loop

            let n_cl2 = node.clone();
            let h = tokio::spawn(async move {
                if leader_id.is_none() || leader_id.unwrap() == i {
                    n_cl2.start(Some(NodeType::Leader)).await.unwrap();
                } else {
                    n_cl2.start(None).await.unwrap();
                }
            });
            self.tasks.lock().unwrap().push(h);

            let (tx, rx) = tarpc::transport::channel::unbounded();
            rpc_clients.insert(
                i,
                ClientRpcClient::new(client::Config::default(), tx).spawn(),
            );
            let server_rpc = server::BaseChannel::with_defaults(rx);
            let node_cl2 = node.clone();
            tokio::spawn(server_rpc.execute(ClientRpc::serve(node_cl2)).for_each(
                |future| async move {
                    tokio::spawn(future);
                },
            ));
        }

        self.clients = Arc::new(rpc_clients);
        self.nodes = Arc::new(nodes);
    }

    pub async fn check_one_leader(&self, current_term: Option<u32>) -> (usize, u32) {
        sleep(Duration::from_millis(5000)).await;
        for _iters in 0..10 {
            sleep(Duration::from_millis(500)).await;

            let mut leaders = Vec::new();

            let nodes_map = self.nodes.clone();
            for i in 0..self.n {
                if self.network.is_connected(i, i) {
                    if let Some(node) = nodes_map.get(&i) {
                        let (term, is_leader) = node.lock().unwrap().get_state();
                        if is_leader && current_term.is_some() && term > current_term.unwrap() {
                            leaders.push((i, term));
                        }
                    }
                }
            }

            if leaders.len() == 1 {
                return leaders[0];
            } else if leaders.len() > 1 {
                panic!("Expected 1 leader, got multiple: {:?}", leaders);
            }
        }
        panic!("Expected 1 leader, got 0");
    }

    pub fn disconnect(&self, i: usize) {
        for j in 0..self.n {
            self.network.set_partition(i, j, false);
        }
    }

    pub fn connect(&self, i: usize) {
        for j in 0..self.n {
            self.network.set_partition(i, j, true);
        }
    }
}

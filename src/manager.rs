use crate::{
    cluster::Cluster,
    node::{Node, NodeEvent},
};
use futures::Future;
use kube::Client;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct NodeManager {
    cluster: Cluster,
}

#[derive(Debug)]
pub enum NodeManagerError {
    CreateClientError,
}

impl NodeManager {
    pub async fn try_default() -> anyhow::Result<Self, NodeManagerError> {
        let client = Client::try_default()
            .await
            .map_err(|_| NodeManagerError::CreateClientError)?;

        let cluster = Cluster::new(client);

        Ok(Self { cluster })
    }

    pub fn new_node(&mut self, image: &str, replicas: i32) -> usize {
        let client = self.cluster.get_client();
        let node_id = self.cluster.get_next_node_id();
        let node = Arc::new(Mutex::new(Node::new(
            node_id,
            image,
            self.cluster.name(),
            replicas,
            client,
        )));
        self.cluster.add_node(node_id, node);
        node_id
    }

    pub async fn watch_node<F, R>(&self, node_id: usize, f: F)
    where
        F: Send + Clone + 'static,
        F: FnOnce(NodeEvent) -> R,
        R: Future<Output = ()> + Send,
    {
        if let Some(node) = self.cluster.get_node(node_id) {
            let mut node = node.lock().await;
            node.watch(f).await;
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        if self.cluster.has_existing_version().await? {
            self.cluster.clean_old_versions().await?;
        }

        self.cluster.create_node_definitions().await?;
        self.cluster.boot_nodes().await?;

        'keep_alive: loop {
            if !self.cluster.has_running_node().await {
                break 'keep_alive;
            }
        }

        Ok(())
    }
}

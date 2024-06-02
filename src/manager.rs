use futures::{Future, StreamExt};
use garde::Validate;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{Api, DeleteParams, PostParams, ResourceExt},
    runtime::{controller::Action, Controller},
    Client, CustomResource, CustomResourceExt,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
    time::sleep,
};
use tracing::*;

#[derive(thiserror::Error, Debug)]
pub enum NodeError {}

pub type ReconcileResult<T, E = NodeError> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum NodeEvent {
    Reconcile,
    Created,
    Destroyed,
}

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(
    group = "node.manager",
    version = "v1",
    kind = "ComputeServer",
    namespaced
)]
#[kube(status = "ComputeServerStatus")]
#[kube(scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#)]
#[kube(printcolumn = r#"{"name":"Team", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
pub struct ComputeServerSpec {}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct ComputeServerStatus {
    replicas: u32,
}

struct ComputeServerCtx {
    id: usize,
    client: Client,
    s: mpsc::Sender<NodeEvent>,
}

impl ComputeServerCtx {
    fn new(id: usize, client: Client, s: mpsc::Sender<NodeEvent>) -> Self {
        Self { id, client, s }
    }
}

pub struct Node {
    id: usize,
    client: Client,
    controller: Option<JoinHandle<()>>,
    executor: Option<JoinHandle<()>>,
}

impl Node {
    fn new(id: usize, client: Client) -> Self {
        Self {
            id,
            client,
            controller: None,
            executor: None,
        }
    }

    pub async fn boot(&self) {}

    pub async fn watch<F, R>(&mut self, on_event: F)
    where
        F: Send + Clone + 'static,
        F: FnOnce(NodeEvent) -> R,
        R: Future<Output = ()> + Send,
    {
        let (s, mut r) = mpsc::channel(1);
        let ctx = ComputeServerCtx::new(self.id, self.client.clone(), s);

        let client = self.client.clone();
        let controller = tokio::spawn(async move {
            let pods = Api::<ComputeServer>::all(client);
            Controller::new(pods.clone(), Default::default())
                .run(Self::reconcile, Self::error_policy, Arc::new(ctx))
                .for_each(|_| futures::future::ready(()))
                .await;
        });

        let executor = tokio::spawn(async move {
            while let Some(e) = r.recv().await {
                let on_event = on_event.clone();
                on_event(e).await;
            }
        });

        self.controller = Some(controller);
        self.executor = Some(executor);
    }

    async fn reconcile(
        obj: Arc<ComputeServer>,
        ctx: Arc<ComputeServerCtx>,
    ) -> ReconcileResult<Action> {
        let _client = ctx.client.clone();
        println!(
            "[{}::node:{}] reconcile request: {}",
            obj.namespace().unwrap(),
            ctx.id,
            obj.name_any()
        );
        let sender = ctx.s.clone();
        sender.send(NodeEvent::Reconcile).await.unwrap();
        Ok(Action::requeue(Duration::from_secs(3600)))
    }

    fn error_policy(
        _object: Arc<ComputeServer>,
        _err: &NodeError,
        _ctx: Arc<ComputeServerCtx>,
    ) -> Action {
        Action::requeue(Duration::from_secs(5))
    }

    fn _destroy(self) {
        warn!("Deleting node id: {}", self.id);
        drop(self);
    }
}

pub struct NodeManager {
    client: Client,
    nodes: HashMap<usize, Arc<Mutex<Node>>>,
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

        Ok(Self {
            client,
            nodes: HashMap::new(),
        })
    }

    pub fn new_node(&mut self) -> Result<usize, ()> {
        let node_id = self.nodes.len();
        let node = Arc::new(Mutex::new(Node::new(node_id, self.client.clone())));
        self.nodes.insert(node_id, node.clone());
        Ok(node_id)
    }

    pub async fn watch_node<F, R>(&self, node_id: usize, f: F)
    where
        F: Send + Clone + 'static,
        F: FnOnce(NodeEvent) -> R,
        R: Future<Output = ()> + Send,
    {
        if let Some(node) = self.nodes.get(&node_id) {
            let mut node = node.lock().await;
            node.watch(f).await;
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        Self::clean_old_versions(self.client.clone()).await?;
        Self::create_node_definitions(self.client.clone()).await?;

        for (_, node) in self.nodes.iter() {
            let node = node.lock().await;
            node.boot().await;
        }

        Ok(())
    }

    async fn clean_old_versions(client: Client) -> anyhow::Result<()> {
        let crds: Api<CustomResourceDefinition> = Api::all(client);
        let dp = DeleteParams::default();

        let _ = crds
            .delete("compute_servers.node.manager", &dp)
            .await
            .map(|res| {
                res.map_left(|o| {
                    info!(
                        "Deleting {}: ({:?})",
                        o.name_any(),
                        o.status.unwrap().conditions.unwrap().last()
                    );
                })
                .map_right(|s| {
                    info!("Deleted compute_servers.node.manager: ({:?})", s);
                })
            });
        sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    async fn create_node_definitions(client: Client) -> anyhow::Result<()> {
        let crds: Api<CustomResourceDefinition> = Api::all(client);

        let nodecrd = ComputeServer::crd();
        info!(
            "Creating Node CRD: {}",
            serde_json::to_string_pretty(&nodecrd)?
        );
        let pp = PostParams::default();
        match crds.create(&pp, &nodecrd).await {
            Ok(o) => {
                info!("Created {} ({:?})", o.name_any(), o.status.unwrap());
                debug!("Created CRD: {:?}", o.spec);
            }
            Err(kube::Error::Api(ae)) => assert_eq!(ae.code, 409),
            Err(e) => return Err(e.into()),
        }

        sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

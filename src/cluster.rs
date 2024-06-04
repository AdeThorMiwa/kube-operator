use std::{
    collections::HashMap,
    fmt::Display,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use futures::StreamExt;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
    api::{ListParams, PostParams, ResourceExt},
    runtime::{controller::Action, Controller},
    Api, Client, CustomResourceExt,
};
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};
use tracing::{debug, info};

use crate::{
    node::{ComputeServer, Node, NodeStatus},
    utils::cluster::cluster_utils,
};

pub struct Cluster {
    client: Client,
    nodes: HashMap<usize, Arc<Mutex<Node>>>,
    pub ready: Arc<Mutex<AtomicBool>>,
    cntrl: Option<JoinHandle<()>>,
}

struct ClusterCtx {
    client: Client,
    ready: Arc<Mutex<AtomicBool>>,
    name: String,
}

#[derive(Debug, thiserror::Error)]
enum ClusterReconcileError {
    FinalizeError,
}

impl Display for ClusterReconcileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Cluster {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            nodes: HashMap::new(),
            ready: Arc::new(Mutex::new(AtomicBool::new(false))),
            cntrl: None,
        }
    }

    pub fn name(&self) -> String {
        "computeservers.node.manager".to_owned()
    }

    pub fn get_client(&self) -> Client {
        self.client.clone()
    }

    pub fn get_next_node_id(&self) -> usize {
        self.nodes.len()
    }

    pub fn add_node(&mut self, node_id: usize, node: Arc<Mutex<Node>>) {
        self.nodes.insert(node_id, node);
    }

    pub fn get_node(&self, node_id: usize) -> Option<Arc<Mutex<Node>>> {
        self.nodes.get(&node_id).map(|n| n.clone())
    }

    pub async fn boot_nodes(&self) -> anyhow::Result<()> {
        for (_, node) in self.nodes.iter() {
            let mut node = node.lock().await;
            node.boot().await?;
        }

        Ok(())
    }

    pub async fn has_running_node(&self) -> bool {
        let mut has_running_node = false;

        for (_, node) in self.nodes.iter() {
            let node = node.lock().await;
            if node.status == NodeStatus::Running {
                has_running_node = true;
            }
        }

        has_running_node
    }

    pub fn setup_controller(&mut self) {
        let client = self.client.clone();
        let ctx = ClusterCtx {
            client: client.clone(),
            ready: self.ready.clone(),
            name: self.name(),
        };
        let controller = tokio::spawn(async move {
            let crds = Api::<CustomResourceDefinition>::all(client);

            Controller::new(crds.clone(), Default::default())
                .run(Self::reconcile, Self::error_policy, Arc::new(ctx))
                .for_each(|_| futures::future::ready(()))
                .await;
        });

        self.cntrl = Some(controller)
    }

    async fn reconcile(
        crd: Arc<CustomResourceDefinition>,
        ctx: Arc<ClusterCtx>,
    ) -> Result<Action, ClusterReconcileError> {
        match cluster_utils::get_action(&crd) {
            cluster_utils::ClusterReconcileAction::Create => {
                info!("created crd: {}", crd.name_any());
                ctx.ready
                    .lock()
                    .await
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(Action::await_change())
            }
            cluster_utils::ClusterReconcileAction::Delete => {
                cluster_utils::finalize(&ctx.name, ctx.client.clone())
                    .await
                    .map_err(|_| ClusterReconcileError::FinalizeError)?;
                ctx.ready
                    .lock()
                    .await
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(Action::await_change())
            }
            cluster_utils::ClusterReconcileAction::NoOp => {
                Ok(Action::requeue(Duration::from_secs(5)))
            }
        }
    }

    fn error_policy(
        crd: Arc<CustomResourceDefinition>,
        err: &ClusterReconcileError,
        _ctx: Arc<ClusterCtx>,
    ) -> Action {
        eprintln!("Reconciliation error:\n{:?}.\n{:?}", err, crd.name_any());
        Action::requeue(Duration::from_secs(5))
    }

    pub async fn clean_old_versions(&self) -> anyhow::Result<()> {
        Ok(cluster_utils::clean(&self.name(), self.client.clone()).await?)
    }

    pub async fn has_existing_version(&self) -> anyhow::Result<bool> {
        let crds: Api<CustomResourceDefinition> = Api::all(self.client.clone());
        let res = crds.list(&ListParams::default()).await.unwrap();
        Ok(res.items.len() > 0)
    }

    pub async fn create_node_definitions(&self) -> anyhow::Result<()> {
        let crds: Api<CustomResourceDefinition> = Api::all(self.client.clone());

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

pub mod cluster_utils {
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use kube::{
        api::{DeleteParams, Patch, PatchParams, ResourceExt},
        Api, Client, Resource,
    };
    use serde_json::json;
    use std::time::Duration;
    use tokio::time::sleep;
    use tracing::info;

    pub enum ClusterReconcileAction {
        Create,
        Delete,
        NoOp,
    }

    pub async fn clean(name: &str, client: Client) -> anyhow::Result<()> {
        let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
        let dp = DeleteParams::default();

        crds.delete(name, &dp).await.map(|res| {
            res.map_left(|o| {
                info!(
                    "Deleting {}: ({:?})",
                    o.name_any(),
                    o.status.unwrap().conditions.unwrap().last()
                );
            })
            .map_right(|s| {
                info!("Deleted {}: ({:?})", name, s);
            })
        })?;

        sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    pub async fn finalize(name: &str, client: Client) -> anyhow::Result<()> {
        let crds: Api<CustomResourceDefinition> = Api::all(client.clone());
        let patch_data = json!({
            "metadata": {
                "finalizers": []
            }
        });

        crds.patch(name, &PatchParams::default(), &Patch::Merge(&patch_data))
            .await
            .map(|_| Ok(()))?
    }

    pub fn get_action(crd: &CustomResourceDefinition) -> ClusterReconcileAction {
        if crd.meta().deletion_timestamp.is_some() {
            ClusterReconcileAction::Delete
        } else if crd
            .meta()
            .finalizers
            .as_ref()
            .map_or(true, |finalizers| finalizers.is_empty())
        {
            ClusterReconcileAction::Create
        } else {
            ClusterReconcileAction::NoOp
        }
    }
}

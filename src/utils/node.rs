pub mod node_utils {
    use std::collections::BTreeMap;

    use k8s_openapi::{
        api::{
            apps::v1::{Deployment, DeploymentSpec},
            core::v1::{
                Container, ContainerPort, PodSpec, PodTemplateSpec, Service, ServicePort,
                ServiceSpec,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    };
    use kube::{api::ObjectMeta, ResourceExt};

    use crate::node::ComputeServer;

    pub fn get_deployment_definition(
        name: &str,
        namespace: &str,
        image: &str,
        replicas: i32,
        port: i32,
        labels: BTreeMap<String, String>,
    ) -> Deployment {
        Deployment {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(namespace.to_owned()),
                labels: Some(labels.clone()),
                ..ObjectMeta::default()
            },
            spec: Some(DeploymentSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_expressions: None,
                    match_labels: Some(labels.clone()),
                },
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        host_network: Some(true),
                        containers: vec![Container {
                            name: name.to_owned(),
                            image: Some(image.to_owned()),
                            image_pull_policy: Some("Never".to_owned()),
                            ports: Some(vec![ContainerPort {
                                container_port: port,
                                host_port: Some(port),
                                ..ContainerPort::default()
                            }]),
                            ..Container::default()
                        }],
                        ..PodSpec::default()
                    }),
                    metadata: Some(ObjectMeta {
                        labels: Some(labels),
                        ..ObjectMeta::default()
                    }),
                },
                ..DeploymentSpec::default()
            }),
            ..Deployment::default()
        }
    }

    pub fn get_node_port_service_definition(
        name: &str,
        namespace: &str,
        port: i32,
        labels: BTreeMap<String, String>,
    ) -> Service {
        Service {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(namespace.to_owned()),
                labels: Some(labels.clone()),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("NodePort".to_owned()),
                selector: Some(labels),
                ports: Some(vec![ServicePort {
                    port,
                    target_port: Some(IntOrString::Int(port)),
                    ..ServicePort::default()
                }]),
                ..ServiceSpec::default()
            }),
            ..Service::default()
        }
    }

    pub fn get_name_and_namespace(server: &ComputeServer) -> Result<(String, String), kube::Error> {
        let name = server.name_any();
        let namespace = match server.namespace() {
            None => {
                let e = "Expected Echo resource to be namespaced. Can't deploy to an unknown namespace.";
                return Err(kube::Error::Discovery(
                    kube::error::DiscoveryError::MissingResource(e.to_owned()),
                ));
            }
            Some(namespace) => namespace,
        };

        Ok((name, namespace))
    }
}

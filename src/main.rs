use anyhow::Result;
use operator::manager::NodeManager;

#[tokio::main]
async fn main() -> Result<()> {
    let mut node_manager = NodeManager::try_default().await.unwrap();

    let node1 = node_manager.new_node("kubes-container:minikube", 1);
    let node2 = node_manager.new_node("kubes-container:minikube", 10);

    let watch_handler = |event| async move { println!("new event {:?}", event) };
    node_manager.watch_node(node1, watch_handler).await;
    node_manager.watch_node(node2, watch_handler).await;

    node_manager.run().await?;

    Ok(())
}

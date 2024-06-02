use anyhow::Result;
use operator::manager::NodeManager;

#[tokio::main]
async fn main() -> Result<()> {
    let mut node_manager = NodeManager::try_default().await.unwrap();

    let node1 = node_manager.new_node().unwrap();
    let node2 = node_manager.new_node().unwrap();

    let watch_handler = |_event| async move {};
    node_manager.watch_node(node1, watch_handler).await;
    node_manager.watch_node(node2, watch_handler).await;

    node_manager.run().await?;

    Ok(())
}

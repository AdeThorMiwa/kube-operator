use futures::{Future, StreamExt};
use reqwest_eventsource::{Event, EventSource};
use serde::{Deserialize, Serialize};

pub struct MessageChannel {
    port: i32,
}

pub enum SubscribeMessage {
    MapCompleted,
    ReduceCompleted,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PublishMessage {
    Map,
    Reduce,
}

impl MessageChannel {
    pub fn new(port: i32) -> Self {
        Self { port }
    }

    pub async fn publish(&self, msg: PublishMessage) -> Result<(), ()> {
        let url = match msg {
            PublishMessage::Map => format!("{}/map", self.get_full_url()),
            PublishMessage::Reduce => format!("{}/reduce", self.get_full_url()),
        };

        reqwest::Client::new()
            .post(url)
            .json(&msg)
            .send()
            .await
            .map_err(|_| ())?
            .json()
            .await
            .map_err(|_| ())?;
        Ok(())
    }

    pub fn subscribe<H, R>(&self, handler: H)
    where
        H: Send + Clone + 'static,
        H: FnOnce(SubscribeMessage) -> R,
        R: Future<Output = ()> + Send,
    {
        let mut es = EventSource::get(format!("{}/events", self.get_full_url()));
        tokio::spawn(async move {
            loop {
                if let Some(event) = es.next().await {
                    match event {
                        Ok(Event::Open) => println!("Connection Open!"),
                        Ok(Event::Message(message)) => {
                            println!("Message: {:#?}", message);
                            handler.clone()(SubscribeMessage::MapCompleted);
                        }
                        Err(err) => {
                            println!("Error: {}", err);
                            // es.close();
                        }
                    }
                }
            }
        });
    }

    fn get_full_url(&self) -> String {
        format!("http:127.0.0.1:{}", self.port)
    }
}

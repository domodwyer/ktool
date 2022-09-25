use anyhow::Context;
use rdkafka::{
    config::FromClientConfig,
    message::OwnedHeaders,
    producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer},
    util::Timeout,
};

use crate::{cli::common::KafkaOpts, message::Message};

use super::Sink;

pub struct Kafka {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
    partition: Option<i32>,
}

impl Kafka {
    pub fn new(
        brokers: Vec<String>,
        topic: String,
        partition: Option<i32>,
        kafka_opts: &KafkaOpts,
    ) -> anyhow::Result<Self> {
        let config = kafka_opts.new_kafka_config(brokers);

        let producer = ThreadedProducer::from_config(&config)
            .context("failed to initialise kafka producer")?;

        Ok(Self {
            producer,
            topic,
            partition,
        })
    }
}

impl Sink for Kafka {
    fn write(&mut self, msg: &crate::message::Message) -> anyhow::Result<()> {
        let mut msg = BaseRecord::from(msg);
        msg.topic = self.topic.as_ref();
        msg.partition = self.partition;

        self.producer
            .send(msg)
            .map_err(|(e, _)| e)
            .context("failed to enqueue message to kafka")
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.producer.flush(Timeout::Never);
        Ok(())
    }
}

impl<'a> From<&'a Message> for BaseRecord<'a, [u8], [u8]> {
    fn from(v: &'a Message) -> Self {
        let headers = v.headers().map(|h| {
            h.iter()
                .fold(OwnedHeaders::new_with_capacity(h.len()), |acc, (k, v)| {
                    acc.add(k, v)
                })
        });

        BaseRecord {
            topic: v.topic(),
            partition: None,
            payload: v.payload(),
            key: v.key(),
            timestamp: None,
            headers,
            delivery_opaque: (),
        }
    }
}

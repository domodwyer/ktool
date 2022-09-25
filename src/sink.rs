pub mod file;
pub mod kafka;

use crate::{
    cli::common::{KafkaOpts, Target},
    message::Message,
};

use self::{file::FileSink, kafka::Kafka};

// TODO: doc buffering

pub trait Sink: Send {
    fn write(&mut self, msg: &Message) -> anyhow::Result<()>;
    fn flush(&mut self) -> anyhow::Result<()>;
}

pub(crate) fn init(target: Target, kafka_opts: &KafkaOpts) -> anyhow::Result<Box<dyn Sink>> {
    match target {
        Target::Kafka {
            brokers,
            topic,
            partition,
        } => {
            eprintln!("[*] connecting to kafka brokers: {}", brokers.join(", "));
            Ok(Box::new(Kafka::new(brokers, topic, partition, kafka_opts)?))
        }
        Target::Path(v) => {
            eprintln!("[*] opening file: {}", v.display());
            Ok(Box::new(FileSink::new(&v)?))
        }
    }
}

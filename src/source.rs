pub mod file;
pub mod kafka;

use crate::{
    cli::common::{KafkaOpts, Target},
    message::Message,
};

type BoxedSource = Box<dyn Iterator<Item = Result<Message, Box<dyn std::error::Error>>>>;

pub(crate) fn init(
    target: Target,
    kafka_opts: &KafkaOpts,
    start_offset: Option<i64>,
) -> anyhow::Result<BoxedSource> {
    match target {
        Target::Kafka {
            brokers,
            topic,
            partition,
        } => {
            eprintln!("[*] connecting to kafka brokers: {}", brokers.join(", "));
            Ok(Box::new(kafka::new(
                brokers,
                topic,
                partition,
                kafka_opts,
                start_offset,
            )?))
        }
        Target::Path(v) => {
            eprintln!("[*] opening dump file: {}", v.display());
            Ok(Box::new(file::new(v)?))
        }
    }
}

struct ApproxBoundedIter<I>(I, usize);

impl<I> Iterator for ApproxBoundedIter<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.1))
    }
}

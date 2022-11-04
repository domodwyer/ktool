use anyhow::Context;
use rdkafka::{
    config::FromClientConfig,
    consumer::{BaseConsumer, Consumer},
    message::{BorrowedMessage, Headers},
    util::Timeout,
    Message as _, Offset, TopicPartitionList,
};

use crate::{
    cli::common::KafkaOpts,
    message::{Message, Timestamp},
};

pub fn new(
    brokers: Vec<String>,
    topic: String,
    partition: Option<i32>,
    kafka_opts: &KafkaOpts,
    start_offset: Option<i64>,
) -> anyhow::Result<impl Iterator<Item = Result<Message, Box<dyn std::error::Error>>>> {
    let config = kafka_opts.new_kafka_config(brokers);

    let consumer =
        BaseConsumer::from_config(&config).context("failed to initialise kafka consumer")?;

    // Get the target partition by reading the optional user-provided partition
    // number, or default to 0 if there's only one partition.
    let partition = match partition {
        Some(v) => v,
        None => {
            // First ensure there is only one partition.
            let n = consumer.committed(kafka_opts.timeout)?.count();
            if n > 1 {
                return Err(anyhow::anyhow!("topic has more than one partition - please specify a partition to consume from"));
            }

            0
        }
    };

    // TODO: if end == None & !-f set end to the current partition offset
    // If end != None, use that and skip lookup
    // Make iterator sized, and use to measure progress

    // Grab the max offset to read.
    let (mut offset_start, offset_end) =
        consumer.fetch_watermarks(&topic, partition, kafka_opts.timeout)?;

    // If a start offset was provided, seek the consumer to it to skip the prior
    // messages.
    let offset = match start_offset {
        Some(v) if v < 0 => {
            offset_start = offset_end + v;
            Offset::OffsetTail(-v)
        }
        Some(v) => {
            offset_start = v;
            Offset::Offset(v)
        }
        None => Offset::Beginning,
    };

    let mut targets = TopicPartitionList::new();
    targets
        .add_partition_offset(&topic, partition, offset)
        .context("failed to configure partition config")?;
    consumer
        .assign(&targets)
        .context("failed to assign target partition to consumer")?;

    let timeout = kafka_opts.timeout;
    let iter = std::iter::from_fn(move || {
        consumer
            .poll(Timeout::After(timeout))
            .map(|v| v.map(Message::from).map_err(Box::from))
    });

    let iter = super::ApproxBoundedIter(iter, (offset_end - offset_start) as usize);

    Ok(Box::new(iter))
}

impl<'a> From<BorrowedMessage<'a>> for Message {
    fn from(v: BorrowedMessage<'a>) -> Self {
        // Read the headers into a vec.
        let parsed_headers = v.headers().map(|headers| {
            (0..headers.count())
                .map(|i| {
                    let (key, value) = headers.get(i).expect("no header");
                    (key.to_owned(), value.to_owned())
                })
                .collect()
        });

        Message::new(
            v.topic(),
            v.partition(),
            v.offset(),
            Timestamp::try_from(v.timestamp()).ok(),
            parsed_headers,
            v.key().map(ToOwned::to_owned),
            v.payload().map(ToOwned::to_owned),
        )
    }
}

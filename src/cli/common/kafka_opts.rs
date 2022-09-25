use std::{num::ParseIntError, str::FromStr, time::Duration};

use clap::Args;
use rdkafka::ClientConfig;

// NOTE: this is shared with all kafka commands, including metadata.
#[derive(Debug, Args)]
pub struct KafkaOpts {
    /// The number of seconds to wait for an ACK before returning a timeout
    /// error.
    #[clap(long, default_value = "10", parse(try_from_str = parse_seconds))]
    pub timeout: Duration,

    /// The Kafka producer group ID to use.
    #[clap(long, default_value = "bananas")]
    pub group: String,

    /// Arbitrary Kafka parameters for customised configuration.
    ///
    /// Any Kafka client configuration parameter accepted by librdkafka can be
    /// provided.
    #[clap(name = "opt", short = 'X', long)]
    pub additional_args: Vec<KeyValueConfig>,
}

impl KafkaOpts {
    pub(crate) fn new_kafka_config(&self, brokers: Vec<String>) -> ClientConfig {
        let mut config = ClientConfig::new();
        for kv in &self.additional_args {
            config.set(&kv.key, &kv.value);
        }
        config.set("enable.auto.commit", "false");
        config.set("enable.partition.eof", "false");
        config.set("auto.offset.reset", "earliest");
        config.set("bootstrap.servers", brokers.join(","));

        config.set("group.id", &self.group);

        config
    }
}

fn parse_seconds(input: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(input.parse::<u64>()?))
}

#[derive(Debug)]
pub struct KeyValueConfig {
    pub key: String,
    pub value: String,
}

impl FromStr for KeyValueConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('=').collect::<Vec<_>>();

        Ok(match parts.as_slice() {
            [a, b] => KeyValueConfig {
                key: a.to_string(),
                value: b.to_string(),
            },
            _ => return Err(anyhow::anyhow!("asdf")),
        })
    }
}

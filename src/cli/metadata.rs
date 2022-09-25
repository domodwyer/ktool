use anyhow::Context;
use clap::Args;
use rdkafka::{
    config::FromClientConfig,
    consumer::{BaseConsumer, Consumer},
};

use super::common::KafkaOpts;

#[derive(Debug, Args)]
pub struct CliArgs {
    /// A comma-delimited set of brokers in "host:port" format to query for
    /// metadata.
    brokers: Vec<String>,

    #[clap(flatten)]
    kafka_opts: KafkaOpts,
}

pub fn run(v: CliArgs) -> Result<(), anyhow::Error> {
    let config = v.kafka_opts.new_kafka_config(v.brokers);

    let consumer =
        BaseConsumer::from_config(&config).context("failed to initialise kafka consumer")?;

    let meta = consumer
        .fetch_metadata(None, v.kafka_opts.timeout)
        .context("failed to read cluster metadata")?;

    println!(
        "[+] metadata retrieved (src: {}, id: {})",
        meta.orig_broker_name(),
        meta.orig_broker_id()
    );
    println!();

    println!("Brokers:");
    for broker in meta.brokers() {
        println!(
            "\tid {id} -> {host}:{port}",
            id = broker.id(),
            host = broker.host(),
            port = broker.port()
        );
    }
    println!();

    println!("Topics:");
    for topic in meta.topics() {
        println!("\t{}", topic.name());
        if let Some(e) = topic.error() {
            println!("\t\terror: {:?}", e);
        }

        for p in topic.partitions() {
            println!(
                "\t\tpartition {}, leader {}, replicas {:?} (in sync: {:?})",
                p.id(),
                p.leader(),
                p.replicas(),
                p.isr()
            );
            if let Some(e) = p.error() {
                println!("\t\t\terror: {:?}", e);
            }
        }
    }

    Ok(())
}

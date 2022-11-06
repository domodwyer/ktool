use std::io::{stdout, BufWriter, Write};

use anyhow::Context;
use clap::Args;

use crate::{json_output::JsonMessage, source};

use super::common::{OffsetClap, Target};

/// Read messages from a source, outputting them as either debug printed data,
/// or  newline delimited JSON objects (ndjson).
#[derive(Debug, Args)]
pub struct CliArgs {
    /// A message source to read.
    ///
    /// Supports either:
    ///
    ///   - Kafka: "kafka://brokers/topic/partition"
    ///
    ///   - File: /path/to/file.bin
    ///
    /// Where a Kafka source can specify one or more comma-delimited broker
    /// addresses, a topic, and a optional partition number. Example:
    /// "kafka://127.0.0.1:9092,another:9092/my_topic/0".
    ///
    /// Where a file source can be a absolute, or relative file path.
    from: Target,

    /// Output messages as newline delimited JSON objects.
    ///
    /// Fields where Kafka allows binary content are base64 encoded, this
    /// includes the message key, payload and header values.
    #[clap(long)]
    json: bool,

    #[clap(flatten)]
    offset: OffsetClap,

    #[clap(flatten)]
    kafka_args: crate::cli::common::KafkaOpts,
}

pub fn run(args: CliArgs) -> anyhow::Result<()> {
    // Initialise the message source.
    //
    // This can either be a file, or another kafka topic.
    let source = source::init(args.from, &args.kafka_args, args.offset.start_offset())
        .context("failed to initialise copy source")?;

    // Limit messages to the configured offsets
    let source = args.offset.wrap_iter(source);

    let mut w = BufWriter::new(stdout());

    // Read from the source, respecting the configured offset ranges, if any.
    for maybe_msg in source {
        match maybe_msg {
            Ok(v) => {
                // Print this message.
                if args.json {
                    // Wrap this in a JsonMessage to generate the field
                    // modifications specifically for the JSON format.
                    serde_json::to_writer(&mut w, &JsonMessage::from(&v))
                        .expect("serialisation of messages is infallible");
                    writeln!(&mut w).unwrap();
                } else {
                    writeln!(&mut w, "{:?}", v).unwrap();
                }
            }
            Err(e) => eprintln!("[-] read error: {}", e),
        }
    }

    w.flush().expect("failed to flush stdout");

    Ok(())
}

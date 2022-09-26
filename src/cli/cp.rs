use std::time::Duration;

use anyhow::{anyhow, Context};
use clap::Args;
use indicatif::{HumanDuration, ProgressBar, ProgressStyle};

use crate::{sink, source};

use super::common::{OffsetClap, Target};

// TODO(dom): examples

/// Copy data from one message source to another.
#[derive(Debug, Args)]
pub struct CliArgs {
    /// A message source to copy.
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

    /// A message sink specified in the same format as the message source.
    to: Target,

    /// Maximum number of messages to buffer while writing is blocked.
    #[clap(long, default_value = "100")]
    buffer: usize,

    #[clap(flatten)]
    offset: OffsetClap,

    #[clap(flatten)]
    kafka_args: crate::cli::common::KafkaOpts,
}

pub fn run(args: CliArgs) -> anyhow::Result<()> {
    // Reject reading and writing to the same kafka topic.
    if args.to == args.from {
        return Err(anyhow!("read source and write sink cannot be the same"));
    }

    // Initialise the message source.
    //
    // This can either be a file, or another kafka topic.
    let source = source::init(args.from, &args.kafka_args, args.offset.start_offset())
        .context("failed to initialise copy source")?;

    // Limit messages to the configured offsets
    let source = args.offset.wrap_iter(source);

    // Initialise the message sink.
    let mut sink = sink::init(args.to, &args.kafka_args)?;

    // And init a buffer between the source/sink to decouple each of their
    // respective read/write latencies.
    let (tx, rx) = std::sync::mpsc::sync_channel(args.buffer);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("[{elapsed_precise:.cyan/blue}] copied {pos} messages ({per_sec} msg/s)"),
    );

    let (_, upper_bound) = source.size_hint();
    if let Some(u) = upper_bound {
        pb.set_style(ProgressStyle::default_bar());
        pb.set_length(u as _);
    }

    // Spawn a thread to handle the persistence of messages.
    //
    // This decouples the write sink latency from the read side, allowing the
    // read side to continue buffering messages from Kafka (a relatively slow
    // read operation) while a batch of writes are blocked while flushing to
    // disk.
    //
    // The number of messages that can be buffered between the read & write
    // side is configurable via a CLI flag before the writer begins applying
    // back-pressure to the read side.
    let writer_handle = std::thread::spawn({
        let pb = pb.clone();
        move || {
            while let Ok(msg) = rx.recv() {
                // Attempt to write the message to the sink, reporting &
                // retrying any errors that occur.
                'retry: loop {
                    match sink.write(&msg) {
                        Ok(_) => break 'retry,
                        Err(e) => pb.println(format!("[-] write error: {}", e).as_str()),
                    }
                    std::thread::sleep(Duration::from_millis(500));
                }
                pb.inc(1);
            }

            pb.println("[*] flushing writes");

            // Flush the sink, reporting & retrying any errors before
            // terminating the thread.
            loop {
                match sink.flush() {
                    Ok(_) => return,
                    Err(e) => pb.println(format!("[-] write flush error: {}", e).as_str()),
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    });

    // Drive the copy by reading from the source, and pushing it to the buffer
    // channel to the sink thread.
    for maybe_msg in source {
        match maybe_msg {
            Ok(v) => {
                tx.send(v).expect("writer thread has died");
            }
            Err(e) => pb.println(format!("[-] read error: {}", e).as_str()),
        }
    }

    pb.println("[*] read complete");

    // Signal the completion to the writer thread and wait for it to flush and
    // exit gracefully.
    drop(tx);
    writer_handle.join().expect("writer thread died");

    pb.println("[*] write complete");

    // Report the final copy stats.
    let count = pb.position();
    let rate = pb.per_sec();
    let elapsed = HumanDuration(pb.elapsed());
    pb.finish_and_clear();
    println!("[+] complete - copied {count} messages in {elapsed} ({rate} msg/s)");

    Ok(())
}

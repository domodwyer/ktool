use std::time::Duration;

use ktool::{cli::common::KafkaOpts, message::Message, sink::Sink};

mod common;

#[test]
fn test_produce_consume() {
    let addr = maybe_skip_integration!();

    static TOPIC: &str = "topic";

    let kafka_config = KafkaOpts {
        timeout: Duration::from_secs(5),
        group: "bananas".to_string(),
        additional_args: vec![],
    };

    let mut sink = ktool::sink::kafka::Kafka::new(
        vec![addr.clone()],
        TOPIC.to_string(),
        Some(0),
        &kafka_config,
    )
    .expect("failed to initialise kafka sink");

    let mut source =
        ktool::source::kafka::new(vec![addr], TOPIC.to_string(), Some(0), &kafka_config, None)
            .expect("failed to initialise kafka source");

    let msg = Message::new(
        TOPIC,
        0,
        0,
        None,
        None,
        Some("banana-key".into()),
        Some("platanos".into()),
    );

    sink.write(&msg).expect("publishing message failed");
    sink.flush().expect("failed to flush producer");

    let got = source
        .next()
        .expect("no message received")
        .expect("unexpected consume error");

    assert_eq!(got.topic(), msg.topic());
    assert_eq!(got.headers(), msg.headers());
    assert_eq!(got.key(), msg.key());
    assert_eq!(got.payload(), msg.payload());
}

#[test]
fn test_consume_tail() {
    let addr = maybe_skip_integration!();

    static TOPIC: &str = "another-topic";

    let kafka_config = KafkaOpts {
        timeout: Duration::from_secs(5),
        group: "bananas".to_string(),
        additional_args: vec![],
    };

    let tail = Message::new(
        TOPIC,
        0,
        0,
        None,
        None,
        Some("banana-key".into()),
        Some("tail message".into()),
    );

    {
        let mut sink = ktool::sink::kafka::Kafka::new(
            vec![addr.clone()],
            TOPIC.to_string(),
            Some(0),
            &kafka_config,
        )
        .expect("failed to initialise kafka sink");

        let msg = Message::new(
            TOPIC,
            0,
            0,
            None,
            None,
            Some("banana-key".into()),
            Some("platanos".into()),
        );

        sink.write(&msg).expect("publishing message failed");
        sink.write(&msg).expect("publishing message failed");
        sink.write(&msg).expect("publishing message failed");
        sink.write(&msg).expect("publishing message failed");
        sink.write(&tail).expect("publishing message failed");
        sink.flush().expect("failed to flush producer");
    }

    let mut source = ktool::source::kafka::new(
        vec![addr],
        TOPIC.to_string(),
        Some(0),
        &kafka_config,
        Some(-1),
    )
    .expect("failed to initialise kafka source");

    let got = source
        .next()
        .expect("no message received")
        .expect("unexpected consume error");

    assert_eq!(got.topic(), tail.topic());
    assert_eq!(got.headers(), tail.headers());
    assert_eq!(got.key(), tail.key());
    assert_eq!(got.payload(), tail.payload());
}

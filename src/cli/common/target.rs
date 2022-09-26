use std::{num::ParseIntError, path::PathBuf, str::FromStr};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TargetError {
    #[error("invalid partition: {}", .0)]
    ParseInt(#[from] ParseIntError),

    #[error(
        "invalid target format (expected 'path', or 'kafka://brokers/topic/<optional-partition>')"
    )]
    Invalid,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Target {
    Kafka {
        brokers: Vec<String>,
        topic: String,
        partition: Option<i32>,
    },
    Path(PathBuf),
}

impl FromStr for Target {
    type Err = TargetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(addr) = s.strip_prefix("kafka://") {
            // Remove any trailing / to prevent it being interpreted as a
            // separator of values, when no value is specified.
            let addr = addr.trim_end_matches('/');

            // Blow apart the string and compare the number of resulting parts.
            let parts = addr.split('/').collect::<Vec<_>>();

            let target = match *parts.as_slice() {
                // Never accept empty broker strings, or empty topics
                [brokers, topic] if !brokers.is_empty() && !topic.is_empty() => Self::Kafka {
                    brokers: brokers.split(',').map(ToString::to_string).collect(),
                    topic: topic.to_string(),
                    partition: None,
                },
                [brokers, topic, partition] if !brokers.is_empty() && !topic.is_empty() => {
                    Self::Kafka {
                        brokers: brokers.split(',').map(ToString::to_string).collect(),
                        topic: topic.to_string(),
                        partition: Some(partition.parse()?),
                    }
                }
                _ => return Err(TargetError::Invalid),
            };

            return Ok(target);
        }

        Ok(Self::Path(s.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;

    macro_rules! test_parse {
        (
			$name:ident,
			input = $input:literal,
			want = $($want:tt)+
		) => {
            paste::paste! {
                #[test]
                fn [<test_parse_ $name>]() {
                    let input: &str = $input;
                    assert_matches!(input.parse::<Target>(), $($want)+);
                }
            }
        };
    }

    test_parse!(empty, input = "kafka://", want = Err(TargetError::Invalid));

    test_parse!(
        empty_brokers,
        input = "kafka:///topic",
        want = Err(TargetError::Invalid)
    );

    test_parse!(
        empty_topic,
        input = "kafka:////partition",
        want = Err(TargetError::Invalid)
    );

    test_parse!(
        kafka_host_no_topic,
        input = "kafka://bananas.local",
        want = Err(TargetError::Invalid)
    );

    test_parse!(
        kafka_host_no_topic_trailing_slash,
        input = "kafka://bananas.local/",
        want = Err(TargetError::Invalid)
    );

    test_parse!(
        kafka_host,
        input = "kafka://bananas.local/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["bananas.local"]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_host_port,
        input = "kafka://bananas.local:9092/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["bananas.local:9092"]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_multiple_host,
        input = "kafka://platanos.local,bananas.local:9092,another.banana:9092/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, [
                "platanos.local",
                "bananas.local:9092",
                "another.banana:9092"
            ]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_ipv4,
        input = "kafka://127.0.0.1/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["127.0.0.1"]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_ipv4_port,
        input = "kafka://127.0.0.1:9092/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["127.0.0.1:9092"]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_ipv6,
        input = "kafka://[2001:db8::1]/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["[2001:db8::1]"]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_ipv6_port,
        input = "kafka://[2001:db8::1]:9092/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["[2001:db8::1]:9092"]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_multiple_ip,
        input = "kafka://127.0.0.1,1.2.3.4:9092,[2001:db8::1]:9092/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, [
                "127.0.0.1",
                "1.2.3.4:9092",
                "[2001:db8::1]:9092"
            ]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_multiple_mixed,
        input = "kafka://localhost:9092,1.2.3.4:9092,[2001:db8::1]:9092/topic",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, [
                "localhost:9092",
                "1.2.3.4:9092",
                "[2001:db8::1]:9092"
            ]);
            assert_eq!(topic, "topic");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        kafka_partition,
        input = "kafka://localhost:9092/bananas/42",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["localhost:9092"]);
            assert_eq!(topic, "bananas");
            assert_eq!(partition, Some(42));
        }
    );

    test_parse!(
        kafka_partition_trailing_slash,
        input = "kafka://localhost:9092/bananas/42/",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["localhost:9092"]);
            assert_eq!(topic, "bananas");
            assert_eq!(partition, Some(42));
        }
    );

    test_parse!(
        kafka_topic_trailing_slash,
        input = "kafka://localhost:9092/bananas/",
        want = Ok(Target::Kafka{brokers, topic, partition}) => {
            assert_eq!(brokers, ["localhost:9092"]);
            assert_eq!(topic, "bananas");
            assert_eq!(partition, None);
        }
    );

    test_parse!(
        relative_path,
        input = "data.bin",
        want = Ok(Target::Path(p)) => {
            assert_eq!(p.to_str(), Some("data.bin"));
        }
    );

    test_parse!(
        absolute_path,
        input = "/test/data.bin",
        want = Ok(Target::Path(p)) => {
            assert_eq!(p.to_str(), Some("/test/data.bin"));
        }
    );
}

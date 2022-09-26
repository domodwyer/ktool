// TODO: doc

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct Message {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: Option<Timestamp>,
    headers: Option<BTreeMap<String, Vec<u8>>>,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
}

impl Message {
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        timestamp: Option<Timestamp>,
        headers: Option<BTreeMap<String, Vec<u8>>>,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            timestamp,
            headers,
            key,
            payload,
        }
    }

    /// Get the message offset.
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Get a reference to the message's topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.topic.as_ref()
    }

    /// Get the message's partition.
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Get a reference to the message's timestamp.
    #[must_use]
    pub fn timestamp(&self) -> Option<&Timestamp> {
        self.timestamp.as_ref()
    }

    /// Get a reference to the message's headers.
    #[must_use]
    pub fn headers(&self) -> Option<&BTreeMap<String, Vec<u8>>> {
        self.headers.as_ref()
    }

    /// Get a reference to the message's key.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Get a reference to the message's payload.
    #[must_use]
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("offset", &self.offset)
            .field("timestamp", &self.timestamp)
            .field(
                "headers",
                &match &self.headers {
                    Some(v) => v
                        .iter()
                        .map(|(k, v)| format!("{} => {}", k, maybe_string(v)))
                        .collect::<Vec<_>>()
                        .join(",  "),
                    None => "NONE".to_string(),
                },
            )
            .field("key", &self.key.as_ref().map(maybe_string))
            .field("payload", &self.payload.as_ref().map(maybe_string))
            .finish()
    }
}

fn maybe_string<T>(t: T) -> String
where
    T: AsRef<[u8]>,
{
    let t = t.as_ref();
    match std::str::from_utf8(t) {
        Ok(v) => v.to_string(),
        Err(_) => format!("<{} BYTES>", t.len()),
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub enum Timestamp {
    CreateTime(i64),
    LogAppendTime(i64),
}

impl TryFrom<rdkafka::Timestamp> for Timestamp {
    type Error = ();

    fn try_from(value: rdkafka::Timestamp) -> Result<Self, Self::Error> {
        match value {
            rdkafka::Timestamp::NotAvailable => Err(()),
            rdkafka::Timestamp::CreateTime(v) => Ok(Timestamp::CreateTime(v)),
            rdkafka::Timestamp::LogAppendTime(v) => Ok(Timestamp::LogAppendTime(v)),
        }
    }
}

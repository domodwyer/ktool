//! JSON representation of a [`Message`], used in CLI output.

use std::collections::BTreeMap;

use crate::message::{Message, Timestamp};

use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Deserialize, Serialize)]
struct Base64Bytes<'m>(#[serde(serialize_with = "base64_serialise")] &'m [u8]);

fn base64_serialise<S>(v: &'_ [u8], s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(v);
    s.serialize_str(&encoded)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JsonMessage<'m> {
    topic: &'m str,
    partition: i32,
    offset: i64,
    timestamp: Option<Timestamp>,
    headers: Option<BTreeMap<&'m str, Base64Bytes<'m>>>,
    key: Option<Base64Bytes<'m>>,
    payload: Option<Base64Bytes<'m>>,
}

impl<'m> From<&'m Message> for JsonMessage<'m> {
    fn from(m: &'m Message) -> Self {
        Self {
            topic: m.topic(),
            partition: m.partition(),
            offset: m.offset(),
            timestamp: m.timestamp().cloned(),
            headers: m.headers().map(|v| {
                v.iter()
                    .map(|(k, v)| (k.as_str(), Base64Bytes(v)))
                    .collect()
            }),
            key: m.key().map(Base64Bytes),
            payload: m.payload().map(Base64Bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_json {
        (
            $name:ident,
            msg = $msg:expr,
            want = $want:literal
        ) => {
            paste::paste! {
                #[test]
                fn [<test_json_representation_ $name>]() {
                    let msg: Message = $msg;

					let json = JsonMessage::from(&msg);
                    let got = serde_json::to_string_pretty(&json).expect("failed to serialise message");

					let want = $want;
                    assert_eq!(got, $want, "got:\n{got}\nwant:\n{want}");
                }
            }
        };
    }

    test_json!(
        all_fields,
        msg = Message::new(
            "bananas",
            42,
            1234,
            Some(Timestamp::CreateTime(11223344)),
            Some(headers([("map_key1", "value"), ("map_key2", "plátanos")])),
            Some("plátanos".into()),
            Some("banana-payload-🍌".into()),
        ),
        want = r#"{
  "topic": "bananas",
  "partition": 42,
  "offset": 1234,
  "timestamp": {
    "CreateTime": 11223344
  },
  "headers": {
    "map_key1": "dmFsdWU=",
    "map_key2": "cGzDoXRhbm9z"
  },
  "key": "cGzDoXRhbm9z",
  "payload": "YmFuYW5hLXBheWxvYWQt8J+NjA=="
}"#
    );

    test_json!(
        optional_nones,
        msg = Message::new("bananas", 42, 1234, None, None, None, None),
        want = r#"{
  "topic": "bananas",
  "partition": 42,
  "offset": 1234,
  "timestamp": null,
  "headers": null,
  "key": null,
  "payload": null
}"#
    );

    test_json!(
        optional_empty,
        msg = Message::new(
            "bananas",
            42,
            1234,
            None,
            Some(Default::default()),
            Some(vec![]),
            Some(vec![])
        ),
        want = r#"{
  "topic": "bananas",
  "partition": 42,
  "offset": 1234,
  "timestamp": null,
  "headers": {},
  "key": "",
  "payload": ""
}"#
    );

    test_json!(
        non_printable,
        msg = Message::new(
            "bananas",
            42,
            1234,
            None,
            Some(headers([("test", [0x00, 0x42, 0x01, 0xFF])])),
            Some(vec![0x00, 0x42, 0x02, 0xFF]),
            Some(vec![0x00, 0x42, 0x03, 0xFF])
        ),
        want = r#"{
  "topic": "bananas",
  "partition": 42,
  "offset": 1234,
  "timestamp": null,
  "headers": {
    "test": "AEIB/w=="
  },
  "key": "AEIC/w==",
  "payload": "AEID/w=="
}"#
    );

    fn headers<T, K, V>(input: T) -> BTreeMap<String, Vec<u8>>
    where
        T: IntoIterator<Item = (K, V)>,
        K: ToString,
        V: Into<Vec<u8>>,
    {
        input
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into()))
            .collect()
    }
}

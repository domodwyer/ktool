//! Binary file format codec, serialising a [`Message`] into an on-disk format.

use std::io::ErrorKind;

use crate::message::Message;

use thiserror::Error;

/// Limit messages read from files to a maximum size of 1024 MiB each.
///
/// This prevents a malicious file from allocating TBs of memory.
const MAX_MSG_SIZE: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("file EOF")]
    Eof,

    #[error("i/o error: {}", .0)]
    IO(#[from] std::io::Error),

    #[error("serialisation error: {}", .0)]
    Serialisation(#[from] bincode::Error),
}

pub(crate) fn serialise_into<W>(mut w: W, msg: &Message) -> Result<(), CodecError>
where
    W: std::io::Write,
{
    let header = bincode::serialized_size(msg)?;
    w.write_all(&header.to_le_bytes())?;

    bincode::serialize_into(&mut w, msg)?;

    Ok(())
}

pub(crate) fn deserialise_from<R>(mut r: R) -> Result<Message, CodecError>
where
    R: std::io::Read,
{
    let mut header = [0; std::mem::size_of::<u64>()];

    // Attempt to read the message length header
    match r.read_exact(&mut header) {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Err(CodecError::Eof),
        Err(e) => return Err(CodecError::IO(e)),
    }

    // Construct the u64 from the raw little-endian bytes.
    let len = u64::from_le_bytes(header);
    // Ensure it is a sensible size to avoid OOMing form a malicious file.
    if len > MAX_MSG_SIZE {
        panic!(
            "found message with byte size {}, max allowed {}",
            len, MAX_MSG_SIZE
        );
    }

    // Read the message of len bytes in length.
    let mut buf = Vec::new();
    buf.resize(len.try_into().expect("message size exceeds usize"), 0);
    r.read_exact(&mut buf)?;

    bincode::deserialize(&buf).map_err(CodecError::from)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::message::Timestamp;

    use super::*;

    macro_rules! test_round_trip {
        (
			$name:ident,
			msgs = [$($msg:expr),+]
		) => {
            paste::paste! {
                #[test]
                fn [<test_round_trip_ $name>]() {
					let mut buf = std::io::Cursor::new(Vec::new());

					$(
						let msg: Message = $msg;
						serialise_into(&mut buf, &msg).expect("should encode");
					)+

					buf.set_position(0);
					$(
						let decoded = deserialise_from(&mut buf).expect("should decode");
						assert_eq!($msg, decoded);
					)+
                }
            }
        };
    }

    test_round_trip!(
        empty,
        msgs = [Message::new("nullable", 0, 0, None, None, None, None)]
    );

    test_round_trip!(
        with_payload,
        msgs = [Message::new(
            "full",
            42,
            24,
            Some(Timestamp::CreateTime(1234)),
            None,
            Some(vec![1, 2, 3, 4, 5]),
            Some(vec![6, 7, 8, 9, 0]),
        )]
    );

    test_round_trip!(
        with_headers,
        msgs = [{
            let mut headers = BTreeMap::default();
            headers.insert("key".into(), "value".into());
            headers.insert("anotherkey".into(), vec![]);

            Message::new(
                "full",
                42,
                24,
                Some(Timestamp::CreateTime(1234)),
                Some(headers),
                Some(vec![1, 2, 3, 4, 5]),
                Some(vec![6, 7, 8, 9, 0]),
            )
        }]
    );

    test_round_trip!(
        multiple_empty,
        msgs = [
            Message::new("nullable", 0, 0, None, None, None, None),
            Message::new("nullable2", 0, 0, None, None, None, None)
        ]
    );

    test_round_trip!(
        multiple_populated,
        msgs = [
            Message::new(
                "populated",
                12,
                34,
                None,
                None,
                Some(vec![4, 4, 4, 4]),
                None
            ),
            {
                let mut headers = BTreeMap::default();
                headers.insert("key".into(), "value".into());
                headers.insert("anotherkey".into(), vec![]);

                Message::new(
                    "full",
                    42,
                    24,
                    Some(Timestamp::CreateTime(1234)),
                    Some(headers),
                    Some(vec![1, 2, 3, 4, 5]),
                    Some(vec![6, 7, 8, 9, 0]),
                )
            }
        ]
    );
}

use std::{cmp::Ordering, num::ParseIntError, str::FromStr};

use clap::Args;
use thiserror::Error;

use crate::message::{Message, Timestamp};

#[derive(Debug, Args, Clone)]
pub(crate) struct OffsetClap {
    /// Restrict messages read from the source to the specified offsets.
    ///
    /// Offsets are inclusive and in the form "start", or with an optional end
    /// offset as "start:end". Negative start values are relative to the current
    /// partition offset.
    ///
    ///   - read all messages from offset 42, inclusive: "42"
    ///
    ///   - read the 3 most recent messages: -3
    ///
    ///   - read messages 42 to 100, inclusive: "42:100"
    ///
    ///   - read all messages up to (and including) offset 1234: ":1234"
    ///
    ///   - read only message number 42: "42:42"
    ///
    #[clap(short, long, parse(try_from_str), conflicts_with = "time-range")]
    offset: Option<OffsetRange>,

    /// Restrict messages read from the source to the specified producer
    /// timestamps.
    ///
    /// Timestamps should be formatted as unix timestamps (seconds since epoch),
    /// and have the same format as offset ranges. Unlike offsets, a kafka
    /// consumer cannot seek based on the timestamp and instead client-side
    /// filtering is performed.
    #[clap(short, long, parse(try_from_str), conflicts_with = "offset")]
    time_range: Option<TimeRange>,
}

impl OffsetClap {
    pub(crate) fn cmp(&self, msg: &Message) -> Option<Ordering> {
        if let Some(range) = self.time_range {
            assert_eq!(self.offset, None);
            return range.cmp(msg);
        }

        let range = self.offset.unwrap_or_default();
        range.cmp(msg)
    }

    /// Return the minimum offset to read, if known.
    pub(crate) fn start_offset(&self) -> Option<i64> {
        if let Some(offset) = self.offset {
            return Some(offset.start);
        }

        None
    }

    /// Wrap the provided iter in an adaptor to limit messages to this offset
    /// range, if any.
    pub fn wrap_iter<I>(&self, iter: I) -> OffsetAwareIter<I> {
        OffsetAwareIter(iter, self.clone())
    }
}

/// An iterator adaptor that constrains the output messages to those that match
/// the configured offset range, if any.
pub(crate) struct OffsetAwareIter<I>(I, OffsetClap);

impl<I> Iterator for OffsetAwareIter<I>
where
    I: Iterator<Item = Result<Message, Box<dyn std::error::Error>>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.next()? {
                Ok(v) if self.1.cmp(&v).is_none() => {
                    panic!("cannot compare offset");
                }
                Ok(v) if self.1.cmp(&v) == Some(Ordering::Greater) => return None,
                Ok(v) if self.1.cmp(&v) == Some(Ordering::Less) => {
                    eprintln!("[-] skipping offset {}", v.offset());
                }
                v => return Some(v),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

#[derive(Debug, Error)]
pub enum OffsetError {
    #[error("invalid offset: {}", .0)]
    ParseInt(#[from] ParseIntError),

    #[error("invalid offset range format (expected 'start', or 'start:end')")]
    TooManyParts,
}

// TODO: doc inclusive
#[derive(Debug, PartialEq, Default, Eq, Clone, Copy)]
pub(crate) struct OffsetRange {
    start: i64,
    end: Option<i64>,
}

impl OffsetRange {
    pub fn cmp(&self, msg: &Message) -> Option<Ordering> {
        if msg.offset() < self.start {
            return Some(Ordering::Less);
        }

        if let Some(end) = self.end {
            if msg.offset() > end {
                return Some(Ordering::Greater);
            }
        }

        Some(Ordering::Equal)
    }
}

impl FromStr for OffsetRange {
    type Err = OffsetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (start, end) = parse(s)?;
        Ok(OffsetRange { start, end })
    }
}

// TODO: doc inclusive
#[derive(Debug, PartialEq, Default, Eq, Clone, Copy)]
pub(crate) struct TimeRange {
    start: i64,
    end: Option<i64>,
}

impl TimeRange {
    pub fn cmp(&self, msg: &Message) -> Option<Ordering> {
        let created_at = match msg.timestamp()? {
            Timestamp::CreateTime(v) => *v,
            Timestamp::LogAppendTime(v) => *v,
        };

        if created_at < self.start {
            return Some(Ordering::Less);
        }

        if let Some(end) = self.end {
            if created_at > end {
                return Some(Ordering::Greater);
            }
        }

        Some(Ordering::Equal)
    }
}

// TODO(dom:test): TimeRange
// TODO(dom:test): TimeRange::cmp
// TODO(dom:test): OffsetRange::cmp

impl FromStr for TimeRange {
    type Err = OffsetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (start, end) = parse(s)?;
        Ok(TimeRange { start, end })
    }
}

fn parse(s: &str) -> Result<(i64, Option<i64>), OffsetError> {
    let parts = s
        .split(':')
        .map(|v| {
            if v.is_empty() {
                None
            } else {
                Some(i64::from_str(v))
            }
            .transpose()
        })
        .collect::<Result<Vec<Option<_>>, _>>()?;

    Ok(match parts.as_slice() {
        [None] => Default::default(),
        [Some(a)] => (*a, None),
        [a, b] => (a.unwrap_or_default(), *b),
        _ => Err(OffsetError::TooManyParts)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;

    macro_rules! test_parse_offset {
        (
			$name:ident,
			input = $input:literal,
			want = $($want:tt)+
		) => {
            paste::paste! {
                #[test]
                fn [<test_parse_offset_ $name>]() {
                    let input: &str = $input;
                    assert_matches!(input.parse::<OffsetRange>(), $($want)+);
                }
            }
        };
    }

    test_parse_offset!(
        start_only,
        input = "42",
        want = Ok(OffsetRange {
            start: 42,
            end: None
        })
    );

    test_parse_offset!(
        end_only,
        input = ":42",
        want = Ok(OffsetRange {
            start: 0,
            end: Some(42)
        })
    );

    test_parse_offset!(
        start_and_end,
        input = "42:24",
        want = Ok(OffsetRange {
            start: 42,
            end: Some(24)
        })
    );

    test_parse_offset!(
        unspecified,
        input = "",
        want = Ok(OffsetRange {
            start: 0,
            end: None,
        })
    );

    test_parse_offset!(
        invalid_start,
        input = "bananas",
        want = Err(OffsetError::ParseInt(_))
    );

    test_parse_offset!(
        invalid_end,
        input = ":bananas",
        want = Err(OffsetError::ParseInt(_))
    );

    test_parse_offset!(
        invalid_both,
        input = "bananas:bananas",
        want = Err(OffsetError::ParseInt(_))
    );

    test_parse_offset!(
        both_specified_invalid_start,
        input = "bananas:24",
        want = Err(OffsetError::ParseInt(_))
    );

    test_parse_offset!(
        both_specified_invalid_end,
        input = "42:bananas",
        want = Err(OffsetError::ParseInt(_))
    );

    test_parse_offset!(
        empty_parts,
        input = ":",
        want = Ok(OffsetRange {
            start: 0,
            end: None,
        })
    );

    test_parse_offset!(
        too_many_parts,
        input = "1:2:3",
        want = Err(OffsetError::TooManyParts)
    );
}

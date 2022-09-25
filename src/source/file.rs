use std::{fs::File, io::BufReader, path::PathBuf};

use anyhow::Context;

use crate::{
    file_codec::{self, CodecError},
    message::Message,
};

pub(crate) fn new(
    path: PathBuf,
) -> anyhow::Result<impl Iterator<Item = Result<Message, Box<dyn std::error::Error>>>> {
    let f = File::open(&path)
        .with_context(|| format!("failed to open file {} for reading", path.display()))?;

    // Use buffered I/O for increased performance.
    let mut f = BufReader::new(f);

    Ok(std::iter::from_fn(
        move || match file_codec::deserialise_from(&mut f) {
            Err(CodecError::Eof) => None,
            v => Some(v.map_err(Into::into)),
        },
    ))
}

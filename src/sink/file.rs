use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use anyhow::Context;

use crate::{file_codec, message::Message};

use super::Sink;

pub(crate) struct FileSink {
    f: BufWriter<File>,
}

impl FileSink {
    pub(crate) fn new(path: &Path) -> anyhow::Result<Self> {
        let f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("failed to open file {} for writing", path.display()))?;

        Ok(Self {
            f: BufWriter::new(f),
        })
    }
}

impl Sink for FileSink {
    fn write(&mut self, msg: &Message) -> anyhow::Result<()> {
        file_codec::serialise_into(&mut self.f, msg).context("failed to write file")
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.f.flush().context("failed to flush file")
    }
}

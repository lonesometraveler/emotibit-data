//! Writer types and functions
use crate::types::Csv;
use anyhow::Result;

/// Use `WriterBuilder` to build this struct.
pub struct Writer {
    writer: csv::Writer<std::fs::File>,
}

impl Writer {
    /// Writes a `DataPacket` to a file
    pub fn write<T: Csv>(&mut self, datapacket: &T) -> Result<()> {
        for item in datapacket.csv() {
            self.writer.write_record(&item)?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

/// Builder struct for `Writer`
pub struct WriterBuilder {
    builder: csv::WriterBuilder,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WriterBuilder {
    pub fn new() -> Self {
        WriterBuilder {
            builder: csv::WriterBuilder::new(),
        }
    }
    /// Creates `Writer` with a flie path
    pub fn from_path(mut self, path: &str) -> Result<Writer> {
        Ok(Writer {
            writer: self.builder.flexible(true).from_path(path)?,
        })
    }
}

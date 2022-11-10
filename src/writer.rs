use crate::types::Csv;
use anyhow::Result;

pub struct ParserWriter {
    writer: csv::Writer<std::fs::File>,
}

impl ParserWriter {
    pub fn write<T: Csv>(&mut self, datapacket: T) -> Result<()> {
        for item in datapacket.csv() {
            self.writer.write_record(&item)?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

pub struct ParserWriterBuilder {
    builder: csv::WriterBuilder,
}

impl Default for ParserWriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ParserWriterBuilder {
    pub fn new() -> Self {
        ParserWriterBuilder {
            builder: csv::WriterBuilder::new(),
        }
    }

    pub fn from_path(mut self, path: &str) -> Result<ParserWriter> {
        Ok(ParserWriter {
            writer: self.builder.flexible(true).from_path(path)?,
        })
    }
}

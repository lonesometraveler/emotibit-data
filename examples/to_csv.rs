use anyhow::Result;
use emotibit_data::{parser, types::DataPacket, writer};
use std::{collections::HashSet, fs::File, io::Write, path::PathBuf};

fn main() {
    match read_write(Some(PathBuf::from("raw_data.csv"))) {
        Ok(()) => println!("success"),
        Err(e) => println!("{:?}", e),
    }
}

fn read_write(path_buf: Option<PathBuf>) -> Result<()> {
    let filename: &str = path_buf
        .as_ref()
        .and_then(|name| name.file_stem())
        .and_then(|name| name.to_str())
        .unwrap_or("default");

    let (datapackets, errors): (Vec<_>, Vec<_>) = parser::get_packets(
        &path_buf
            .as_ref()
            .unwrap()
            .clone()
            .into_os_string()
            .into_string()
            .unwrap(),
    )?
    .into_iter()
    .partition(Result::is_ok);

    let mut output_file = path_buf.clone().unwrap();
    output_file.set_file_name(format!("{}_ERROR.csv", filename));

    // Write Errors
    let mut output = File::create(output_file.clone())?;
    for err in errors.into_iter().filter_map(|result| result.err()) {
        writeln!(output, "{}", err)?;
    }

    // Write TimeSyncs
    output_file.set_file_name("timesyncs.csv");
    let syncs = parser::find_syncs(&datapackets)?;
    let mut writer = writer::ParserWriterBuilder::new().from_path(output_file.to_str().unwrap())?;

    for packet in syncs {
        writer.write(packet)?;
    }

    // Extract TypeTags
    let set: HashSet<&str> = HashSet::from_iter(
        datapackets
            .iter()
            .map(|result| result.as_ref().unwrap().data_type.as_str()),
    );

    // Write Packets
    let packets: Vec<DataPacket> = datapackets
        .into_iter()
        .filter_map(|result| result.ok())
        .collect();

    for t in set.iter() {
        output_file.set_file_name(format!("{}_{}.csv", filename, t));
        let mut writer =
            writer::ParserWriterBuilder::new().from_path(output_file.to_str().unwrap())?;

        for packet in packets
            .iter()
            .cloned()
            .filter(|x| x.data_type.as_str() == *t)
        {
            writer.write(packet)?;
        }
    }

    Ok(())
}

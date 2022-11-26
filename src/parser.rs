//! Parser functions
use crate::types::{DataPacket, DataType, TimeSync, TimeSyncMap};
use anyhow::{anyhow, Result};
use chrono::{offset::TimeZone, DateTime, Local, NaiveDateTime};
use csv::ReaderBuilder;
use itertools::izip;

const PARSER_VERSION: &str = "0.1.0";
const MIN_SYNCS_REQUIRED: usize = 3;

/// Reads a csv file and creates `DataPacket`s
pub fn get_packets(file_path: &str) -> Result<Vec<Result<DataPacket>>> {
    let mut vec: Vec<Result<DataPacket>> = Vec::new();
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_path(file_path)?;
    for record in reader.records() {
        match DataPacket::try_from(record.as_ref().unwrap()) {
            Ok(packet) => vec.push(Ok(split_tx_or_as_is(packet)?)),
            Err(e) => vec.push(Err(anyhow!("{}, record: {:?}", e, record))),
        }
    }
    Ok(vec)
}

fn split_tx_or_as_is(x: DataPacket) -> Result<DataPacket> {
    if let DataType::TX(data) = &x.data_type {
        if let (Some(tag1), Some(val1), Some(tag2), Some(val2)) =
            (data.get(0), data.get(1), data.get(2), data.get(3))
        {
            let data_type = match (tag1.as_ref(), tag2.as_ref()) {
                ("LC", "LM") => Some(DataType::TxLcLm(vec![val1.parse()?, val2.parse()?])),
                ("TL", "LC") => Some(DataType::TxTlLc((val1.to_owned(), val2.parse()?))),
                _ => None,
            };

            if let Some(data_type) = data_type {
                return Ok(DataPacket {
                    host_timestamp: None,
                    emotibit_timestamp: x.emotibit_timestamp,
                    packet_id: x.packet_id,
                    data_points: x.data_points,
                    version: x.version,
                    reliability: x.reliability,
                    data_type,
                });
            }
            return Err(anyhow!("Invalid data"));
        }
    }
    Ok(x)
}

/// Finds blocks of RD, TL, and AK and creates `TimeSync`s
pub fn find_syncs(packets: &[Result<DataPacket>]) -> Result<Vec<TimeSync>> {
    use DataType::*;
    let mut vec = vec![];
    let syncs: Vec<&DataPacket> = packets
        .iter()
        .filter_map(|x| {
            x.as_ref().ok().and_then(|x| match x.data_type {
                RD(_) | TL(_) | AK(_) => Some(x),
                _ => None,
            })
        })
        .collect();

    if syncs.len() < MIN_SYNCS_REQUIRED {
        return Err(anyhow!("Not enough sync data"));
    }

    let syncs2 = syncs.clone();
    let syncs3 = syncs.clone();
    for (rd, tl, ak) in izip!(&syncs, &syncs2[1..], &syncs3[2..]) {
        if let (RD(_), TL(date_time), AK(_)) = (&rd.data_type, &tl.data_type, &ak.data_type) {
            vec.push(TimeSync {
                rd: rd.emotibit_timestamp,
                ts_received: tl.emotibit_timestamp,
                ts_sent: date_time.to_owned(),
                ak: ak.emotibit_timestamp,
                round_trip: tl.emotibit_timestamp - rd.emotibit_timestamp,
            });
        }
    }
    Ok(vec)
}

/// Creates a `TimeSyncMap`
pub fn generate_sync_map(packets: &[Result<DataPacket>]) -> Result<TimeSyncMap> {
    let filtered = packets
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .map(|p| p.emotibit_timestamp);

    let emotibit_start_time = filtered.clone().reduce(f64::min).unwrap();
    let emotibit_end_time = filtered.reduce(f64::max).unwrap();

    let syncs = find_syncs(packets)?;

    let quartiles: Vec<Option<&TimeSync>> = syncs
        .chunks(num::integer::div_ceil(syncs.len(), 4))
        .map(|x| {
            x.iter()
                .min_by(|a, b| a.round_trip.partial_cmp(&b.round_trip).unwrap())
        })
        .collect();

    let best_timestamps = match (
        quartiles.get(0),
        quartiles.get(1),
        quartiles.get(2),
        quartiles.get(3),
    ) {
        (Some(Some(x)), _, _, Some(Some(y))) => Some((x, y)),
        (_, Some(Some(x)), _, Some(Some(y))) => Some((x, y)),
        (_, Some(Some(x)), Some(Some(y)), _) => Some((x, y)),
        (Some(Some(x)), Some(Some(y)), _, _) => Some((x, y)),
        (_, _, Some(Some(x)), Some(Some(y))) => Some((x, y)),
        // TODO: add data from the same quartile
        _ => None,
    };

    let (p0, p1) = best_timestamps
        .ok_or_else(|| anyhow!("Cannot generate a time sync map from these:\n{:?}", syncs))?;
    let (tl0, te0) = get_tl_te(p0)?;
    let (tl1, te1) = get_tl_te(p1)?;

    Ok(TimeSyncMap {
        te0,
        te1,
        tl0,
        tl1,
        syncs_received: syncs.len(),
        emotibit_start_time,
        emotibit_end_time,
        parse_version: PARSER_VERSION.to_owned(),
    })
}

fn get_tl_te(sync: &TimeSync) -> Result<(f64, f64)> {
    let e0 = sync.ts_received;
    let ts = &sync.ts_sent;

    let pos = ts
        .rfind('-')
        .ok_or_else(|| anyhow!("Invalid date string. : {:?}", sync))?;
    let (head, tail) = ts.split_at(pos);

    let naive_date_time = NaiveDateTime::parse_from_str(head, "%Y-%m-%d_%H-%M-%S")?;
    let date_time: DateTime<Local> = Local.from_local_datetime(&naive_date_time).unwrap();
    let c = date_time.timestamp();

    let last_n_char = tail.len() - 1;
    let m: f64 = tail[1..].parse()?;
    let m = m / 10_i32.pow(last_n_char.try_into()?) as f64;

    let mut c0 = (c as f64) + m;
    c0 += sync.round_trip as f64 / 2_f64 / 1000_f64;

    Ok((c0, e0))
}

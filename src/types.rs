// Raw data packet architecture
// TIMESTAMP-PACKET#-#DATAPOINTS-TYPETAG-VERSION-RELIABILITY-PAYLOAD
// Timestamp: milliseconds since start of EmotiBit
// Packet Number: packet count since start of EmotiBit
// Number of Datapoints: Number of data points in the payload
// TypeTag: type of data being sent
// Version: version of packet protocol
// Reliability: data reliability score out of 100, currently always 100
// Payload: data to send

use anyhow::{anyhow, Result};
use csv::StringRecord;
use std::str::FromStr;

pub trait Csv {
    fn csv(&self) -> Vec<StringRecord>;
}

#[derive(Debug, Clone)]
pub struct DataPacket {
    pub timestamp: f32,
    pub packet_id: u32,
    pub data_points: u8,
    pub version: u8,
    pub reliability: u8,
    pub data_type: DataType,
}

impl Csv for DataPacket {
    fn csv(&self) -> Vec<StringRecord> {
        let mut vec = Vec::new();
        let payload = Self::parse_data_type(&self.data_type, self.data_type.payload());
        for p in payload {
            vec.push(StringRecord::from(vec![
                self.timestamp.to_string(),
                self.packet_id.to_string(),
                self.data_points.to_string(),
                self.data_type.as_str().to_owned(),
                self.version.to_string(),
                self.reliability.to_string(),
                p.to_string(),
            ]));
        }
        vec
    }
}
impl DataPacket {
    fn parse_data_type(data_type: &DataType, payload: Vec<String>) -> Vec<String> {
        use DataType::*;
        match data_type {
            TxLcLm(_) | TxTlLc(_) => vec![format!(
                "{},{}",
                payload.get(0).unwrap(),
                payload.get(1).unwrap()
            )],
            _ => payload,
        }
    }
}

impl TryFrom<&StringRecord> for DataPacket {
    type Error = anyhow::Error;
    fn try_from(r: &StringRecord) -> Result<Self, Self::Error> {
        if let (
            Some(timestamp),
            Some(packet_id),
            Some(data_points),
            Some(data_type),
            Some(version),
            Some(reliability),
        ) = (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5))
        {
            Ok(DataPacket {
                timestamp: timestamp.parse()?,
                packet_id: packet_id.parse()?,
                data_points: data_points.parse()?,
                version: version.parse()?,
                reliability: reliability.parse()?,
                data_type: get_data_type(r, data_type)?,
            })
        } else {
            Err(anyhow!("Missing Column, record: {:?}", r))
        }
    }
}

impl TryFrom<&str> for DataPacket {
    type Error = anyhow::Error;
    fn try_from(str: &str) -> Result<Self, Self::Error> {
        let r: Vec<&str> = str.split(',').collect();
        let r = csv::ByteRecord::from(r);
        let r = &StringRecord::from_byte_record(r)?;
        r.try_into()
    }
}

#[test]
fn string_to_data() {
    let input = "1126349,49106,10,PI,1,100,156593,156471,156372,156300,156205,156136,156130,156103,156051,156103";
    let packet: DataPacket = input.try_into().unwrap();
    assert_eq!(packet.packet_id, 49106);
    assert_eq!(packet.data_points, 10);
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    EA(Vec<f32>),
    EL(Vec<f32>),
    ER(Vec<f32>),
    PI(Vec<u32>),
    PR(Vec<u32>),
    PG(Vec<u32>),
    O2,
    T0(Vec<f32>),
    T1(Vec<f32>),
    TH(Vec<f32>),
    H0,
    AX(Vec<f32>),
    AY(Vec<f32>),
    AZ(Vec<f32>),
    GX(Vec<f32>),
    GY(Vec<f32>),
    GZ(Vec<f32>),
    MX(Vec<i32>),
    MY(Vec<i32>),
    MZ(Vec<i32>),
    BV(Vec<f32>),
    BATLV(Vec<u32>), // B%
    BS,
    BL,
    DC,
    DO,
    SD,
    RS,
    DB,
    AK(Vec<String>),
    RD(Vec<String>),
    TE,
    TL(String),
    TU,
    TX(Vec<String>),
    TxTlLc((String, f32)),
    TxLcLm(Vec<f32>),
    EM(Vec<String>),
    EI,
    HR(Vec<i32>),
    BI(Vec<i32>),
    SA(Vec<f32>),
    SF(Vec<f32>),
    SR(Vec<f32>),
    // Computer data TypeTags (sent over reliable channel e.g. Control)
    GL,
    GS,
    GB,
    BA,
    UN(Vec<String>),
    LM,
    // Control TypeTags
    RB(String),
    RE,
    MN,
    ML,
    MM,
    MO,
    MH,
    ED,
    SPLUS,  // S+
    SMINUS, // S-
    // Advertising TypeTags
    PN,
    PO,
    HE,
    HH,
    EC,
}

impl DataType {
    pub fn as_str(&self) -> &'static str {
        use DataType::*;
        match self {
            EA(_) => "EA",
            EL(_) => "EL",
            ER(_) => "ER",
            PI(_) => "PI",
            PR(_) => "PR",
            PG(_) => "PG",
            O2 => "O2",
            T0(_) => "T0",
            T1(_) => "T1",
            TH(_) => "TH",
            H0 => "H0",
            AX(_) => "AX",
            AY(_) => "AY",
            AZ(_) => "AZ",
            GX(_) => "GX",
            GY(_) => "GY",
            GZ(_) => "GZ",
            MX(_) => "MX",
            MY(_) => "MY",
            MZ(_) => "MZ",
            BV(_) => "BV",
            BATLV(_) => "B%",
            BS => "BS",
            BL => "BL",
            DC => "DC",
            DO => "DO",
            SD => "SD",
            RS => "RS",
            DB => "DB",
            AK(_) => "AK",
            RD(_) => "RD",
            TE => "TE",
            TL(_) => "TL",
            TU => "TU",
            TX(_) => "TX",
            TxTlLc(_) => "TX_TL_LC",
            TxLcLm(_) => "TX_LC_LM",
            EM(_) => "EM",
            EI => "EI",
            HR(_) => "HR",
            BI(_) => "BI",
            SA(_) => "SA",
            SF(_) => "SF",
            SR(_) => "SR",
            // Computer data TypeTags (sent over reliable channel e.g. Control)
            GL => "GL",
            GS => "GS",
            GB => "GB",
            BA => "BA",
            UN(_) => "UN",
            LM => "LM",
            // Control TypeTags
            RB(_) => "RB",
            RE => "RE",
            MN => "MN",
            ML => "ML",
            MM => "MM",
            MO => "MO",
            MH => "MH",
            ED => "ED",
            SPLUS => "S+",  // S+
            SMINUS => "S-", // S-
            // Advertising TypeTags
            PN => "PN",
            PO => "PO",
            HE => "HE",
            HH => "HH",
            EC => "EC",
        }
    }

    pub fn payload(&self) -> Vec<String> {
        use DataType::*;
        match self {
            EA(v) => v.iter().map(|p| p.to_string()).collect(),
            EL(v) => v.iter().map(|p| p.to_string()).collect(),
            ER(v) => v.iter().map(|p| p.to_string()).collect(),
            PI(v) => v.iter().map(|p| p.to_string()).collect(),
            PR(v) => v.iter().map(|p| p.to_string()).collect(),
            PG(v) => v.iter().map(|p| p.to_string()).collect(),
            O2 => vec![],
            T0(v) => v.iter().map(|p| p.to_string()).collect(),
            T1(v) => v.iter().map(|p| p.to_string()).collect(),
            TH(v) => v.iter().map(|p| p.to_string()).collect(),
            H0 => vec![],
            AX(v) => v.iter().map(|p| p.to_string()).collect(),
            AY(v) => v.iter().map(|p| p.to_string()).collect(),
            AZ(v) => v.iter().map(|p| p.to_string()).collect(),
            GX(v) => v.iter().map(|p| p.to_string()).collect(),
            GY(v) => v.iter().map(|p| p.to_string()).collect(),
            GZ(v) => v.iter().map(|p| p.to_string()).collect(),
            MX(v) => v.iter().map(|p| p.to_string()).collect(),
            MY(v) => v.iter().map(|p| p.to_string()).collect(),
            MZ(v) => v.iter().map(|p| p.to_string()).collect(),
            BV(v) => v.iter().map(|p| p.to_string()).collect(),
            BATLV(v) => v.iter().map(|p| p.to_string()).collect(),
            BS => vec![],
            BL => vec![],
            DC => vec![],
            DO => vec![],
            SD => vec![],
            RS => vec![],
            DB => vec![],
            AK(sv) => sv.to_vec(),
            RD(sv) => sv.to_vec(),
            TE => vec![],
            TL(s) => vec![s.to_owned()],
            TU => vec![],
            TX(sv) => sv.to_vec(),
            TxTlLc((s, f)) => vec![s.to_owned(), f.to_string()],
            TxLcLm(v) => v.iter().map(|p| p.to_string()).collect(),
            EM(sv) => sv.to_vec(),
            EI => vec![],
            HR(v) => v.iter().map(|p| p.to_string()).collect(),
            BI(v) => v.iter().map(|p| p.to_string()).collect(),
            SA(v) => v.iter().map(|p| p.to_string()).collect(),
            SF(v) => v.iter().map(|p| p.to_string()).collect(),
            SR(v) => v.iter().map(|p| p.to_string()).collect(),
            // // Computer data TypeTags (sent over reliable channel e.g. Control)
            GL => vec![],
            GS => vec![],
            GB => vec![],
            BA => vec![],
            UN(sv) => sv.to_vec(),
            LM => vec![],
            // // Control TypeTags
            RB(s) => vec![s.to_owned()],
            RE => vec![],
            MN => vec![],
            ML => vec![],
            MM => vec![],
            MO => vec![],
            MH => vec![],
            ED => vec![],
            SPLUS => vec![],  // S+
            SMINUS => vec![], // S-
            // // Advertising TypeTags
            PN => vec![],
            PO => vec![],
            HE => vec![],
            HH => vec![],
            EC => vec![],
        }
    }
}

pub fn get_data_type(record: &StringRecord, type_str: &str) -> Result<DataType> {
    let skip_to_payload = 6_usize;
    match type_str {
        "RB" => Ok(DataType::RB(to_string(record, skip_to_payload)?)),
        "AK" => Ok(DataType::AK(to_string_vec(record, skip_to_payload))),
        "EA" => Ok(DataType::EA(to_vec::<f32>(record, skip_to_payload)?)),
        "EL" => Ok(DataType::EL(to_vec::<f32>(record, skip_to_payload)?)),
        "ER" => Ok(DataType::ER(to_vec::<f32>(record, skip_to_payload)?)),
        "PI" => Ok(DataType::PI(to_vec::<u32>(record, skip_to_payload)?)),
        "PR" => Ok(DataType::PR(to_vec::<u32>(record, skip_to_payload)?)),
        "PG" => Ok(DataType::PG(to_vec::<u32>(record, skip_to_payload)?)),
        "T0" => Ok(DataType::T0(to_vec::<f32>(record, skip_to_payload)?)),
        "T1" => Ok(DataType::T1(to_vec::<f32>(record, skip_to_payload)?)),
        "TH" => Ok(DataType::TH(to_vec::<f32>(record, skip_to_payload)?)),
        "AX" => Ok(DataType::AX(to_vec::<f32>(record, skip_to_payload)?)),
        "AY" => Ok(DataType::AY(to_vec::<f32>(record, skip_to_payload)?)),
        "AZ" => Ok(DataType::AZ(to_vec::<f32>(record, skip_to_payload)?)),
        "GX" => Ok(DataType::GX(to_vec::<f32>(record, skip_to_payload)?)),
        "GY" => Ok(DataType::GY(to_vec::<f32>(record, skip_to_payload)?)),
        "GZ" => Ok(DataType::GZ(to_vec::<f32>(record, skip_to_payload)?)),
        "MX" => Ok(DataType::MX(to_vec::<i32>(record, skip_to_payload)?)),
        "MY" => Ok(DataType::MY(to_vec::<i32>(record, skip_to_payload)?)),
        "MZ" => Ok(DataType::MZ(to_vec::<i32>(record, skip_to_payload)?)),
        "BI" => Ok(DataType::BI(to_vec::<i32>(record, skip_to_payload)?)),
        "SA" => Ok(DataType::SA(to_vec::<f32>(record, skip_to_payload)?)),
        "SF" => Ok(DataType::SF(to_vec::<f32>(record, skip_to_payload)?)),
        "SR" => Ok(DataType::SR(to_vec::<f32>(record, skip_to_payload)?)),
        "BV" => Ok(DataType::BV(to_vec::<f32>(record, skip_to_payload)?)),
        "HR" => Ok(DataType::HR(to_vec::<i32>(record, skip_to_payload)?)),
        "B%" => Ok(DataType::BATLV(to_vec::<u32>(record, skip_to_payload)?)),
        "RD" => Ok(DataType::RD(to_string_vec(record, skip_to_payload))),
        "UN" => Ok(DataType::UN(to_string_vec(record, skip_to_payload))),
        "EM" => Ok(DataType::EM(to_string_vec(record, skip_to_payload))),
        "TL" => Ok(DataType::TL(to_string(record, skip_to_payload)?)),
        "TX" => Ok(DataType::TX(to_string_vec(record, skip_to_payload))),
        // TODO: add data types
        _ => Err(anyhow!("Unknown Type: {}, {:?}", type_str, record)),
    }
}

// Helpers
fn to_string(record: &StringRecord, index: usize) -> Result<String> {
    if let Some(data) = record.get(index) {
        Ok(data.to_string())
    } else {
        Err(anyhow!("Parse Error: {:?}", record))
    }
}

fn to_vec<T>(record: &StringRecord, index_from: usize) -> Result<Vec<T>>
where
    T: num::Num + FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    let mut errors = vec![];
    let vec = record
        .iter()
        .skip(index_from)
        .map(|x| x.to_string().trim().parse::<T>())
        .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
        .collect::<Vec<T>>();
    if errors.is_empty() {
        Ok(vec)
    } else {
        Err(anyhow!("Parse to Num Error: {:?}", record))
    }
}

fn to_string_vec(record: &StringRecord, index_from: usize) -> Vec<String> {
    record
        .iter()
        .skip(index_from)
        .map(|str| str.to_owned())
        .collect()
}

// Time Syncs
#[derive(Debug, Clone)]
pub struct TimeSync {
    pub rd: f32,
    pub ts_received: f32,
    pub ts_sent: String,
    pub ak: f32,
    pub round_trip: f32,
}

impl Csv for TimeSync {
    fn csv(&self) -> Vec<StringRecord> {
        vec![StringRecord::from(vec![
            self.rd.to_string(),
            self.ts_received.to_string(),
            self.ts_sent.to_owned(),
            self.ak.to_string(),
            self.round_trip.to_string(),
        ])]
    }
}

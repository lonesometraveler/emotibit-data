use anyhow::Result;
use emotibit_data::{parser, types::DataType};
use std::path::PathBuf;

fn main() {
    match average_hr(Some(PathBuf::from("raw_data.csv"))) {
        Ok(rate) => println!("Average heart rate: {} bpm", rate),
        Err(e) => println!("{:?}", e),
    }
}

fn average_hr(path_buf: Option<PathBuf>) -> Result<f32> {
    let rates: Vec<i32> = parser::get_packets(
        &path_buf
            .as_ref()
            .unwrap()
            .clone()
            .into_os_string()
            .into_string()
            .unwrap(),
    )?
    .into_iter()
    .filter_map(|x| x.ok())
    .filter_map(|x| match x.data_type {
        DataType::HR(v) => Some(v[0]),
        _ => None,
    })
    .collect();

    Ok(average(&rates))
}

fn average(numbers: &[i32]) -> f32 {
    numbers.iter().sum::<i32>() as f32 / numbers.len() as f32
}

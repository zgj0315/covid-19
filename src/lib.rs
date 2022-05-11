use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
pub async fn read_file_and_input_buffer(src_dir: &String, line_buf: Arc<Mutex<Vec<String>>>) {
    let path = Path::new(src_dir);
    for entry in path.read_dir().expect("read_dir call failed") {
        if let Ok(entry) = entry {
            if entry.path().is_file()
                && entry
                    .path()
                    .display()
                    .to_string()
                    .to_lowercase()
                    .ends_with(".csv")
            {
                let file = tokio::fs::File::open(entry.path()).await.unwrap();
                let mut buf_reader = tokio::io::BufReader::new(file).lines();
                while let Some(line) = buf_reader.next_line().await.unwrap() {
                    loop {
                        let mut line_list = line_buf.lock().unwrap();
                        if line_list.len() < 10 {
                            line_list.push(line);
                            drop(line_list);
                            break;
                        } else {
                            drop(line_list);
                            tokio::time::sleep(tokio::time::Duration::from_millis(3)).await;
                        }
                    }
                }
            }
        }
    }
}

pub async fn read_buffer_and_input_db(line_buf: Arc<Mutex<Vec<String>>>) {
    let mut sleep_time = 0;
    loop {
        let mut line_list = line_buf.lock().unwrap();
        if line_list.is_empty() {
            drop(line_list);
            if sleep_time > 30 {
                break;
            }
            sleep_time += 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        } else {
            let line = line_list[0].clone();
            line_list.remove(0);
            drop(line_list);
            sleep_time = 0;
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(line.as_bytes());
            for result in rdr.deserialize() {
                if result.is_ok() {
                    let record: Covid19DailyReport = result.unwrap();
                    if record.fips.is_some() && record.clone().fips.unwrap().eq("FIPS") {
                        // println!("this is header, {:?}", record);
                    } else {
                        println!("{:?}", record);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Covid19DailyReport {
    fips: Option<String>,             //美国境内县代码
    admin2: Option<String>,           //美国境内县名
    province_state: Option<String>,   //省
    country_region: Option<String>,   //国家
    last_update: Option<String>,      //最后更新时间
    lat: Option<f32>,                 //纬度
    long_: Option<f32>,               //经度
    confirmed: Option<u32>,           //累计确诊人数
    deaths: Option<u32>,              //累计死亡人数
    recovered: Option<u32>,           //累计康复人数
    active: Option<u32>,              //当前活跃病例，病例总数-康复总数-死亡总数
    combined_key: Option<String>,     //省+国家
    incident_rate: Option<f32>,       //发病率，每十万人的病例数
    case_fatality_ratio: Option<f32>, //病死率，死亡人数/病例数
}

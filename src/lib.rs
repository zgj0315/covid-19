use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{Local, TimeZone, Utc};
use clickhouse::{error::Result, Client, Row};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;
pub async fn read_file_and_input_buffer(src_dir: &str, line_buf: Arc<Mutex<Vec<String>>>) {
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
                    if line.starts_with("Province/State") {
                        // 这种格式数据不处理
                        break;
                    }
                    if line.starts_with("FIPS,") {
                        // 跳过表头
                        continue;
                    }
                    loop {
                        let mut line_list = line_buf.lock().unwrap();
                        if line_list.len() < 10000 {
                            line_list.push(line);
                            drop(line_list);
                            break;
                        } else {
                            drop(line_list);
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                    }
                }
            }
        }
    }
}

pub async fn read_buffer_and_input_db(line_buf: Arc<Mutex<Vec<String>>>) {
    let client = Client::default()
        .with_url("http://127.0.0.1:8123")
        .with_database("covid_19");
    ddl(&client).await.unwrap();
    let mut inserter = client
        .inserter("daily_report")
        .unwrap()
        .with_max_entries(100_000)
        .with_period(Some(Duration::from_secs(15)));
    let mut sleep_time = 0;
    let mut count = 0;
    let mut last_time = Local::now().timestamp_millis();
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
            // 手工读取字段
            let (fips, line) = line.split_once(",").unwrap();
            let fips = fips.to_owned();
            let (admin2, line) = line.split_once(",").unwrap();
            let admin2 = admin2.to_owned();
            let (province_state, line) = {
                if line.starts_with("\"") {
                    let (_, suffix) = line.split_once("\"").unwrap();
                    let (prefix, suffix) = suffix.split_once("\",").unwrap();
                    (prefix, suffix)
                } else {
                    let (prefix, suffix) = line.split_once(",").unwrap();
                    (prefix, suffix)
                }
            };
            let province_state = province_state.to_owned();
            let (country_region, line) = {
                if line.starts_with("\"") {
                    let (_, suffix) = line.split_once("\"").unwrap();
                    let (prefix, suffix) = suffix.split_once("\",").unwrap();
                    (prefix, suffix)
                } else {
                    let (prefix, suffix) = line.split_once(",").unwrap();
                    (prefix, suffix)
                }
            };
            let country_region = country_region.to_owned();
            let (last_update, line) = line.split_once(",").unwrap();
            let last_update = {
                if last_update.len() == 19 {
                    match Utc.datetime_from_str(&last_update, "%Y-%m-%d %H:%M:%S") {
                        Ok(utc) => utc.timestamp(),
                        Err(_) => {
                            // 更新时间缺失，丢弃本条数据
                            continue;
                        }
                    }
                } else if last_update.len() == 16 {
                    match Utc.datetime_from_str(&last_update, "%Y-%m-%d %H:%M") {
                        Ok(utc) => utc.timestamp(),
                        Err(_) => {
                            // 更新时间缺失，丢弃本条数据
                            continue;
                        }
                    }
                } else {
                    // 更新时间缺失，丢弃本条数据
                    continue;
                }
            };
            let (lat, line) = line.split_once(",").unwrap();
            let lat = {
                if lat.len() == 0 {
                    0.0
                } else {
                    let lat: f32 = lat.parse().unwrap();
                    lat
                }
            };
            let (long_, line) = line.split_once(",").unwrap();
            let long_ = {
                if long_.len() == 0 {
                    0.0
                } else {
                    let long_: f32 = long_.parse().unwrap();
                    long_
                }
            };
            let (confirmed, line) = line.split_once(",").unwrap();
            let confirmed = {
                if confirmed.len() == 0 || confirmed.starts_with("-") {
                    // 确诊数据缺失，丢弃本条数据
                    continue;
                } else {
                    let confirmed: u32 = confirmed.parse().unwrap();
                    confirmed
                }
            };
            let (deaths, line) = line.split_once(",").unwrap();
            let deaths = {
                if deaths.len() == 0 || deaths.starts_with("-") {
                    0
                } else {
                    let deaths: u32 = deaths.parse().unwrap();
                    deaths
                }
            };
            let (recovered, line) = line.split_once(",").unwrap();
            let recovered = {
                if recovered.len() == 0 || recovered.starts_with("-") {
                    0
                } else {
                    let recovered: u32 = recovered.parse().unwrap();
                    recovered
                }
            };
            let (active, line) = line.split_once(",").unwrap();
            let active = {
                if active.len() == 0 {
                    0
                } else {
                    let active: u32 = active.parse().unwrap();
                    active
                }
            };
            let (combined_key, line) = {
                if line.starts_with("\"") {
                    let (_, suffix) = line.split_once("\"").unwrap();
                    if !suffix.contains("\",") {
                        continue;
                    }
                    let (prefix, suffix) = suffix.split_once("\",").unwrap();
                    (prefix, suffix)
                } else {
                    if !line.contains(",") {
                        continue;
                    }
                    let (prefix, suffix) = line.split_once(",").unwrap();
                    (prefix, suffix)
                }
            };
            let combined_key = combined_key.to_owned();
            let (incident_rate, case_fatality_ratio) = line.split_once(",").unwrap();
            let incident_rate = {
                if incident_rate.len() == 0 {
                    0.0
                } else {
                    let incident_rate: f32 = incident_rate.parse().unwrap();
                    incident_rate
                }
            };
            let case_fatality_ratio = {
                if case_fatality_ratio.len() == 0 {
                    0.0
                } else {
                    let case_fatality_ratio: f32 = case_fatality_ratio.parse().unwrap();
                    case_fatality_ratio
                }
            };
            let daily_report = DailyReport {
                fips,
                admin2,
                province_state,
                country_region,
                last_update,
                lat,
                long_,
                confirmed,
                deaths,
                recovered,
                active,
                combined_key,
                incident_rate,
                case_fatality_ratio,
            };
            let _ = inserter.write(&daily_report).await;
            let _ = inserter.commit().await;
            count += 1;
            let now = Local::now().timestamp_millis();
            if count % 10000 == 0 {
                log::info!(
                    "data count: {}, speed: {}row/s",
                    count,
                    (10000 * 1000) / (now - last_time)
                );
                last_time = now;
            }
        }
    }
    let _ = inserter.end().await;
    log::info!("job finished, data count: {}", count);
}

#[derive(Debug, Deserialize, Serialize, Clone, Row)]
pub struct Covid19DailyReportCSV {
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

#[derive(Debug, Deserialize, Serialize, Clone, Row)]
pub struct DailyReport {
    fips: String,             //美国境内县代码
    admin2: String,           //美国境内县名
    province_state: String,   //省
    country_region: String,   //国家
    last_update: i64,         //最后更新时间
    lat: f32,                 //纬度
    long_: f32,               //经度
    confirmed: u32,           //累计确诊人数
    deaths: u32,              //累计死亡人数
    recovered: u32,           //累计康复人数
    active: u32,              //当前活跃病例，病例总数-康复总数-死亡总数
    combined_key: String,     //省+国家
    incident_rate: f32,       //发病率，每十万人的病例数
    case_fatality_ratio: f32, //病死率，死亡人数/病例数
}

async fn ddl(client: &Client) -> Result<()> {
    client
        .query("DROP TABLE IF EXISTS daily_report")
        .execute()
        .await?;
    client
        .query(
            "
            CREATE TABLE covid_19.daily_report
            (
                fips String,
                admin2 String,
                province_state String,
                country_region String,
                last_update DateTime64(0),
                lat Float32,
                long_ Float32,
                confirmed UInt32,
                deaths UInt32,
                recovered UInt32,
                active UInt32,
                combined_key String,
                incident_rate Float32,
                case_fatality_ratio Float32
            )
            ENGINE = MergeTree()
            PRIMARY KEY (province_state, country_region, last_update)
            ",
        )
        .execute()
        .await
}

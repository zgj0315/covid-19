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
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(line.as_bytes());
            for result in rdr.deserialize() {
                if result.is_ok() {
                    let record: Covid19DailyReportCSV = result.unwrap();
                    if record.fips.is_some() && record.clone().fips.unwrap().eq("FIPS") {
                    } else {
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
                        let report = DailyReport {
                            fips: match record.fips {
                                Some(value) => value,
                                None => "".to_string(),
                            },
                            admin2: match record.admin2 {
                                Some(value) => value,
                                None => "".to_string(),
                            },
                            province_state: match record.province_state {
                                Some(value) => value,
                                None => "".to_string(),
                            },
                            country_region: match record.country_region {
                                Some(value) => value,
                                None => "".to_string(),
                            },
                            last_update: match record.last_update {
                                Some(value) => {
                                    if value.len() == 19 {
                                        match Utc.datetime_from_str(&value, "%Y-%m-%d %H:%M:%S") {
                                            Ok(utc) => utc.timestamp(),
                                            Err(err) => {
                                                log::error!("err: {}, value: {}", err, value);
                                                0
                                            }
                                        }
                                    } else if value.len() == 16 {
                                        match Utc.datetime_from_str(&value, "%Y-%m-%d %H:%M") {
                                            Ok(utc) => utc.timestamp(),
                                            Err(err) => {
                                                log::error!("err: {}, value: {}", err, value);
                                                0
                                            }
                                        }
                                    } else {
                                        log::error!("value is err, {}", value);
                                        0
                                    }
                                }
                                None => 0,
                            },
                            lat: match record.lat {
                                Some(value) => value,
                                None => 0.0,
                            },
                            long_: match record.long_ {
                                Some(value) => value,
                                None => 0.0,
                            },
                            confirmed: match record.confirmed {
                                Some(value) => value,
                                None => 0,
                            },
                            deaths: match record.deaths {
                                Some(value) => value,
                                None => 0,
                            },
                            recovered: match record.recovered {
                                Some(value) => value,
                                None => 0,
                            },
                            active: match record.active {
                                Some(value) => value,
                                None => 0,
                            },
                            combined_key: match record.combined_key {
                                Some(value) => value,
                                None => "".to_string(),
                            },
                            incident_rate: match record.incident_rate {
                                Some(value) => value,
                                None => 0.0,
                            },
                            case_fatality_ratio: match record.case_fatality_ratio {
                                Some(value) => value,
                                None => 0.0,
                            },
                        };
                        let _ = inserter.write(&report).await;
                        let _ = inserter.commit().await; // 取消每次的commit，确认是否会提速
                    }
                }
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

mod lib;
use std::{
    env,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Local;
use lib::{init_tbl, put_into_db};
use tokio::{join, sync::mpsc, time::sleep};

#[tokio::main]
async fn main() {
    let format = tracing_subscriber::fmt::format()
        .with_level(true)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false);
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .event_format(format)
        .init();
    // 处理输入参数，获取输入和输出路径
    let args: Vec<String> = env::args().collect();
    let src_dir: &str;
    if args.len() != 2 {
        src_dir = "../../CSSEGISandData/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports";
    } else {
        src_dir = &args[1];
    }
    let future = init_tbl();
    let _ = join!(future);
    let num_cpus = num_cpus::get_physical();
    let thread_counter = Arc::new(Mutex::new(0));
    let (tx, mut rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let mut last_time = Local::now().timestamp_millis();
        let mut sum: i32 = 0;
        while let Some(count) = rx.recv().await {
            sum += count;
            let now = Local::now().timestamp_millis();
            let time: i32 = (now - last_time).try_into().unwrap();
            if time > 1000 * 3 {
                last_time = now;
                log::info!("speed: {}/s", (sum * 1000) / time);
                sum = 0;
                sleep(Duration::from_secs(3)).await;
            }
        }
    });
    let path = Path::new(src_dir);
    for entry in path.read_dir().unwrap() {
        let entry = entry.unwrap();
        let csv_path = entry.path();
        if csv_path.is_file() {
            let file_name = csv_path.to_str().unwrap();
            let (_, file_name) = file_name.rsplit_once("/").unwrap();
            if file_name.ends_with(".csv") {
                let thread_counter = Arc::clone(&thread_counter);
                let tx = tx.clone();
                loop {
                    let mut thread_count = thread_counter.lock().unwrap();
                    if *thread_count < num_cpus {
                        *thread_count += 1;
                        drop(thread_count);
                        tokio::spawn(async move {
                            let future = put_into_db(&csv_path, Arc::clone(&thread_counter), tx);
                            let _ = join!(future);
                        });
                        break;
                    } else {
                        drop(thread_count);
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
    let thread_counter = Arc::clone(&thread_counter);
    loop {
        let thread_count = thread_counter.lock().unwrap();
        if *thread_count > 0 {
            drop(thread_count);
            sleep(Duration::from_millis(1000)).await;
        } else {
            drop(thread_count);
            break;
        }
    }
}

use std::{
    env,
    sync::{Arc, Mutex},
};

use covid_19::{read_buffer_and_input_db, read_file_and_input_buffer};

#[tokio::main(flavor = "current_thread")]
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
    // 原始数据缓存，一行数据
    let line_buf = Arc::new(Mutex::new(Vec::new()));
    // 读取一行，写入缓存
    let future_read_file = read_file_and_input_buffer(src_dir, Arc::clone(&line_buf));
    // 读取缓存，写入CH
    let future_write_db = read_buffer_and_input_db(Arc::clone(&line_buf));
    // join
    tokio::join!(future_read_file, future_write_db);
}

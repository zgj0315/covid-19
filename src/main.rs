use std::{
    env, process,
    sync::{Arc, Mutex},
};

use covid_19::{read_buffer_and_input_db, read_file_and_input_buffer};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // 处理输入参数，获取输入和输出路径
    let args: Vec<String> = env::args().collect();
    let src_dir: &String;
    if args.len() != 2 {
        println!("arguments error!");
        println!("eg: {} /Users/zhaoguangjian/tmp/input", args[0]);
        process::exit(1);
    } else {
        src_dir = &args[1];
    }
    // 原始数据缓存，一行数据
    let line_buf = Arc::new(Mutex::new(Vec::new()));
    // 读取一行，写入缓存
    let future_read_file = read_file_and_input_buffer(src_dir, line_buf.clone());
    // 读取缓存，写入CH
    let future_write_db = read_buffer_and_input_db(line_buf.clone());
    // join
    tokio::join!(future_read_file, future_write_db);
}

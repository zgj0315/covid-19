use std::{
    path::Path,
    sync::{Arc, Mutex},
};

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
            println!("{}", line);
        }
    }
}

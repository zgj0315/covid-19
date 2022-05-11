use std::sync::{Arc, Mutex};

pub async fn read_file_and_input_buffer(src_dir: &String, line_buffer: Arc<Mutex<Vec<String>>>) {
    println!("begin read_file_and_input_buffer");
    println!("src_dir: {}", src_dir);
    for i in 0..100 {
        loop {
            let mut line_list = line_buffer.lock().unwrap();
            println!("buffer lock by read_file_and_input_buffer");
            if line_list.len() < 10 {
                line_list.push(format!("data {}", i));
                println!("put data into buffer");
                drop(line_list);
                println!("buffer free");
                break;
            } else {
                drop(line_list);
                println!("buffer free");
                println!("buffer is full, sleep...");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }
}

pub async fn read_buffer_and_input_db(line_buffer: Arc<Mutex<Vec<String>>>) {
    println!("begin read_buffer_and_input_db");
    let mut sleep_time = 0;
    loop {
        let mut line_list = line_buffer.lock().unwrap();
        println!("buffer lock by read_buffer_and_input_db");
        if line_list.is_empty() {
            drop(line_list);
            println!("buffer free");
            if sleep_time > 30 {
                println!("buffer is empty, job is finished");
                break;
            }
            sleep_time += 1;
            println!("buffer is empty, sleep... sleep_time: {}", sleep_time);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        } else {
            let line = line_list[0].clone();
            line_list.remove(0);
            drop(line_list);
            sleep_time = 0;
            println!("buffer free");
            println!("read a line: {}", line);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}

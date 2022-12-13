use encoding_rs::UTF_8;
use encoding_rs_io::DecodeReaderBytesBuilder;
use std::env;
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use walkdir::WalkDir;

fn main() {
    let args: Vec<String> = env::args().collect();
    let source = &args[1];
    let copyright_path = Path::new(&args[2]);
    let mut copyright_file = File::open(copyright_path).expect("unable to read copyright file");
    let mut copyright = String::new();
    copyright_file.read_to_string(&mut copyright);

    let footer_path = Path::new(&args[3]);
    let mut footer_file = File::open(footer_path).expect("unable to read footer file");
    let mut footer = String::new();
    footer_file.read_to_string(&mut footer);

    let dest = Path::new("Temp");
    for e in WalkDir::new(source).into_iter().filter_map(|e| e.ok()) {
        if e.metadata().unwrap().is_dir() {
            fs::create_dir_all(dest.join(e.path()));
        } else if e.path().to_str().unwrap().to_lowercase().contains(".sql") && !e.path().to_str().unwrap().to_lowercase().contains("initialization data") {
            let mut file = File::open(e.path()).expect("failed to open file");
            let mut contents = String::new();
            let mut reader = BufReader::new(
                DecodeReaderBytesBuilder::new()
                    .utf8_passthru(true)
                    .encoding(Some(UTF_8))
                    .bom_override(true)
                    .build(file)
            );
            for line in reader.lines() {
                contents.push_str(&line.unwrap());
                contents.push_str(&"\n")
            }

            let out_path_str = dest.join(e.path().to_str().unwrap());
            let out_path = Path::new(&out_path_str);
            let mut out_file = File::create(out_path).expect("Couldn't create file at");

            let file_name = e.path().to_str().unwrap();
            if file_name.contains(&"Procedure") {
                let mut is_edited = 0;
                for line in contents.lines() {
                    is_edited = is_edited;
                    if line.to_lowercase().contains("create proc") && is_edited == 0 {
                        out_file.write_all(copyright.as_bytes());
                        out_file.write_all(b"\n");
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n");
                        is_edited += 1;
                    } else if line.to_lowercase().contains("set nocount off")
                        || line.to_lowercase() == "go" && is_edited == 1
                    {
                        out_file.write_all(footer.as_bytes());
                        out_file.write_all(b"\n");
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n ");
                        is_edited += 1;
                    } else {
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n ");
                    }
                }
            } else if file_name.contains(&"Function") {
                let mut is_edited = 0;
                for line in contents.lines() {
                    is_edited = is_edited;
                    if line.to_lowercase() == "as" || line.to_lowercase().contains("returns") {
                        if is_edited == 0 {
                            out_file.write_all(line.as_bytes());
                            out_file.write_all(b"\n");
                            out_file.write_all(copyright.as_bytes());
                            out_file.write_all(b"\n");
                            is_edited += 1;
                        } else {
                            out_file.write_all(line.as_bytes());
                            out_file.write_all(b"\n");
                        }
                    } else if line.to_lowercase() == "go" && is_edited == 1 {
                        out_file.write_all(footer.as_bytes());
                        out_file.write_all(b"\n");
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n");
                        is_edited += 1;
                    } else {
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n ");
                    }
                }
            } else if file_name.contains(&"SYN") {
                let mut is_edited = 0;
                for line in contents.lines() {
                    is_edited = is_edited;
                    if line.to_lowercase().contains(&"create synonym") {
                        if is_edited == 0 {
                            out_file.write_all(line.as_bytes());
                            out_file.write_all(b"\n");
                            out_file.write_all(copyright.as_bytes());
                            out_file.write_all(b"\n");
                            is_edited += 1;
                        }
                    } else {
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n ");
                    }
                }
                out_file.write_all(footer.as_bytes());
            } else if file_name.contains(&"Table") {
                out_file.write_all(copyright.as_bytes());
                out_file.write_all(b"\n");
                for line in contents.lines() {
                    out_file.write_all(line.as_bytes());
                    out_file.write_all(b"\n ");
                }
                out_file.write_all(b"\n");
                out_file.write_all(footer.as_bytes());
            } else if file_name.contains(&"View") {
                let mut is_edited = 0;
                for line in contents.lines() {
                    is_edited = is_edited;
                    if line.to_lowercase().contains(&"create view")
                        || line.to_lowercase().contains(&"create  view")
                    {
                        if is_edited == 0 {
                            out_file.write_all(line.as_bytes());
                            out_file.write_all(b"\n");
                            out_file.write_all(copyright.as_bytes());
                            out_file.write_all(b"\n");
                            is_edited += 1;
                        } else {
                            out_file.write_all(line.as_bytes());
                            out_file.write_all(b"\n ");
                        }
                    } else if line.to_lowercase() == "go" && is_edited == 1 {
                        out_file.write_all(footer.as_bytes());
                        out_file.write_all(b"\n");
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n ");
                        is_edited += 1;
                    } else {
                        out_file.write_all(line.as_bytes());
                        out_file.write_all(b"\n ");
                    }
                }
                out_file.write_all(footer.as_bytes());
            }
        } else {
            fs::copy(e.path(), dest.join(e.path()));
        }
    }
}

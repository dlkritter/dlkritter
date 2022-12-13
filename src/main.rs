use polars::prelude::CsvReader;
use polars::prelude::SerReader;
use polars::frame::DataFrame;
use std::result::Result;
extern crate odbc;
extern crate env_logger;
use odbc::*;
use std::io;
use std::error::Error;
use odbc_safe::AutocommitOn;

fn connect() -> std::result::Result<(), DiagnosticRecord> {

    let env = create_environment_v3().map_err(|e| e.unwrap())?;

    let mut buffer = String::new();
    println!("Please enter connection string: ");
    io::stdin().read_line(&mut buffer).unwrap();

    let conn = env.connect_with_connection_string(&buffer)?;
    insert_encounters(&conn, &enc_df)
}

fn insert_encounters<'env>(conn: &Connection<'env, AutocommitOn>, enc_df: Polars::frame::DataFrame) -> Result<(), Error> {



    let stmt = Statement::with_parent(conn)?.prepare(
        "Insert into fhir.RustTest values (?,?,?",)?;
    
    let stmt = stmt.bind_parameter(1, &enc_df.column("class.code"));
    let stmt = stmt.bind_parameter(2, &enc_df.column("class.system")); 
    let stmt = stmt.bind_parameter(3, &enc_df.column("id"));          


    match stmt.execute()? {
        Data(mut stmt) => {
            let cols = stmt.num_result_cols()?;
            while let Some(mut cursor) = stmt.fetch()? {
                for i in 1..(cols + 1) {
                    match cursor.get_data::<&str>(i as u16)? {
                        Some(val) => print!(" {}", val),
                        None => print!(" NULL"),
                    }
                }
                println!("");
            }
        }
        NoData(_) => println!("Query executed, no data returned"),
    }

    Ok(())
}

fn main() {

    env_logger::init();

    let pt_df = CsvReader::from_path("ptflat.csv").unwrap()
    .infer_schema(None)
    .has_header(true)
    .finish()
    .unwrap();

    let enc_df = CsvReader::from_path("encflat.csv").unwrap()
        .infer_schema(None)
        .has_header(true)
        .finish()
        .unwrap();

        match connect() {
            Ok(()) => println!("Success"),
            Err(diag) => println!("Error: {}", diag),
        };
    
    

    println!("{:?} {:?}{:?}{:?}", enc_df, pt_df, enc_df.schema(), pt_df.schema());

}

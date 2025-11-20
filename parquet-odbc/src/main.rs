use polars_core::prelude::*;
use polars_io::prelude::*;
use polars::*;
use std::fs::File;
use std::fs;
use std::path::PathBuf;
use std::io;
use polars_io::prelude::ParquetCompression::Snappy;
use polars::series::ops::NullBehavior::Ignore;


fn example(u: Vec<PathBuf>) -> std::io::Result<DataFrame> {
    let f = File::open("C:/Parquetdemo/Encounter\\Elwood28_Kilback373_95d1d3e8-4e20-d2d4-d39e-9cb863c31d98_Encounter_2.parquet").unwrap();
    let read = ParquetReader::new(f);
    let mut final_df = read.finish().unwrap();
    let schema = final_df.schema();
    for path in u {
        let r = File::open(&path).unwrap();
        let reader = ParquetReader::new(r);
        let df1 = reader.finish().unwrap();
        if df1.schema() == schema {
            final_df = final_df.vstack(&df1).unwrap()
        } else { final_df = polars::functions::diag_concat_df(&[final_df, df1]).unwrap()}
    }
     Ok(final_df)
}

//fn write_csv(df: &mut DataFrame) -> std::result::Result<()> {
  //  let outfile_name = String::from("target/encounter/exp") + &df.width().to_string() + ".csv";
  //  let outfile = File::create(outfile_name).unwrap();

  //  CsvWriter::new(outfile)
  //  .has_header(true)
  //  .with_delimiter(b',')
  //  .finish(df)
//}


fn main() {

        
    let mut u = Vec::<PathBuf>::new();
    for file in fs::read_dir("C:/Parquetdemo/Encounter").unwrap() {
        u.push(file.unwrap().path())
    }

    let dfs = example(u).unwrap();
    let r = File::open("metadata.parquet").unwrap();
    let read = ParquetReader::new(r);
    let hx_df = read.finish().unwrap();

    
    let df_schema = polars::frame::DataFrame::schema(&dfs);
    let df_shape = polars::frame::DataFrame::shape(&dfs);
    let mut df_meta = polars::frame::DataFrame::new(vec!(dfs.column("Encounter_id").unwrap().to_owned())).unwrap();
    let mut to_import = hx_df.vstack(&df_meta).unwrap();
    let to_import = to_import.drop_duplicates(true, None).unwrap();
    //let file = File::create("metadata.parquet").unwrap();
    //ParquetWriter::new(file).with_compression(Snappy).finish(&mut df_meta).unwrap();


    print!("{:?}, {:?}, {:?}, {:?}, {:?},{:?}", dfs, df_schema, df_shape, df_meta, to_import, hx_df);
}
use crate::model::Bundle_Entry::Bundle_Entry;
use crate::model::Encounter::Encounter;
use crate::model::Patient::Patient;
use crate::ResourceListEnum::ResourcePatient;
use fhir_rs::model::Bundle::Bundle;
use fhir_rs::model::ResourceList::ResourceListEnum;
use fhir_rs::{fhir_parse, model};
use polars::*;
use polars_core::prelude::*;
use polars_io::prelude::*;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Read;
use std::io::Result;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

// This function parses incoming FHIR JSON files according to the FHIR 2022 schema found in root crate
// Creating files and beginning parse

fn select_from_dir(u: &str) -> Result<Vec<PathBuf>> {
    let mut v = Vec::<PathBuf>::new();
    for file in fs::read_dir(u).unwrap() {
        v.push(file.unwrap().path());
    };

    return Ok(v)
}

fn parse() -> Result<()> {
    let mut patientlist =
        BufWriter::new(File::create("PatientList.json").expect("Couldn't create Patient List"));
    let mut encounterlist =
        BufWriter::new(File::create("EncounterList.json").expect("Couldn't create Encounter List"));
    let mut files = select_from_dir("./test/").expect("Unable to read files");
    for file in files {
        let mut data = String::new();
        let mut f = File::open(file).unwrap();
        f.read_to_string(&mut data).expect("Unable to read string");
        if let Some(resource_list) = fhir_parse(&data) {
            match resource_list.resource().unwrap() {
                model::ResourceList::ResourceListEnum::ResourceBundle(bundle) => {
                    let b = Bundle::entry(&bundle).expect("Entry function error");
                    let iter_b = b.iter();
                    for val in iter_b {
                        // break bundle into smaller entries. Write each resource to resource-specific file type, write metadata into another
                        if let Some(resource_list) = Bundle_Entry::resource(&val) {
                            match resource_list
                                .resource()
                                .expect("Couldn't parse bundle entry")
                            {
                                model::ResourceList::ResourceListEnum::ResourcePatient(patient) => {
                                    let pt = patient.to_json();
                                    writeln!(patientlist, "{}", pt).unwrap();
                                }
                                model::ResourceList::ResourceListEnum::ResourceEncounter(
                                    encounter
                                ) => {
                                    let enc = encounter.to_json();
                                    writeln!(encounterlist, "{}", enc).unwrap();
                                
                                }
                                _ => {}
                            };
                        }
                    }
                }
                _ => {}
            };
        };
    }
    return Ok(())
}

fn main() {
    parse();

    let length = Vec::new();
    let paths = Vec::new();

    for line in fs::read("Patientlist.json")
    {
        let frame = polars::read_json(line)
        length.push(frame.count())
    };



    println!("{:?}", length)
}

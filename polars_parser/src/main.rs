use fhir_rs::{fhir_parse, model};
use std::fs::File;
use std::fs;
use fhir_rs::model::ResourceList::ResourceListEnum;
use std::io::Read;
use std::io::Write;
use std::io::BufWriter;
use std::io::BufReader;
use fhir_rs::model::Bundle::Bundle; 
use crate::model::Bundle_Entry::Bundle_Entry;
use crate::model::Patient::Patient;
use fhir_rs::model::Encounter::Encounter;
use crate::ResourceListEnum::ResourcePatient;
use std::io::Result;
use std::path::PathBuf;
use polars_core::prelude::*;
use polars_io::prelude::*;
use polars::prelude::*;
use serde_json::Value;
use serde_json::json;

// This function parses incoming FHIR JSON files according to the FHIR 2022 schema found in root crate
// Creating files and beginning parse

fn select_from_dir (u: &str) -> Result<Vec<PathBuf>> {
  let mut v = Vec::<PathBuf>::new();
  for file in fs::read_dir(u).unwrap() {
      v.push(file.unwrap().path());
  }
  Ok(v)
}


fn parse () -> Result<()> {
  let mut patientlist = BufWriter::new(File::create("PatientList.json").expect("Couldn't create Patient List"));
  let mut encounterlist = BufWriter::new(File::create("EncounterList.json").expect("Couldn't create Encounter List"));
  let mut pt_df = DataFrame::new().unwrap();
  let mut enc_df = DataFrame::new().unwrap();
  let pt_schema = Schema::new(vec![
    Field::new("Patient_address_city",DataType::Utf8, ),
    Field::new("Patient_address_country",DataType::Utf8, ),
    Field::new("Patient_address_extension_extension_url",DataType::Utf8, ),
    Field::new("Patient_address_extension_extension_valueDecimal",DataType::Utf8, ),
    Field::new("Patient_address_extension_url",DataType::Utf8, ),
    Field::new("Patient_address_line",DataType::Utf8, ),
    Field::new("Patient_address_postalCode",DataType::Utf8, ),
    Field::new("Patient_address_state",DataType::Utf8, ),
    Field::new("Patient_birthDate",DataType::Utf8, ),
    Field::new("Patient_communication_language_coding_code",DataType::Utf8, ),
    Field::new("Patient_communication_language_coding_display",DataType::Utf8, ),
    Field::new("Patient_communication_language_coding_system",DataType::Utf8, ),
    Field::new("Patient_communication_language_text",DataType::Utf8, ),
    Field::new("Patient_deceasedDateTime",DataType::Utf8, ),
    Field::new("Patient_extension_extension_url",DataType::Utf8, ),
    Field::new("Patient_extension_extension_valueCoding_code",DataType::Utf8, ),
    Field::new("Patient_extension_extension_valueCoding_display",DataType::Utf8, ),
    Field::new("Patient_extension_extension_valueCoding_system",DataType::Utf8, ),
    Field::new("Patient_extension_extension_valueString",DataType::Utf8, ),
    Field::new("Patient_extension_url",DataType::Utf8, ),
    Field::new("Patient_extension_valueAddress_city",DataType::Utf8, ),
    Field::new("Patient_extension_valueAddress_country",DataType::Utf8, ),
    Field::new("Patient_extension_valueAddress_state",DataType::Utf8, ),
    Field::new("Patient_extension_valueCode",DataType::Utf8, ),
    Field::new("Patient_extension_valueDecimal",DataType::Utf8, ),
    Field::new("Patient_extension_valueString",DataType::Utf8, ),
    Field::new("Patient_gender",DataType::Utf8, ),
    Field::new("Patient_id",DataType::Utf8, ),
    Field::new("Patient_identifier_system",DataType::Utf8, ),
    Field::new("Patient_identifier_type_coding_code",DataType::Utf8, ),
    Field::new("Patient_identifier_type_coding_display",DataType::Utf8, ),
    Field::new("Patient_identifier_type_coding_system",DataType::Utf8, ),
    Field::new("Patient_identifier_type_text",DataType::Utf8, ),
    Field::new("Patient_identifier_value",DataType::Utf8, ),
    Field::new("Patient_maritalStatus_coding_code",DataType::Utf8, ),
    Field::new("Patient_maritalStatus_coding_display",DataType::Utf8, ),
    Field::new("Patient_maritalStatus_coding_system",DataType::Utf8, ),
    Field::new("Patient_maritalStatus_text",DataType::Utf8, ),
    Field::new("Patient_meta_profile",DataType::Utf8, ),
    Field::new("Patient_multipleBirthBoolean",DataType::Utf8, ),
    Field::new("Patient_name_family",DataType::Utf8, ),
    Field::new("Patient_name_given",DataType::Utf8, ),
    Field::new("Patient_name_prefix",DataType::Utf8, ),
    Field::new("Patient_name_use",DataType::Utf8, ),
    Field::new("Patient_resourceType",DataType::Utf8, ),
    Field::new("Patient_telecom_system",DataType::Utf8, ),
    Field::new("Patient_telecom_use",DataType::Utf8, ),
    Field::new("Patient_telecom_value",DataType::Utf8, ),
    Field::new("Patient_text_div",DataType::Utf8, ),
    Field::new("Patient_text_status",DataType::Utf8, ),
  ]);
  let enc_schema = Schema::new(vec![
    Field::new("class.code" DataType::Utf8, ),
    Field::new("class.system" DataType::Utf8, ),
    Field::new("id" DataType::Utf8, ),
    Field::new("identifier.0.system" DataType::Utf8, ),
    Field::new("identifier.0.use" DataType::Utf8, ),
    Field::new("identifier.0.value" DataType::Utf8, ),
    Field::new("location.0.location.display" DataType::Utf8, ),
    Field::new("location.0.location.reference" DataType::Utf8, ),
    Field::new("meta.profile.0" DataType::Utf8, ),
    Field::new("participant.0.individual.display" DataType::Utf8, ),
    Field::new("participant.0.individual.reference" DataType::Utf8, ),
    Field::new("participant.0.period.end" DataType::Utf8, ),
    Field::new("participant.0.period.start" DataType::Utf8, ),
    Field::new("participant.0.type.0.coding.0.code" DataType::Utf8, ),
    Field::new("participant.0.type.0.coding.0.display" DataType::Utf8, ),
    Field::new("participant.0.type.0.coding.0.system" DataType::Utf8, ),
    Field::new("participant.0.type.0.text" DataType::Utf8, ),
    Field::new("period.end" DataType::Utf8, ),
    Field::new("period.start" DataType::Utf8, ),
    Field::new("resourceType" DataType::Utf8, ),
    Field::new("serviceProvider.display" DataType::Utf8, ),
    Field::new("serviceProvider.reference" DataType::Utf8, ),
    Field::new("status" DataType::Utf8, ),
    Field::new("subject.display" DataType::Utf8, ),
    Field::new("subject.reference" DataType::Utf8, ),
    Field::new("type.0.coding.0.code" DataType::Utf8, ),
    Field::new("type.0.coding.0.display" DataType::Utf8, ),
    Field::new("type.0.coding.0.system" DataType::Utf8, ),
    Field::new("type.0.text," DataType::Utf8, )
  ]);

  write!(encounterlist, "{}", String::from(r#"{"Encounters": ["#));
  let mut f  = select_from_dir("./test/").expect("Unable to read files");
  for file in f.iter() {
      let mut f = File::open(file).unwrap();
      let mut data = String::new();
      f.read_to_string(&mut data).expect("Unable to read string");
           if let Some(resource_list) = fhir_parse(&data) {
               match resource_list.resource().unwrap() {
                  model::ResourceList::ResourceListEnum::ResourceBundle(bundle) => {
                      let b = Bundle::entry(&bundle).expect("Entry function error");
                      let iter_b = b.iter();
                      for val in iter_b {
            // break bundle into smaller entries. Write each resource to resource-specific file type, write metadata into another 
                          if let Some(resource_list) = Bundle_Entry::resource(&val) {
                          match resource_list.resource().expect("Couldn't parse bundle entry") {
                               model::ResourceList::ResourceListEnum::ResourcePatient(patient) => {
                                 let pt = DataFrame::new(patient.to_json().to_string()).unwrap();
                                 pt_df.vstack(&pt);
                                 //write!(patientlist, "{:?},", pt.to_string());
                               }
                               model::ResourceList::ResourceListEnum::ResourceEncounter(encounter) => {
                                 let enc = DataFrame::new(encounter.to_json().to_string()).unwrap();
                                 enc_df.vstack(&enc);
                                // write!(encounterlist, "{:?},", enc.to_string());
                               }
                               _ => {}
                             }
                             
                          }
                       }

                    } 
                  _ => {}
            }
         }
    }  
  let mut encounter_str = String::new();
  let mut f = File::open("EncounterList.json").unwrap();
  f.read_to_string(&mut encounter_str);
  json!(&mut encounter_str);    
  let mut pt_str = String::new();
  let mut g = File::open("PatientList.json").unwrap();
  g.read_to_string(&mut pt_str);
  json!(&pt_str);
  
  let pt_df = DataFrame::new(enc_vec).unwrap();
  let enc_df = DataFrame::new(pt_vec).unwrap();
  println!("{:?}", pt_str.len());
  println!("{:?}", encounter_str.len());
  writeln!(patientlist, "{}", pt_str);
  writeln!(encounterlist, "{}", encounter_str);
  println!("{:?}, {:?}", enc_vec, pt_vec);
  println!("{:?}, {:?}, {:?}, {:?}", pt_df, pt_df.shape(), enc_df, enc_df.shape());
  Ok(())       
}

fn main() {
  parse();


}
// Nested Data Structure

use arrow::record_batch::RecordBatch;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet_common::prelude::*;
use std::error::Error;
use std::fs::File;

fn create_data() -> Vec<Contact> {
    vec![
        ContactBuilder::default()
            .with_name("Alice")
            .with_phone(
                PhoneBuilder::default()
                    .with_number("555-1234")
                    .with_phone_type(PhoneType::Home)
                    .build(),
            )
            .with_phone(
                PhoneBuilder::default()
                    .with_number("555-5678")
                    .with_phone_type(PhoneType::Work)
                    .build(),
            )
            .build(),
        ContactBuilder::default().with_name("Bob").build(),
        ContactBuilder::default()
            .with_name("Charlie")
            .with_phones(Vec::<Phone>::new())
            .build(),
        ContactBuilder::default()
            .with_name("Diana")
            .with_phones(vec![
                PhoneBuilder::default()
                    .with_number("555-9999")
                    .with_phone_type(PhoneType::Work)
                    .build(),
            ])
            .build(),
        ContactBuilder::default()
            .with_phones(vec![
                PhoneBuilder::default()
                    .with_phone_type(PhoneType::Home)
                    .build(),
            ])
            .build(),
    ]
}

fn write_parquet(file_path: &str, record_batch: RecordBatch) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None)?;
    writer.write(&record_batch)?;
    writer.close()?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let contacts = create_data();
    info!("Created {} contacts.", contacts.len());

    let schema = get_contact_schema();
    info!("Created Arrow schema definition: {schema}");

    let record_batch = create_record_batch(schema, &contacts)?;
    info!("Created RecordBatch: {record_batch:?}");

    let file_path = "contacts.parquet";
    write_parquet(file_path, record_batch)?;
    info!("Created parquet here: {file_path}");

    info!("Fin.");
    Ok(())
}

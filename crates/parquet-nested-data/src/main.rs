// Nested Data Structure

use arrow::array::{ArrayRef, ListBuilder, StringBuilder, StructBuilder};
use arrow::datatypes;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datatypes::SchemaRef;
use log::info;
use parquet::arrow::ArrowWriter;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
enum PhoneType {
    Home,
    Work,
}

impl PhoneType {
    fn as_str(&self) -> &str {
        match self {
            PhoneType::Home => "Home",
            PhoneType::Work => "Work",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct Phone {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

#[derive(Default)]
struct PhoneBuilder {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

impl PhoneBuilder {
    fn with_number(mut self, number: impl Into<String>) -> Self {
        self.number = Some(number.into());
        self
    }

    fn with_phone_type(mut self, phone_type: PhoneType) -> Self {
        self.phone_type = Some(phone_type);
        self
    }

    fn build(self) -> Phone {
        Phone {
            number: self.number,
            phone_type: self.phone_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct Contact {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}

impl Contact {
    fn new(name: Option<String>, phones: Option<Vec<Phone>>) -> Contact {
        Self { name, phones }
    }
}

#[derive(Default)]
struct ContactBuilder {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}

impl ContactBuilder {
    fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    fn with_phone(mut self, phone: Phone) -> Self {
        self.phones.get_or_insert_with(Vec::new).push(phone);
        self
    }

    fn with_phones<I>(mut self, phones: I) -> Self
    where
        I: IntoIterator<Item = Phone>,
    {
        self.phones.get_or_insert_with(Vec::new).extend(phones);
        self
    }

    fn build(self) -> Contact {
        Contact::new(self.name, self.phones)
    }
}

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

fn create_arrow_schema() -> SchemaRef {
    let phone_fields = vec![
        Field::new("number", DataType::Utf8, true),
        Field::new("phone_type", DataType::Utf8, true),
    ];
    let phone_struct = DataType::Struct(phone_fields.into());

    let phones_list_field = Field::new("item", phone_struct, true);

    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("phones", DataType::List(Arc::new(phones_list_field)), true),
    ]))
}

fn create_record_batch(
    schema: SchemaRef,
    contacts: &[Contact],
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut name_builder = StringBuilder::new();

    let phone_number_builder = StringBuilder::new();
    let phone_type_builder = StringBuilder::new();
    let phone_fields = vec![
        Field::new("number", DataType::Utf8, true),
        Field::new("phone_type", DataType::Utf8, true),
    ];
    let phone_struct_builder = StructBuilder::new(
        phone_fields,
        vec![Box::new(phone_number_builder), Box::new(phone_type_builder)],
    );

    let mut phones_list_builder = ListBuilder::new(phone_struct_builder);

    for contact in contacts {
        name_builder.append_option(contact.name.as_deref());

        match &contact.phones {
            None => phones_list_builder.append_null(),
            Some(phone_list) => {
                let struct_builder = phones_list_builder.values();

                for phone in phone_list {
                    struct_builder.append(true);

                    let number_builder = struct_builder.field_builder::<StringBuilder>(0).unwrap();
                    number_builder.append_option(phone.number.as_deref());

                    let type_builder = struct_builder.field_builder::<StringBuilder>(1).unwrap();
                    let phone_type_str = phone.phone_type.as_ref().map(|pt| pt.as_str());
                    type_builder.append_option(phone_type_str);
                }

                phones_list_builder.append(true);
            }
        }
    }

    let name_array = Arc::new(name_builder.finish());
    let phones_array = Arc::new(phones_list_builder.finish());

    RecordBatch::try_new(
        schema,
        vec![name_array as ArrayRef, phones_array as ArrayRef],
    )
    .map_err(Into::into)
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

    let schema = create_arrow_schema();
    info!("Created Arrow schema definition: {}", schema);

    let record_batch = create_record_batch(schema, &contacts)?;
    info!("Created RecordBatch: {:?}", record_batch);

    let file_path = "contacts.parquet";
    write_parquet(file_path, record_batch)?;
    info!("Created parquet here: {}", file_path);

    info!("Fin.");
    Ok(())
}

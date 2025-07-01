use super::Contact;
use arrow::array::{
    ListBuilder, RecordBatch, StringBuilder, StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt8Type};
use std::error::Error;
use std::sync::Arc;

pub fn get_contact_phone_fields() -> Vec<Arc<Field>> {
    vec![
        Arc::from(Field::new("number", DataType::Utf8, true)),
        Arc::from(Field::new(
            "phone_type",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        )),
    ]
}

fn build_contact_fields() -> Vec<Arc<Field>> {
    let phone_struct = DataType::Struct(get_contact_phone_fields().into());
    let phones_list_field = Field::new("item", phone_struct, true);

    vec![
        Arc::from(Field::new("name", DataType::Utf8, true)),
        Arc::from(Field::new(
            "phones",
            DataType::List(Arc::new(phones_list_field)),
            true,
        )),
    ]
}

pub fn get_contact_schema() -> SchemaRef {
    Arc::new(Schema::new(build_contact_fields()))
}

pub const PHONE_NUMBER_FIELD_INDEX: usize = 0;
pub const PHONE_TYPE_FIELD_INDEX: usize = 1;

pub fn create_record_batch(
    schema: SchemaRef,
    contacts: &[Contact],
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut name_builder = StringBuilder::new();

    let phone_number_builder = StringBuilder::new();
    let phone_type_builder = StringDictionaryBuilder::<UInt8Type>::new();
    let phone_struct_builder = StructBuilder::new(
        get_contact_phone_fields(),
        vec![Box::new(phone_number_builder), Box::new(phone_type_builder)],
    );

    let mut phones_list_builder = ListBuilder::new(phone_struct_builder);

    for contact in contacts {
        name_builder.append_option(contact.name());

        if let Some(phones) = contact.phones() {
            let struct_builder = phones_list_builder.values();

            for phone in phones {
                struct_builder.append(true);

                // Here unwrap() is safe because it matches the index of `number` and `phone_type`
                // fields which we get from `get_phone_fields()`.
                struct_builder
                    .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                    .unwrap()
                    .append_option(phone.number());
                struct_builder
                    .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                    .unwrap()
                    .append_option(phone.phone_type().map(AsRef::as_ref));
            }

            phones_list_builder.append(true);
        } else {
            phones_list_builder.append_null();
        }
    }

    let name_array = Arc::new(name_builder.finish());
    let phones_array = Arc::new(phones_list_builder.finish());

    RecordBatch::try_new(schema, vec![name_array, phones_array]).map_err(Into::into)
}

pub mod contact;

pub mod prelude {
    pub use super::contact::Contact;
    pub use super::contact::ContactBuilder;
    pub use super::contact::Phone;
    pub use super::contact::PhoneBuilder;
    pub use super::contact::PhoneType;
    pub use super::contact::arrow::PHONE_NUMBER_FIELD_INDEX;
    pub use super::contact::arrow::PHONE_TYPE_FIELD_INDEX;
    pub use super::contact::arrow::create_record_batch;
    pub use super::contact::arrow::get_contact_phone_fields;
    pub use super::contact::arrow::get_contact_schema;
}

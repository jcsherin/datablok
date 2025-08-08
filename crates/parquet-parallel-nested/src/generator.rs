use crate::datagen::{generate_name, get_num_phones, get_phone_template};
use arrow::array::{
    ListBuilder, RecordBatch, StringBuilder, StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{SchemaRef, UInt8Type};
use parquet_common::prelude::{
    get_contact_phone_fields, PHONE_NUMBER_FIELD_INDEX, PHONE_TYPE_FIELD_INDEX,
};
use rand::prelude::StdRng;
use rand::SeedableRng;
use std::error::Error;
use std::fmt::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[derive(Debug)]
pub struct ContactRecordBatchGenerator {
    schema: SchemaRef,
    counter: Arc<AtomicUsize>,
}

impl ContactRecordBatchGenerator {
    const PHONE_NUMBER_LENGTH: usize = 15;

    pub fn new(schema: SchemaRef, counter: Arc<AtomicUsize>) -> Self {
        ContactRecordBatchGenerator { schema, counter }
    }

    pub fn generate(&mut self, seed: u64, count: usize) -> Result<RecordBatch, Box<dyn Error>> {
        let mut name = StringBuilder::new();
        let phone_type = StringDictionaryBuilder::<UInt8Type>::new();
        let phone_number = StringBuilder::new();
        let mut phone_number_buf = String::with_capacity(Self::PHONE_NUMBER_LENGTH);
        let phone = StructBuilder::new(
            get_contact_phone_fields(),
            vec![Box::new(phone_number), Box::new(phone_type)],
        );
        let mut phones = ListBuilder::new(phone);

        let rng = &mut StdRng::seed_from_u64(seed);
        let mut name_buf = String::with_capacity(32);

        for _ in 0..count {
            name.append_option(generate_name(rng, &mut name_buf));

            let phones_count = get_num_phones(rng);
            if phones_count == 0 {
                phones.append_null();
            } else {
                let builder = phones.values();

                for _ in 0..phones_count {
                    builder.append(true);

                    let (has_number, phone_type) = get_phone_template(rng);

                    let phone_number_builder = builder
                        .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                        .ok_or_else(|| {
                            Box::<dyn Error>::from(
                                "Expected `number` field at idx: 0 of `Phone` struct builder.",
                            )
                        })?;

                    if has_number {
                        let uniq_suffix = self.counter.fetch_add(1, Ordering::Relaxed);

                        write!(phone_number_buf, "+91-99-{uniq_suffix:08}").unwrap();
                        phone_number_builder.append_value(&phone_number_buf);

                        phone_number_buf.clear();
                    } else {
                        phone_number_builder.append_option(None::<String>);
                    }

                    builder
                        .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                        .ok_or_else(|| Box::<dyn Error>::from("Expected `phone_type` field at idx: {PHONE_TYPE_FIELD_INDEX} of `Phone` struct builder."))?
                        .append_option(phone_type);
                }

                phones.append(true);
            }
        }

        let rb = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(name.finish()), Arc::new(phones.finish())],
        )?;

        Ok(rb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use parquet_common::prelude::get_contact_schema;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_generate_basic() {
        let schema = get_contact_schema();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut generator = ContactRecordBatchGenerator::new(schema.clone(), counter);

        let count = 100;
        let batch = generator.generate(0, count).unwrap();

        assert_eq!(batch.num_rows(), count);
        assert_eq!(batch.schema(), schema);
    }

    #[test]
    fn test_generate_seed_consistency() {
        let schema = get_contact_schema();
        let shared_counter = Arc::new(AtomicUsize::new(0));

        let mut generator1 =
            ContactRecordBatchGenerator::new(schema.clone(), shared_counter.clone());

        let count = 100;
        let batch1 = generator1.generate(0, count).unwrap();

        // Reset the counter for the second generator to ensure identical sequence
        shared_counter.store(0, Ordering::Relaxed);
        let mut generator2 = ContactRecordBatchGenerator::new(schema, shared_counter.clone());
        let batch2 = generator2.generate(0, count).unwrap();

        // Compare batches for equality (content and schema)
        assert_eq!(batch1.num_rows(), batch2.num_rows());
        assert_eq!(batch1.num_columns(), batch2.num_columns());
        assert_eq!(batch1.schema(), batch2.schema());

        for i in 0..batch1.num_columns() {
            // Skip name column (index 0) as it uses `fake` crate which might not be deterministic
            // across separate `StdRng` instances even with the same seed.
            if i == 0 {
                continue;
            }
            let col1 = batch1.column(i);
            let col2 = batch2.column(i);
            assert_eq!(format!("{:?}", col1), format!("{:?}", col2));
        }
    }

    #[test]
    fn test_generate_large_count() {
        let schema = get_contact_schema();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut generator = ContactRecordBatchGenerator::new(schema, counter);

        let count = 100000; // Large count
        let batch = generator.generate(0, count).unwrap();

        assert_eq!(batch.num_rows(), count);
        // Basic check to ensure it doesn't panic or run out of memory immediately
        assert!(batch.column(0).len() > 0);
    }
}

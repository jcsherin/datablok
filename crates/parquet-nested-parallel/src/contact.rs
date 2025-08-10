use crate::skew::{generate_name, generate_phone_template, generate_phones_count};
use arrow::array::{
    ListBuilder, RecordBatch, StringBuilder, StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{SchemaRef, UInt8Type};
use parquet_nested_common::prelude::{
    PHONE_NUMBER_FIELD_INDEX, PHONE_TYPE_FIELD_INDEX, get_contact_phone_fields,
};
use rand::SeedableRng;
use rand::prelude::StdRng;
use std::error::Error;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

            let phones_count = generate_phones_count(rng);
            if phones_count == 0 {
                phones.append_null();
            } else {
                let builder = phones.values();

                for _ in 0..phones_count {
                    builder.append(true);

                    let (has_number, phone_type) = generate_phone_template(rng);

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
    use arrow::array::{Array, ListArray, StringArray};
    use arrow::datatypes::DataType;
    use parquet_nested_common::prelude::get_contact_schema;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_generator_basic() {
        let schema = get_contact_schema();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut generator = ContactRecordBatchGenerator::new(schema.clone(), counter);

        let count = 100;
        let batch = generator.generate(0, count).unwrap();

        assert_eq!(batch.num_rows(), count);
        assert_eq!(batch.schema(), schema);
    }

    #[test]
    fn test_generator_reproducibility() {
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
            let col1 = batch1.column(i);
            let col2 = batch2.column(i);

            assert_eq!(
                col1.data_type(),
                col2.data_type(),
                "Data types for column {} should be equal",
                i
            );

            // Downcast and compare based on the column's data type
            match col1.data_type() {
                DataType::Utf8 => {
                    let arr1 = col1.as_any().downcast_ref::<StringArray>().unwrap();
                    let arr2 = col2.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(arr1, arr2, "Column {} (Name) should be equal", i);
                }
                DataType::List(_) => {
                    let arr1 = col1.as_any().downcast_ref::<ListArray>().unwrap();
                    let arr2 = col2.as_any().downcast_ref::<ListArray>().unwrap();
                    assert_eq!(arr1, arr2, "Column {} (Phones) should be equal", i);
                }
                other => panic!("Unhandled data type for comparison: {}", other),
            }
        }
    }

    #[test]
    fn test_generator_large_batch() {
        let schema = get_contact_schema();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut generator = ContactRecordBatchGenerator::new(schema, counter);

        let count = 100_000;
        let batch = generator.generate(0, count).unwrap();

        assert_eq!(batch.num_rows(), count);

        // Assert that the null count for the name column is within an expected
        // statistical range. `generate_name` has a 20% chance of returning None.
        let name_col = batch.column(0);
        let null_count = name_col.null_count();

        let expected_null_ratio = 0.2;
        let tolerance = 0.05;
        let lower_bound = (count as f64 * (expected_null_ratio - tolerance)) as usize;
        let upper_bound = (count as f64 * (expected_null_ratio + tolerance)) as usize;

        assert!(
            null_count >= lower_bound && null_count <= upper_bound,
            "Null count {} for name column is outside the expected range [{}, {}]",
            null_count,
            lower_bound,
            upper_bound
        );

        // Assert that the null count for the phones column is within an expected
        // statistical range. `generate_phones_count` has a 40% chance of returning 0,
        // which corresponds to a null entry in the ListArray.
        let phones_col = batch.column(1);
        let phones_null_count = phones_col.null_count();

        let expected_phones_null_ratio = 0.4;
        let phones_lower_bound = (count as f64 * (expected_phones_null_ratio - tolerance)) as usize;
        let phones_upper_bound = (count as f64 * (expected_phones_null_ratio + tolerance)) as usize;

        assert!(
            phones_null_count >= phones_lower_bound && phones_null_count <= phones_upper_bound,
            "Null count {} for phones column is outside the expected range [{}, {}]",
            phones_null_count,
            phones_lower_bound,
            phones_upper_bound
        );
    }

    #[test]
    fn test_generator_empty_batch() {
        let schema = get_contact_schema();
        let counter = Arc::new(AtomicUsize::new(0));
        let mut generator = ContactRecordBatchGenerator::new(schema.clone(), counter);

        let batch = generator.generate(0, 0).unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }
}

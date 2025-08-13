use crate::pipeline::PipelineConfig;
use crate::skew::{generate_name, generate_phone_template, generate_phones_count};
use arrow::array::{
    ListBuilder, RecordBatch, StringBuilder, StringDictionaryBuilder, StructBuilder,
};
use arrow::datatypes::{SchemaRef, UInt8Type};
use parquet_nested_common::prelude::{
    get_contact_phone_fields, PHONE_NUMBER_FIELD_INDEX, PHONE_TYPE_FIELD_INDEX,
};
use rand::prelude::StdRng;
use rand::SeedableRng;
use std::error::Error;
use std::fmt::Write;
use std::sync::Arc;

/// A factory for creating `RecordBatchGenerator` instances.
pub trait RecordBatchGeneratorFactory: Send + Sync {
    type Generator: RecordBatchGenerator + Send;
    /// Create a new generator for a specific batch
    fn create_generator(&self, batch_index: usize) -> Self::Generator;
}

/// A single instance will generate one `RecordBatch`.
pub trait RecordBatchGenerator {
    /// A stateless method which is used for generating a single RecordBatch
    /// using data skew primitives.
    fn generate(&self, count: usize) -> Result<RecordBatch, Box<dyn Error + Send + Sync>>;
}

pub struct ContactGeneratorFactory {
    schema: SchemaRef,
    phone_numbers_per_batch: usize,
}

impl ContactGeneratorFactory {
    pub fn from_config(config: PipelineConfig) -> Self {
        let phone_numbers_per_batch =
            ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND.div_ceil(config.total_batches());

        Self {
            schema: config.arrow_schema().clone(),
            phone_numbers_per_batch,
        }
    }

    pub fn new(schema: SchemaRef, total_batches: usize) -> Self {
        let phone_numbers_per_batch =
            ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND.div_ceil(total_batches);

        Self {
            schema,
            phone_numbers_per_batch,
        }
    }

    pub fn phone_numbers_per_batch(&self) -> usize {
        self.phone_numbers_per_batch
    }
}

impl RecordBatchGeneratorFactory for ContactGeneratorFactory {
    type Generator = ContactRecordBatchGenerator;

    fn create_generator(&self, batch_index: usize) -> Self::Generator {
        let phone_id_offset = self.phone_numbers_per_batch * batch_index;

        ContactRecordBatchGenerator::new(self.schema.clone(), batch_index as u64, phone_id_offset)
    }
}

#[derive(Debug)]
pub struct ContactRecordBatchGenerator {
    schema: SchemaRef,
    seed: u64,
    phone_id_offset: usize,
}

impl ContactRecordBatchGenerator {
    const PHONE_NUMBER_LENGTH: usize = 15; // +91 12345 56789 (15 bytes)
    pub const PHONE_NUMBER_UPPER_BOUND: usize = 100_000_000; // for an 8-digit unique suffix
    pub fn new(schema: SchemaRef, seed: u64, phone_id_offset: usize) -> Self {
        Self {
            schema,
            seed,
            phone_id_offset,
        }
    }
}

impl RecordBatchGenerator for ContactRecordBatchGenerator {
    fn generate(&self, count: usize) -> Result<RecordBatch, Box<dyn Error + Send + Sync>> {
        let mut phone_number_counter = self.phone_id_offset;

        // Invariant: upper bound should not exceed 100M (8 digits)
        assert!(
            phone_number_counter + count < Self::PHONE_NUMBER_UPPER_BOUND,
            "Phone number overflow exceeds {}. (offset={phone_number_counter} + count={count})",
            Self::PHONE_NUMBER_UPPER_BOUND
        );

        let mut name = StringBuilder::new();
        let phone_type = StringDictionaryBuilder::<UInt8Type>::new();
        let phone_number = StringBuilder::new();
        let mut phone_number_buf = String::with_capacity(Self::PHONE_NUMBER_LENGTH);
        let phone = StructBuilder::new(
            get_contact_phone_fields(),
            vec![Box::new(phone_number), Box::new(phone_type)],
        );
        let mut phones = ListBuilder::new(phone);

        let rng = &mut StdRng::seed_from_u64(self.seed);
        let mut name_buf = String::with_capacity(32);

        for _ in 0..count {
            if generate_name(rng, &mut name_buf) {
                name.append_value(&name_buf);
            } else {
                name.append_null();
            }

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
                            Box::<dyn Error + Send + Sync>::from(
                                "Expected `number` field at idx: 0 of `Phone` struct builder.",
                            )
                        })?;

                    if has_number {
                        // Invariant: The phone number we are generating should fit in the 8-digit suffix.
                        //
                        // We know how many `count` of records are to be generated in this function call. But the value
                        // of `phone_count` varies between 0 and 5. So the total number of phone numbers we need to
                        // generate in this run is non-trivial to precompute. Therefore, this is a more direct and
                        // simper way to check it. Rest assured, this does not make the code slower!
                        assert!(
                            phone_number_counter < Self::PHONE_NUMBER_UPPER_BOUND,
                            "Phone number ID exceeded global limit"
                        );

                        write!(phone_number_buf, "+91-99-{phone_number_counter:08}").unwrap();
                        phone_number_counter += 1;

                        phone_number_builder.append_value(&phone_number_buf);

                        phone_number_buf.clear();
                    } else {
                        phone_number_builder.append_option(None::<String>);
                    }

                    builder
                        .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                        .ok_or_else(|| Box::<dyn Error + Send + Sync>::from("Expected `phone_type` field at idx: {PHONE_TYPE_FIELD_INDEX} of `Phone` struct builder."))?
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

    fn setup_test_factory() -> ContactGeneratorFactory {
        let schema = get_contact_schema();

        // self.target_records.div_ceil(self.record_batch_size)
        let target_records = 1_000_000;
        let record_batch_size = 4096;
        let total_batches = target_records / record_batch_size;

        ContactGeneratorFactory::new(schema, total_batches)
    }

    const BATCH_INDEX: usize = 123;
    const BATCH_EMPTY: usize = 0;
    const BATCH_SMALL: usize = 100;
    const BATCH_LARGE: usize = 100_000;

    #[test]
    fn test_generator_properties() {
        let factory = setup_test_factory();

        let batch = factory
            .create_generator(BATCH_INDEX)
            .generate(BATCH_SMALL)
            .unwrap();

        assert_eq!(batch.num_rows(), BATCH_SMALL);
        assert_eq!(batch.schema(), get_contact_schema());
    }

    #[test]
    fn test_generator_is_deterministic() {
        let factory1 = setup_test_factory();
        let batch1 = factory1
            .create_generator(BATCH_INDEX)
            .generate(BATCH_SMALL)
            .unwrap();

        let factory2 = setup_test_factory();
        let batch2 = factory2
            .create_generator(BATCH_INDEX)
            .generate(BATCH_SMALL)
            .unwrap();

        assert_eq!(batch1.num_rows(), BATCH_SMALL);
        assert_eq!(batch1.schema(), get_contact_schema());

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
    fn test_generator_100k_null_distribution() {
        let factory = setup_test_factory();
        let batch = factory
            .create_generator(BATCH_INDEX)
            .generate(BATCH_LARGE)
            .unwrap();

        assert_eq!(batch.num_rows(), BATCH_LARGE);

        // Assert that the null count for the name column is within an expected
        // statistical range. `generate_name` has a 20% chance of returning None.
        let name_col = batch.column(0);
        let null_count = name_col.null_count();

        let expected_null_ratio = 0.2;
        let tolerance = 0.05;
        let lower_bound = (BATCH_LARGE as f64 * (expected_null_ratio - tolerance)) as usize;
        let upper_bound = (BATCH_LARGE as f64 * (expected_null_ratio + tolerance)) as usize;

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
        let phones_lower_bound =
            (BATCH_LARGE as f64 * (expected_phones_null_ratio - tolerance)) as usize;
        let phones_upper_bound =
            (BATCH_LARGE as f64 * (expected_phones_null_ratio + tolerance)) as usize;

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
        let factory = setup_test_factory();

        let batch = factory
            .create_generator(BATCH_INDEX)
            .generate(BATCH_EMPTY)
            .unwrap();

        assert_eq!(batch.num_rows(), BATCH_EMPTY);
        assert_eq!(batch.schema(), get_contact_schema());
    }
}

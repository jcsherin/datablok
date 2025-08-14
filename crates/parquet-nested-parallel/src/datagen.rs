use crate::pipeline::PipelineConfig;
use crate::skew::{
    generate_name, generate_phone_template, generate_phones_count, MAX_PHONES_PER_CONTACT,
};
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

/// An iterator that generates a sequence of unique phone number IDs for a batch.
#[derive(Debug)]
pub struct PhoneNumberIdIterator {
    range: std::ops::Range<u64>,
}

impl PhoneNumberIdIterator {
    /// Creates a new iterator for a given range of phone number IDs.
    pub fn new(range: std::ops::Range<u64>) -> Self {
        Self { range }
    }

    pub fn start_id(&self) -> u64 {
        self.range.start
    }

    pub fn end_id(&self) -> u64 {
        self.range.end
    }
}

impl Iterator for PhoneNumberIdIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next()
    }
}

/// A factory for creating `RecordBatchGenerator` instances.
pub trait RecordBatchGeneratorFactory: Send + Sync {
    type Generator: RecordBatchGenerator + Send;
    /// Create a new generator for a specific batch
    fn create_generator(&self, batch_index: usize) -> Self::Generator;
}

/// A single instance will generate one `RecordBatch`.
pub trait RecordBatchGenerator {
    /// A stateful method which is used for generating a single RecordBatch
    /// using data skew primitives and resets the internal builders for computing
    /// the next RecordBatch.
    fn generate(&mut self, count: usize) -> Result<RecordBatch, Box<dyn Error + Send + Sync>>;
}

pub struct ContactGeneratorFactory {
    schema: SchemaRef,
    records_per_batch: usize,
}

impl ContactGeneratorFactory {
    pub fn from_config(config: &PipelineConfig) -> Self {
        Self {
            schema: config.arrow_schema().clone(),
            records_per_batch: config.record_batch_size(),
        }
    }

    pub fn new(schema: SchemaRef, records_per_batch: usize) -> Self {
        Self {
            schema,
            records_per_batch,
        }
    }
}

impl RecordBatchGeneratorFactory for ContactGeneratorFactory {
    type Generator = ContactRecordBatchGenerator;

    fn create_generator(&self, batch_index: usize) -> Self::Generator {
        let start_id = (batch_index * self.records_per_batch * MAX_PHONES_PER_CONTACT) as u64;
        let end_id = ((batch_index + 1) * self.records_per_batch * MAX_PHONES_PER_CONTACT) as u64;
        let phone_number_iter = PhoneNumberIdIterator::new(start_id..end_id);

        ContactRecordBatchGenerator::new(self.schema.clone(), batch_index as u64, phone_number_iter)
    }
}

#[derive(Debug)]
pub struct ContactRecordBatchGenerator {
    schema: SchemaRef,
    phone_number_iter: PhoneNumberIdIterator,

    name_builder: StringBuilder,
    name_buf: String,

    phones_builder: ListBuilder<StructBuilder>,
    phone_number_buf: String,

    rng: StdRng,
}

impl ContactRecordBatchGenerator {
    const PHONE_NUMBER_PREFIX: &str = "+91-";
    const PHONE_NUMBER_LENGTH: usize = 14; // +91-1234567890
    const NAME_LENGTH: usize = 32;
    // The total number of unique phone numbers supported, currently 10 billion.
    // This serves as an upper limit for the entire pipeline, as each phone number
    // is expected to be unique across the whole dataset.
    pub const PHONE_NUMBER_UPPER_BOUND: u64 = 10_000_000_000;
    pub fn new(schema: SchemaRef, seed: u64, phone_number_iter: PhoneNumberIdIterator) -> Self {
        let name_builder = StringBuilder::new();
        let name_buf = String::with_capacity(Self::NAME_LENGTH);

        let phone_type_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let phone_number_builder = StringBuilder::new();
        let phone_builder = StructBuilder::new(
            get_contact_phone_fields(),
            vec![Box::new(phone_number_builder), Box::new(phone_type_builder)],
        );
        let phones_builder = ListBuilder::new(phone_builder);

        let phone_number_buf = String::with_capacity(Self::PHONE_NUMBER_LENGTH);

        let rng = StdRng::seed_from_u64(seed);

        Self {
            schema,
            phone_number_iter,
            name_builder,
            name_buf,
            phones_builder,
            phone_number_buf,
            rng,
        }
    }
}

impl RecordBatchGenerator for ContactRecordBatchGenerator {
    fn generate(&mut self, count: usize) -> Result<RecordBatch, Box<dyn Error + Send + Sync>> {
        for _ in 0..count {
            if generate_name(&mut self.rng, &mut self.name_buf) {
                self.name_builder.append_value(&self.name_buf);
            } else {
                self.name_builder.append_null();
            }

            let phones_count = generate_phones_count(&mut self.rng);
            if phones_count == 0 {
                self.phones_builder.append_null();
            } else {
                let builder = self.phones_builder.values();

                for _ in 0..phones_count {
                    builder.append(true);

                    let (has_number, phone_type) = generate_phone_template(&mut self.rng);

                    let phone_number_builder = builder
                        .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                        .ok_or_else(|| {
                            Box::<dyn Error + Send + Sync>::from(
                                "Expected `number` field at idx: 0 of `Phone` struct builder.",
                            )
                        })?;

                    if has_number {
                        let phone_id = self.phone_number_iter.next()
                            .expect("Phone number ID iterator exhausted. This indicates an overflow in the allocated range for this batch.");

                        // Invariant: The phone number we are generating should not exceed the global limit.
                        assert!(
                            phone_id < Self::PHONE_NUMBER_UPPER_BOUND,
                            "Phone number ID exceeded global limit of {}",
                            Self::PHONE_NUMBER_UPPER_BOUND
                        );

                        write!(
                            self.phone_number_buf,
                            "{}{phone_id:010}",
                            Self::PHONE_NUMBER_PREFIX
                        )
                        .unwrap();

                        phone_number_builder.append_value(&self.phone_number_buf);

                        self.phone_number_buf.clear();
                    } else {
                        phone_number_builder.append_option(None::<String>);
                    }

                    builder
                        .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                        .ok_or_else(|| Box::<dyn Error + Send + Sync>::from("Expected `phone_type` field at idx: {PHONE_TYPE_FIELD_INDEX} of `Phone` struct builder."))?
                        .append_option(phone_type);
                }

                self.phones_builder.append(true);
            }
        }

        let name_array = self.name_builder.finish();
        let phones_array = self.phones_builder.finish();

        let rb = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(name_array), Arc::new(phones_array)],
        )?;

        Ok(rb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skew::MAX_PHONES_PER_CONTACT;
    use arrow::array::{Array, ListArray, StringArray, StructArray};
    use arrow::datatypes::DataType;
    use parquet_nested_common::prelude::get_contact_schema;

    const RECORD_BATCH_SIZE: usize = 4096;
    const NAME_COLUMN_INDEX: usize = 0;
    const PHONES_COLUMN_INDEX: usize = 1;

    fn setup_test_factory() -> ContactGeneratorFactory {
        let schema = get_contact_schema();
        ContactGeneratorFactory::new(schema, RECORD_BATCH_SIZE)
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
        let mut batch1_gen = factory1.create_generator(BATCH_INDEX);
        let batch1 = batch1_gen.generate(BATCH_SMALL).unwrap();

        let factory2 = setup_test_factory();
        let mut batch2_gen = factory2.create_generator(BATCH_INDEX);
        let batch2 = batch2_gen.generate(BATCH_SMALL).unwrap();

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
        // For this test, we need a factory configured to handle a large batch size.
        let factory = ContactGeneratorFactory::new(get_contact_schema(), BATCH_LARGE);
        let batch = factory
            .create_generator(BATCH_INDEX)
            .generate(BATCH_LARGE)
            .unwrap();

        assert_eq!(batch.num_rows(), BATCH_LARGE);

        // Assert that the null count for the name column is within an expected
        // statistical range. `generate_name` has a 20% chance of returning None.
        let name_col = batch.column(NAME_COLUMN_INDEX);
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
        let phones_col = batch.column(PHONES_COLUMN_INDEX);
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

    fn get_phone_numbers(batch: &RecordBatch) -> Vec<u64> {
        let phones_col = batch
            .column(PHONES_COLUMN_INDEX)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let mut numbers = Vec::new();

        for i in 0..phones_col.len() {
            if phones_col.is_valid(i) {
                let list = phones_col.value(i);
                let struct_array = list.as_any().downcast_ref::<StructArray>().unwrap();
                let number_col = struct_array
                    .column(PHONE_NUMBER_FIELD_INDEX)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                for j in 0..number_col.len() {
                    if number_col.is_valid(j) {
                        let phone_str = number_col.value(j);

                        // Assert format
                        assert_eq!(
                            phone_str.len(),
                            ContactRecordBatchGenerator::PHONE_NUMBER_LENGTH
                        );
                        assert!(
                            phone_str.starts_with(ContactRecordBatchGenerator::PHONE_NUMBER_PREFIX)
                        );

                        // Parse number
                        let num_part =
                            &phone_str[ContactRecordBatchGenerator::PHONE_NUMBER_PREFIX.len()..];
                        numbers.push(num_part.parse().unwrap());
                    }
                }
            }
        }
        numbers
    }

    #[test]
    fn test_phone_id_offset_calculation() {
        let factory = setup_test_factory();
        let range_size = (RECORD_BATCH_SIZE * MAX_PHONES_PER_CONTACT) as u64;

        for &batch_index in &[0, 1, 123] {
            let mut gen = factory.create_generator(batch_index);
            let expected_start = batch_index as u64 * range_size;
            let expected_end = expected_start + range_size;

            assert_eq!(
                gen.phone_number_iter.start_id(),
                expected_start,
                "Incorrect start offset for batch {}",
                batch_index
            );
            assert_eq!(
                gen.phone_number_iter.end_id(),
                expected_end,
                "Incorrect end offset for batch {}",
                batch_index
            );
            assert_eq!(
                gen.phone_number_iter.end_id() - gen.phone_number_iter.start_id(),
                range_size,
                "Incorrect range size for batch {}",
                batch_index
            );
            assert_eq!(
                gen.phone_number_iter.next().unwrap(),
                expected_start,
                "Iterator did not start at the expected offset for batch {}",
                batch_index
            );
        }
    }

    #[test]
    fn test_phone_number_sequence_within_batch() {
        let factory = setup_test_factory();
        let mut generator = factory.create_generator(BATCH_INDEX);
        let batch = generator.generate(RECORD_BATCH_SIZE).unwrap();

        let numbers = get_phone_numbers(&batch);

        let start_id = (BATCH_INDEX * RECORD_BATCH_SIZE * MAX_PHONES_PER_CONTACT) as u64;

        for (i, &num) in numbers.iter().enumerate() {
            assert_eq!(
                num,
                start_id + i as u64,
                "Phone number sequence is broken at index {}",
                i
            );
        }
    }

    #[test]
    fn test_phone_number_sequence_across_batches() {
        let factory = setup_test_factory();

        // Batch N
        let mut generator1 = factory.create_generator(BATCH_INDEX);
        let batch1 = generator1.generate(RECORD_BATCH_SIZE).unwrap();
        let numbers1 = get_phone_numbers(&batch1);
        let last_num_in_batch1 = numbers1.last().cloned();

        assert!(
            last_num_in_batch1.is_some(),
            "First batch generated no phone numbers"
        );

        // Check that the generated numbers are a continuous block starting from the correct offset.
        let start_id_1 = (BATCH_INDEX * RECORD_BATCH_SIZE * MAX_PHONES_PER_CONTACT) as u64;
        assert_eq!(
            numbers1[0], start_id_1,
            "First number in batch is not at the starting offset"
        );
        assert_eq!(
            last_num_in_batch1.unwrap(),
            start_id_1 + numbers1.len() as u64 - 1,
            "The sequence of generated numbers in the first batch is not continuous"
        );

        // Batch N+1
        let mut generator2 = factory.create_generator(BATCH_INDEX + 1);
        let batch2 = generator2.generate(RECORD_BATCH_SIZE).unwrap();
        let numbers2 = get_phone_numbers(&batch2);
        let first_num_in_batch2 = numbers2.first().cloned();

        assert!(
            first_num_in_batch2.is_some(),
            "Second batch generated no phone numbers"
        );

        // Check that the second batch starts exactly at the beginning of its allocated range.
        let start_id_2 = ((BATCH_INDEX + 1) * RECORD_BATCH_SIZE * MAX_PHONES_PER_CONTACT) as u64;
        assert_eq!(
            first_num_in_batch2.unwrap(),
            start_id_2,
            "Second batch does not start at the correct offset, indicating a range calculation error"
        );
    }
}

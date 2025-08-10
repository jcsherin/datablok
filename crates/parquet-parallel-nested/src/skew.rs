//! # Skewed Data Generation
//!
//! This module provides functions for generating skewed, Zipfian-like data distributions.
//! The goal is to create datasets that mimic real-world scenarios where a few values
//! occur with high frequency, while most other values are rare (a long-tail distribution).
//! This is useful for simulating realistic workloads.

use fake::Fake;
use fake::faker::name::en::{FirstName, LastName};
use parquet_common::contact::PhoneType;
use rand::Rng;
use rand::distr::Distribution;
use rand::distr::weighted::WeightedIndex;
use std::fmt::Write;

/// Probability distribution for generating `PhoneType` enum variants.
///
/// The distribution is defined by the following weights:
/// - 55%: Mobile
/// - 35%: Work
/// - 10%: Home
const PHONE_TYPE_WEIGHTS: [u32; 3] = [55, 35, 10];

/// Generates a random `PhoneType` based on the `PHONE_TYPE_WEIGHTS` distribution.
fn generate_phone_type(rng: &mut impl Rng) -> PhoneType {
    let dist = WeightedIndex::new(PHONE_TYPE_WEIGHTS).unwrap();
    match dist.sample(rng) {
        0 => PhoneType::Mobile,
        1 => PhoneType::Work,
        2 => PhoneType::Home,
        _ => unreachable!(
            "WeightedIndex should only return indices within the bounds of PHONE_TYPE_WEIGHTS"
        ),
    }
}

/// Probability distribution for generating the number of phones.
///
/// The distribution is defined by the following weights:
/// - 40%: 0 phones
/// - 45%: 1 phone
/// - 10%: 2 phones
/// -  5%: 3-5 phones
const PHONES_COUNT_WEIGHTS: [u32; 4] = [40, 45, 10, 5];

/// The range for the rare case where a contact has a high number of phones.
const HIGH_PHONE_COUNT_RANGE: std::ops::RangeInclusive<i32> = 3..=5;

/// Generates a skewed number of phones (from 0 to 5) based on the `PHONES_COUNT_WEIGHTS` distribution.
pub fn generate_phones_count(rng: &mut impl Rng) -> i32 {
    let dist = WeightedIndex::new(PHONES_COUNT_WEIGHTS).unwrap();
    match dist.sample(rng) {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => rng.random_range(HIGH_PHONE_COUNT_RANGE),
        _ => unreachable!(
            "WeightedIndex should only return indices within the bounds of PHONES_COUNT_WEIGHTS"
        ),
    }
}

/// Probability distribution for generating a phone template.
///
/// A phone template is a tuple `(bool, Option<PhoneType>)`. It handles all the permutations of
/// whether a phone number is present, and the `PhoneType` value.
///
/// The distribution is defined by the following weights:
/// - 90%: `(true, Some(PhoneType))`
/// -  5%: `(true, None)`
/// -  4%: `(false, Some(PhoneType))`
/// -  1%: `(false, None)`
const PHONE_TEMPLATE_WEIGHTS: [u32; 4] = [90, 5, 4, 1];

/// Generates a phone template based on the `PHONE_TEMPLATE_WEIGHTS` distribution.
///
/// A phone template is a tuple `(bool, Option<PhoneType>)` that defines the shape of a
/// phone entry before a concrete phone number is generated. This allows for lazy
/// initialization and gives the caller flexibility in how values are materialized.
///
/// The returned tuple has the following meaning:
/// - The `bool` indicates if a phone number is present or should be generated.
/// - The `Option<PhoneType>` specifies the category of the phone number.
///
/// This allows for all permutations, including representing a placeholder for a phone
/// number that is expected but not yet available. For example, the tuple
/// `(false, Some(PhoneType::Work))` can signify that a work phone is required for a
/// contact but the number itself is currently missing.
pub fn generate_phone_template(rng: &mut impl Rng) -> (bool, Option<PhoneType>) {
    let dist = WeightedIndex::new(PHONE_TEMPLATE_WEIGHTS).unwrap();
    match dist.sample(rng) {
        0 => (true, Some(generate_phone_type(rng))),
        1 => (true, None),
        2 => (false, Some(generate_phone_type(rng))),
        _ => (false, None),
    }
}

/// Generates a fake name with an 80% probability, writing it into the provided buffer.
///
/// This function takes a mutable buffer `name_buf` to avoid repeated memory allocations
/// when generating many names in a loop. The buffer is cleared after each name is
/// generated.
pub fn generate_name(rng: &mut impl Rng, name_buf: &mut String) -> Option<String> {
    if rng.random_bool(0.8) {
        write!(
            name_buf,
            "{} {}",
            FirstName().fake::<&str>(),
            LastName().fake::<&str>(),
        )
        .unwrap();

        let name = Some(name_buf.clone());
        name_buf.clear();

        name
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use std::collections::HashMap;

    /// Defines the acceptable deviation from predefined data skew in the final generated data due
    /// to minor statistical variations between test runs.
    ///
    /// The 5% is small enough to catch larger deviations, and at the same time large enough to
    /// prevent flakiness due to minor statistical variations.
    const TOLERANCE: f64 = 0.05;

    /// Asserts that the distribution of generated values matches the expected weights
    /// within a given tolerance.
    ///
    /// # Arguments
    ///
    /// * `counts`: A `HashMap` containing the counts of generated items.
    /// * `num_iterations`: Total number of items generated.
    /// * `expected_weights`: A slice of `u32` weights for the distribution.
    /// * `tolerance`: The allowed statistical deviation (e.g., 0.05 for 5%).
    /// * `checks`: An array of tuples to check, where each tuple is
    ///   `(value, weight_index, label)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // First, populate a HashMap with the counts of generated items.
    /// // (In a real test, this is done in a loop over the data generator).
    /// let mut counts = HashMap::new();
    /// counts.insert(PhoneType::Mobile, 55000);
    /// counts.insert(PhoneType::Work, 35000);
    /// counts.insert(PhoneType::Home, 10000);
    ///
    /// // Then, assert the distribution.
    /// assert_distribution!(
    ///     counts,
    ///     100000,
    ///     &[55, 35, 10],
    ///     0.05,
    ///     [
    ///         (PhoneType::Mobile, 0, "Mobile"),
    ///         (PhoneType::Work, 1, "Work"),
    ///         (PhoneType::Home, 2, "Home")
    ///     ]
    /// );
    /// ```
    macro_rules! assert_distribution {
        ($counts:expr, $num_iterations:expr, $expected_weights:expr, $tolerance:expr, [$(($value:expr, $weight_index:expr, $label:expr)),+]) => {
            let total_weight: u32 = $expected_weights.iter().sum();
            $(
                let count = *$counts.get(&$value).unwrap_or(&0);
                let actual_percent = count as f64 / $num_iterations as f64;
                let expected_percent = $expected_weights[$weight_index] as f64 / total_weight as f64;
                assert!(
                    (actual_percent - expected_percent).abs() < $tolerance,
                    "{label} distribution: {actual_percent}, expected {expected_percent}",
                    label = $label
                );
            )+
        };
    }

    #[derive(Debug, Eq, PartialEq, Hash)]
    enum PhoneCountCategory {
        Zero,
        One,
        Two,
        ThreeToFive,
    }

    #[test]
    fn test_generate_phone_type_distribution() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut counts = HashMap::new();
        let num_iterations = 100000;

        for _ in 0..num_iterations {
            *counts.entry(generate_phone_type(&mut rng)).or_insert(0) += 1;
        }

        assert_distribution!(
            counts,
            num_iterations,
            &PHONE_TYPE_WEIGHTS,
            TOLERANCE,
            [
                (PhoneType::Mobile, 0, "Mobile"),
                (PhoneType::Work, 1, "Work"),
                (PhoneType::Home, 2, "Home")
            ]
        );
    }

    #[test]
    fn test_generate_phones_count_distribution() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut counts = HashMap::new();
        let num_iterations = 100000;

        for _ in 0..num_iterations {
            let num = generate_phones_count(&mut rng);
            let category = match num {
                0 => PhoneCountCategory::Zero,
                1 => PhoneCountCategory::One,
                2 => PhoneCountCategory::Two,
                3..=5 => PhoneCountCategory::ThreeToFive,
                _ => unreachable!("Generated unexpected number of phones: {num}"),
            };
            *counts.entry(category).or_insert(0) += 1;
        }

        assert_distribution!(
            counts,
            num_iterations,
            &PHONES_COUNT_WEIGHTS,
            TOLERANCE,
            [
                (PhoneCountCategory::Zero, 0, "0 phones"),
                (PhoneCountCategory::One, 1, "1 phone"),
                (PhoneCountCategory::Two, 2, "2 phones"),
                (PhoneCountCategory::ThreeToFive, 3, "3-5 phones")
            ]
        );
    }

    #[derive(Debug, Eq, PartialEq, Hash)]
    enum PhoneTemplateCategory {
        TrueSome,
        TrueNone,
        FalseSome,
        FalseNone,
    }

    #[test]
    fn test_generate_phone_template_distribution() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut counts = HashMap::new();
        let num_iterations = 100000;

        for _ in 0..num_iterations {
            let (has_number, phone_type) = generate_phone_template(&mut rng);
            let category = match (has_number, phone_type) {
                (true, Some(_)) => PhoneTemplateCategory::TrueSome,
                (true, None) => PhoneTemplateCategory::TrueNone,
                (false, Some(_)) => PhoneTemplateCategory::FalseSome,
                (false, None) => PhoneTemplateCategory::FalseNone,
            };
            *counts.entry(category).or_insert(0) += 1;
        }

        assert_distribution!(
            counts,
            num_iterations,
            &PHONE_TEMPLATE_WEIGHTS,
            TOLERANCE,
            [
                (PhoneTemplateCategory::TrueSome, 0, "true, Some"),
                (PhoneTemplateCategory::TrueNone, 1, "true, None"),
                (PhoneTemplateCategory::FalseSome, 2, "false, Some"),
                (PhoneTemplateCategory::FalseNone, 3, "false, None")
            ]
        );
    }

    #[test]
    fn test_generate_name() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut name_buf = String::new();
        let mut counts = HashMap::new();
        let num_iterations = 100000;
        let mut name_validated = false;

        for _ in 0..num_iterations {
            let name = generate_name(&mut rng, &mut name_buf);
            if let Some(n) = &name {
                // As a one-time check, validate the contents of the first name generated.
                if !name_validated {
                    assert!(!n.is_empty());
                    assert!(n.contains(" "));
                    name_validated = true;
                }
            }
            *counts.entry(name.is_some()).or_insert(0) += 1;
        }

        // Ensure that at least one name was generated and validated during the test.
        assert!(
            name_validated,
            "Failed to generate a single valid name in {} iterations.",
            num_iterations
        );

        const NAME_WEIGHTS: [u32; 2] = [80, 20]; // Some, None

        assert_distribution!(
            counts,
            num_iterations,
            &NAME_WEIGHTS,
            TOLERANCE,
            [(true, 0, "Some(Name)"), (false, 1, "None")]
        );
    }
}

use fake::Fake;
use fake::faker::name::en::{FirstName, LastName};
use parquet_common::contact::PhoneType;
use rand::Rng;
use std::fmt::Write;

fn generate_phone_type(rng: &mut impl Rng) -> PhoneType {
    match rng.random_range(0..100) {
        0..=54 => PhoneType::Mobile, // 0.55
        55..=89 => PhoneType::Work,  // 0.35
        _ => PhoneType::Home,        // 0.10
    }
}

pub fn generate_phones_count(rng: &mut impl Rng) -> i32 {
    match rng.random_range(0..100) {
        0..=39 => 0,                  // 0.40
        40..=84 => 1,                 // 0.45
        85..=94 => 2,                 // 0.10
        _ => rng.random_range(3..=5), // 0.05
    }
}

pub fn generate_phone_template(rng: &mut impl Rng) -> (bool, Option<PhoneType>) {
    match rng.random_range(0..100) {
        0..=89 => (true, Some(generate_phone_type(rng))), // 90%
        90..=94 => (true, None),                          //  5%
        95..=98 => (false, Some(generate_phone_type(rng))), //  4%
        _ => (false, None),                               //  1%
    }
}

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

    #[test]
    fn test_generate_phone_type_distribution() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut counts = HashMap::new();
        let num_iterations = 100000;

        for _ in 0..num_iterations {
            *counts.entry(generate_phone_type(&mut rng)).or_insert(0) += 1;
        }

        let mobile_count = *counts.get(&PhoneType::Mobile).unwrap_or(&0);
        let work_count = *counts.get(&PhoneType::Work).unwrap_or(&0);
        let home_count = *counts.get(&PhoneType::Home).unwrap_or(&0);

        let mobile_percent = mobile_count as f64 / num_iterations as f64;
        let work_percent = work_count as f64 / num_iterations as f64;
        let home_percent = home_count as f64 / num_iterations as f64;

        // Allow for a 5% deviation
        assert!(
            mobile_percent > 0.50 && mobile_percent < 0.60,
            "Mobile distribution: {}",
            mobile_percent
        );
        assert!(
            work_percent > 0.30 && work_percent < 0.40,
            "Work distribution: {}",
            work_percent
        );
        assert!(
            home_percent > 0.05 && home_percent < 0.15,
            "Home distribution: {}",
            home_percent
        );
    }

    #[test]
    fn test_get_num_phones_distribution() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut counts = HashMap::new();
        let num_iterations = 100000;

        for _ in 0..num_iterations {
            let num = generate_phones_count(&mut rng);
            *counts.entry(num).or_insert(0) += 1;
        }

        let count_0 = *counts.get(&0).unwrap_or(&0);
        let count_1 = *counts.get(&1).unwrap_or(&0);
        let count_2 = *counts.get(&2).unwrap_or(&0);
        let count_3_5 = counts
            .iter()
            .filter(|&(&k, _)| k >= 3 && k <= 5)
            .map(|(_, &v)| v)
            .sum::<i32>();

        let percent_0 = count_0 as f64 / num_iterations as f64;
        let percent_1 = count_1 as f64 / num_iterations as f64;
        let percent_2 = count_2 as f64 / num_iterations as f64;
        let percent_3_5 = count_3_5 as f64 / num_iterations as f64;

        // Allow for a 5% deviation
        assert!(
            percent_0 > 0.35 && percent_0 < 0.45,
            "0 phones distribution: {}",
            percent_0
        );
        assert!(
            percent_1 > 0.40 && percent_1 < 0.50,
            "1 phone distribution: {}",
            percent_1
        );
        assert!(
            percent_2 > 0.05 && percent_2 < 0.15,
            "2 phones distribution: {}",
            percent_2
        );
        assert!(
            percent_3_5 > 0.00 && percent_3_5 < 0.10,
            "3-5 phones distribution: {}",
            percent_3_5
        );
    }

    #[test]
    fn test_get_phone_template_distribution() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut counts: HashMap<(bool, String), i32> = HashMap::new();
        let num_iterations = 100000;

        for _ in 0..num_iterations {
            let (has_number, phone_type) = generate_phone_template(&mut rng);
            let phone_type_str = match phone_type {
                Some(pt) => format!("{:?}", pt),
                None => "None".to_string(),
            };
            *counts.entry((has_number, phone_type_str)).or_insert(0) += 1;
        }

        let true_some_count = counts
            .iter()
            .filter(|&((hn, pt_str), _)| *hn && pt_str != "None")
            .map(|(_, &v)| v)
            .sum::<i32>();
        let true_none_count = *counts.get(&(true, "None".to_string())).unwrap_or(&0);
        let false_some_count = counts
            .iter()
            .filter(|&((hn, pt_str), _)| !*hn && pt_str != "None")
            .map(|(_, &v)| v)
            .sum::<i32>();
        let false_none_count = *counts.get(&(false, "None".to_string())).unwrap_or(&0);

        let true_some_percent = true_some_count as f64 / num_iterations as f64;
        let true_none_percent = true_none_count as f64 / num_iterations as f64;
        let false_some_percent = false_some_count as f64 / num_iterations as f64;
        let false_none_percent = false_none_count as f64 / num_iterations as f64;

        // Allow for a 5% deviation
        assert!(
            true_some_percent > 0.85 && true_some_percent < 0.95,
            "true, Some distribution: {}",
            true_some_percent
        );
        assert!(
            true_none_percent > 0.00 && true_none_percent < 0.10,
            "true, None distribution: {}",
            true_none_percent
        );
        assert!(
            false_some_percent > 0.00 && false_some_percent < 0.09,
            "false, Some distribution: {}",
            false_some_percent
        );
        assert!(
            false_none_percent > 0.00 && false_none_percent < 0.06,
            "false, None distribution: {}",
            false_none_percent
        );
    }

    #[test]
    fn test_generate_name() {
        let mut name_buf = String::new();

        // Test case where name is generated
        let mut rng = StdRng::seed_from_u64(0);
        let mut generated_name_found = false;
        for _ in 0..1000 {
            // Try up to 1000 times to get a name
            let name = generate_name(&mut rng, &mut name_buf);
            if let Some(n) = name {
                assert!(!n.is_empty());
                assert!(n.contains(" "));
                generated_name_found = true;
                break;
            }
        }
        assert!(
            generated_name_found,
            "Failed to generate a name after 1000 attempts."
        );

        // Test case where name is not generated
        let mut rng = StdRng::seed_from_u64(1); // Use a different seed
        let mut no_name_found = false;
        for _ in 0..1000 {
            // Try up to 1000 times to get no name
            let name = generate_name(&mut rng, &mut name_buf);
            if name.is_none() {
                no_name_found = true;
                break;
            }
        }
        assert!(no_name_found, "Failed to get no name after 1000 attempts.");
    }
}

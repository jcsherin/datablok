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

pub fn get_num_phones(rng: &mut impl Rng) -> i32 {
    match rng.random_range(0..100) {
        0..=39 => 0,                  // 0.40
        40..=84 => 1,                 // 0.45
        85..=94 => 2,                 // 0.10
        _ => rng.random_range(3..=5), // 0.05
    }
}

pub fn get_phone_template(rng: &mut impl Rng) -> (bool, Option<PhoneType>) {
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

use rand::prelude::{IndexedRandom, SliceRandom};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug)]
pub struct TitleGenerator {
    rng: StdRng,
}

impl Default for TitleGenerator {
    fn default() -> Self {
        Self::new(0)
    }
}

impl TitleGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

/// Generates title strings.
///
/// The title include control phrases with a known probability distribution. In a generated dataset
/// when searching for this control phrase, we can know beforehand approximately how many rows will
/// contain this specific phrase.
impl Iterator for TitleGenerator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let mut title_parts: Vec<&str> = Vec::new();

        for _ in 0..self.rng.random_range(3..=8) {
            title_parts.push(
                crate::data_generator::words::FILLER_WORDS
                    .choose(&mut self.rng)
                    .unwrap(),
            );
        }
        for (phrase, selectivity) in crate::data_generator::words::SELECTIVITY_PHRASES {
            if self.rng.random::<f64>() < *selectivity {
                title_parts.push(phrase);
            }
        }
        title_parts.shuffle(&mut self.rng);

        Some(title_parts.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use crate::data_generator::title::TitleGenerator;

    #[test]
    fn test_reproducible_with_seed() {
        let iter1 = TitleGenerator::new(0).take(1000);
        let iter2 = TitleGenerator::new(0).take(1000);

        iter1.zip(iter2).for_each(|(a, b)| assert_eq!(a, b));
    }
}

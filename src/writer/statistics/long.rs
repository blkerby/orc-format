use std::cmp;
use super::common::{BaseStatistics, merge_min, merge_max, merge_sum};

#[derive(Debug, Copy, Clone)]
pub struct LongStatistics {
    pub num_rows: u64,
    pub has_null: bool,
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub sum: Option<i64>,
}

impl LongStatistics {
    pub fn new() -> LongStatistics {
        LongStatistics {
            has_null: false,
            num_rows: 0,
            min: None,
            max: None,
            sum: Some(0),
        }
    }

    pub fn update(&mut self, x: Option<i64>) {
        if let Some(y) = x {
            self.min = match self.min {
                None => x,
                Some(z) => Some(cmp::min(y, z)),
            };
            self.max = match self.max {
                None => x,
                Some(z) => Some(cmp::max(y, z)),
            };
            self.sum = match self.sum {
                None => None,
                Some(z) => y.checked_add(z),
            };
        };
        self.num_rows += 1;
    }
}

impl BaseStatistics for LongStatistics {
    fn num_rows(&self) -> u64 { self.num_rows }

    fn has_null(&self) -> bool { self.has_null }

    fn merge(&mut self, rhs: &Self) {
        self.has_null = self.has_null || rhs.has_null;
        self.num_rows = self.num_rows + rhs.num_rows;
        self.min = merge_min(self.min, rhs.min);
        self.max = merge_max(self.max, rhs.max);
        self.sum = merge_sum(self.sum, rhs.sum);
    }
}

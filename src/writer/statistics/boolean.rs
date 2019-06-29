use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct BooleanStatistics {
    pub num_values: u64,
    pub num_true: u64,
    pub num_false: u64,
}

impl BooleanStatistics {
    pub fn new() -> BooleanStatistics {
        BooleanStatistics {
            num_values: 0,
            num_true: 0,
            num_false: 0,
        }
    }

    pub fn update(&mut self, x: bool) {
        self.num_values += 1;
        if x {
            self.num_true += 1;
        } else {
            self.num_false += 1;
        }
    }
}

impl BaseStatistics for BooleanStatistics {
    fn update_null(&mut self) {
        self.num_values += 1;
    }

    fn num_values(&self) -> u64 { self.num_values }

    fn num_present(&self) -> u64 { self.num_true + self.num_false }

    fn merge(&mut self, rhs: &Self) {
        self.num_values += rhs.num_values;
        self.num_true += rhs.num_true;
        self.num_false += rhs.num_false;
    }
}

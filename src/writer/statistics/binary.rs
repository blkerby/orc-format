use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct BinaryStatistics {
    pub num_values: u64,
    pub num_present: u64,
    pub sum_lengths: u64,
}

impl BinaryStatistics {
    pub fn new() -> Self {
        Self {
            num_values: 0,
            num_present: 0,
            sum_lengths: 0,
        }
    }

    pub fn update(&mut self, x: &[u8]) {
        self.num_values += 1;
        self.num_present += 1;
        self.sum_lengths += x.len() as u64;
    }
}

impl BaseStatistics for BinaryStatistics {
    fn update_null(&mut self) {
        self.num_values += 1;
    }

    fn num_values(&self) -> u64 { self.num_values }

    fn num_present(&self) -> u64 { self.num_present }

    fn merge(&mut self, rhs: &Self) {
        self.num_values += rhs.num_values;
        self.num_present += rhs.num_present;
        self.sum_lengths += rhs.sum_lengths;
    }
}

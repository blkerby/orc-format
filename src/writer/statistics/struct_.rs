use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct StructStatistics {
    pub num_values: u64,
    pub num_present: u64,
}

impl StructStatistics {
    pub fn new() -> StructStatistics {
        StructStatistics {
            num_values: 0,
            num_present: 0,
        }
    }

    pub fn update(&mut self, present: bool) {
        self.num_values += 1;
        self.num_present += present as u64;        
    }
}

impl BaseStatistics for StructStatistics {
    fn num_values(&self) -> u64 { self.num_values }

    fn num_present(&self) -> u64 { self.num_present }

    fn merge(&mut self, rhs: &Self) {
        self.num_values += rhs.num_values;
        self.num_present += rhs.num_present;
    }
}

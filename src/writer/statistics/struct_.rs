use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct StructStatistics {
    pub num_rows: u64,
    pub has_null: bool,
}

impl StructStatistics {
    pub fn new() -> StructStatistics {
        StructStatistics {
            num_rows: 0,
            has_null: false,
        }
    }

    pub fn update(&mut self, present: bool) {
        if !present { 
            self.has_null = true; 
        }
        self.num_rows += 1;
    }
}

impl BaseStatistics for StructStatistics {
    fn num_rows(&self) -> u64 { self.num_rows }
    
    fn has_null(&self) -> bool { self.has_null }

    fn merge(&mut self, rhs: &Self) {
        self.has_null = self.has_null || rhs.has_null;
        self.num_rows = self.num_rows + rhs.num_rows;
    }
}

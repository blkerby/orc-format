use std::cmp;

#[derive(Copy, Clone)]
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

#[derive(Copy, Clone)]
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

#[derive(Copy, Clone)]
pub enum Statistics {
    Long(LongStatistics),
    Struct(StructStatistics),
}

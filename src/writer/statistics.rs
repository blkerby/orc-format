use std::cmp;
use std::ops::Add;

trait BaseStatistics {
    fn num_rows(&self) -> u64;
    fn has_null(&self) -> bool;
    fn merge(&mut self, rhs: &Self);
}

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

fn merge_min<T: Ord>(x: Option<T>, y: Option<T>) -> Option<T> {
    match x {
        None => y,
        Some(a) => match y {
            None => None,
            Some(b) => Some(cmp::min(a, b))
        }
    }
}

fn merge_max<T: Ord>(x: Option<T>, y: Option<T>) -> Option<T> {
    match x {
        None => y,
        Some(a) => match y {
            None => None,
            Some(b) => Some(cmp::max(a, b))
        }
    }
}

fn merge_sum<T: Add<Output=T>>(x: Option<T>, y: Option<T>) -> Option<T> {
    match x {
        None => None,
        Some(a) => match y {
            None => None,
            Some(b) => Some(a + b)
        }
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

#[derive(Debug, Copy, Clone)]
pub enum Statistics {
    Long(LongStatistics),
    Struct(StructStatistics),
}

impl Statistics {
    fn unwrap_long(&self) -> &LongStatistics { 
        if let Statistics::Long(x) = self { x } else { panic!("invalid argument to unwrap_long"); }
    }

    fn unwrap_struct(&self) -> &StructStatistics { 
        if let Statistics::Struct(x) = self { x } else { panic!("invalid argument to unwrap_struct"); }
    }
}

impl BaseStatistics for Statistics {
    fn num_rows(&self) -> u64 {
        match self {
            Statistics::Long(x) => x.num_rows(),
            Statistics::Struct(x) => x.num_rows(),
        }
    }

    fn has_null(&self) -> bool {
        match self {
            Statistics::Long(x) => x.has_null(),
            Statistics::Struct(x) => x.has_null(),
        }
    }

    fn merge(&mut self, rhs: &Statistics) {
        match self {
            Statistics::Long(x) => x.merge(rhs.unwrap_long()),
            Statistics::Struct(x) => x.merge(rhs.unwrap_struct()),
        }
    }
}
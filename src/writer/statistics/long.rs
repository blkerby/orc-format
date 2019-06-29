use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct LongStatistics {
    pub num_values: u64,
    pub num_present: u64,
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub sum: Option<i64>,
}

fn merge_min(x: &mut Option<i64>, y: Option<i64>) {
    if let Some(yv) = y {
        if let Some(xv) = x {
            if yv < *xv {
                *x = y;
            }
        } else {
            *x = y;
        }
    }
}

fn merge_max(x: &mut Option<i64>, y: Option<i64>) {
    if let Some(yv) = y {
        if let Some(xv) = x {
            if yv > *xv {
                *x = y;
            }
        } else {
            *x = y;
        }
    }
}

fn merge_sum(x: &mut Option<i64>, y: Option<i64>) {
    if let Some(yv) = y {
        if let Some(xv) = x {
            *x = xv.checked_add(yv);
        } else {
            *x = None;
        }
    } else {
        *x = None;
    }
}

impl LongStatistics {
    pub fn new() -> LongStatistics {
        LongStatistics {
            num_values: 0,
            num_present: 0,
            min: None,
            max: None,
            sum: Some(0),
        }
    }

    pub fn update(&mut self, x: i64) {
        self.num_values += 1;
        self.num_present += 1;
        merge_min(&mut self.min, Some(x));
        merge_max(&mut self.max, Some(x));
        merge_sum(&mut self.sum, Some(x));
    }
}

impl BaseStatistics for LongStatistics {
    fn update_null(&mut self) {
        self.num_values += 1;
    }

    fn num_values(&self) -> u64 { self.num_values }

    fn num_present(&self) -> u64 { self.num_present }

    fn merge(&mut self, rhs: &Self) {
        self.num_values += rhs.num_values;
        self.num_present += rhs.num_present;
        merge_min(&mut self.min, rhs.min);
        merge_max(&mut self.max, rhs.max);
        merge_sum(&mut self.sum, rhs.sum);        
    }
}

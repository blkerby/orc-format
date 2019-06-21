use std::cmp;
use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct LongStatistics {
    pub num_rows: u64,
    pub has_null: bool,
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
            num_rows: 0,
            has_null: false,
            min: None,
            max: None,
            sum: Some(0),
        }
    }

    pub fn update(&mut self, x: Option<i64>) {
        self.num_rows += 1;
        self.has_null |= x.is_none();
        merge_min(&mut self.min, x);
        merge_max(&mut self.max, x);
        merge_sum(&mut self.sum, x);
    }
}

impl BaseStatistics for LongStatistics {
    fn num_rows(&self) -> u64 { self.num_rows }

    fn has_null(&self) -> bool { self.has_null }

    fn merge(&mut self, rhs: &Self) {
        self.num_rows += rhs.num_rows;
        self.has_null |= rhs.has_null;
        merge_min(&mut self.min, rhs.min);
        merge_max(&mut self.max, rhs.max);
        merge_sum(&mut self.sum, rhs.sum);        
    }
}

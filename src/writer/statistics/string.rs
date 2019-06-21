use std::cmp;
use super::common::BaseStatistics;

#[derive(Debug, Clone)]
pub struct StringStatistics {
    pub num_rows: u64,
    pub has_null: bool,
    pub min: Option<String>,
    pub max: Option<String>,
    pub sum_lengths: u64,
}

fn merge_min(x: &mut Option<String>, y: Option<&str>) {
    if let Some(yv) = y {
        if let Some(xv) = x {
            if yv < xv {
                *x = Some(yv.to_string());
            }
        } else {
            *x = Some(yv.to_string());
        }
    }   
}

fn merge_max(x: &mut Option<String>, y: Option<&str>) {
    if let Some(yv) = y {
        if let Some(xv) = x {
            if yv > xv {
                *x = Some(yv.to_string());
            }
        } else {
            *x = Some(yv.to_string());
        }
    }   
}

impl StringStatistics {
    pub fn new() -> StringStatistics {
        StringStatistics {
            num_rows: 0,
            has_null: false,
            min: None,
            max: None,
            sum_lengths: 0,
        }
    }

    pub fn update(&mut self, x: Option<&str>) {
        self.num_rows += 1;
        self.has_null |= x.is_none();
        merge_min(&mut self.min, x);
        merge_max(&mut self.max, x);
        if let Some(xv) = x {
            self.sum_lengths += xv.len() as u64;
        }   
    }
}

impl BaseStatistics for StringStatistics {
    fn num_rows(&self) -> u64 { self.num_rows }

    fn has_null(&self) -> bool { self.has_null }

    fn merge(&mut self, rhs: &Self) {
        self.has_null = self.has_null || rhs.has_null;
        self.num_rows = self.num_rows + rhs.num_rows;
        merge_min(&mut self.min, rhs.min.as_ref().map(|x| x.as_str()));
        merge_max(&mut self.max, rhs.max.as_ref().map(|x| x.as_str()));
        self.sum_lengths += rhs.sum_lengths;
    }
}

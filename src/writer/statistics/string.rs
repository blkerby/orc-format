use std::cmp;
use super::common::BaseStatistics;

#[derive(Debug, Clone)]
pub struct StringStatistics {
    pub num_values: u64,
    pub num_present: u64,
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
            num_values: 0,
            num_present: 0,
            min: None,
            max: None,
            sum_lengths: 0,
        }
    }

    pub fn update(&mut self, x: Option<&str>) {
        self.num_values += 1;
        self.num_present += x.is_some() as u64;
        merge_min(&mut self.min, x);
        merge_max(&mut self.max, x);
        if let Some(xv) = x {
            self.sum_lengths += xv.len() as u64;
        }   
    }
}

impl BaseStatistics for StringStatistics {
    fn num_values(&self) -> u64 { self.num_values }

    fn num_present(&self) -> u64 { self.num_present }

    fn merge(&mut self, rhs: &Self) {
        self.num_values += rhs.num_values;
        self.num_present += rhs.num_present;
        merge_min(&mut self.min, rhs.min.as_ref().map(|x| x.as_str()));
        merge_max(&mut self.max, rhs.max.as_ref().map(|x| x.as_str()));
        self.sum_lengths += rhs.sum_lengths;
    }
}

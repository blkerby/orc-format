use std::fmt::Write;
use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct DecimalStatistics {
    pub scale: u32,
    pub num_values: u64,
    pub num_present: u64,
    pub min: Option<i128>,
    pub max: Option<i128>,
    pub sum: Option<i128>,
}

fn merge_min(x: &mut Option<i128>, y: Option<i128>) {
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

fn merge_max(x: &mut Option<i128>, y: Option<i128>) {
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

fn merge_sum(x: &mut Option<i128>, y: Option<i128>) {
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

impl DecimalStatistics {
    pub fn new(scale: u32) -> Self {
        Self {
            scale,
            num_values: 0,
            num_present: 0,
            min: None,
            max: None,
            sum: Some(0),
        }
    }

    pub fn update(&mut self, x: i128) {
        self.num_values += 1;
        self.num_present += 1;
        merge_min(&mut self.min, Some(x));
        merge_max(&mut self.max, Some(x));
        merge_sum(&mut self.sum, Some(x));
    }

    pub fn format(&self, mut val: i128) -> String {
        let mut out = String::new();
        if val < 0 {
            out.push_str("-");
            val = -val;
        }
        let modulus = 10i128.pow(self.scale);
        let quo = val / modulus;
        let rem = val % modulus;
        write!(&mut out, "{}", quo).unwrap();
        if rem > 0 {
            write!(&mut out, ".{1:00$}", self.scale as usize, rem).unwrap();
        }
        out
    }
}

impl BaseStatistics for DecimalStatistics {
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

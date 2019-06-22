use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct TimestampStatistics {
    pub num_values: u64,
    pub num_present: u64,
    pub min_epoch_millis: Option<i64>,
    pub max_epoch_millis: Option<i64>,
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

impl TimestampStatistics {
    pub fn new() -> Self {
        Self {
            num_values: 0,
            num_present: 0,
            min_epoch_millis: None,
            max_epoch_millis: None,
        }
    }

    pub fn update(&mut self, x: Option<i64>) {
        self.num_values += 1;
        self.num_present += x.is_some() as u64;
        if x.is_some() {
            merge_min(&mut self.min_epoch_millis, x);
            merge_max(&mut self.max_epoch_millis, x);
        }
    }
}

impl BaseStatistics for TimestampStatistics {
    fn num_values(&self) -> u64 { self.num_values }

    fn num_present(&self) -> u64 { self.num_present }

    fn merge(&mut self, rhs: &Self) {
        self.num_values += rhs.num_values;
        self.num_present += rhs.num_present;
        merge_min(&mut self.min_epoch_millis, rhs.min_epoch_millis);
        merge_max(&mut self.max_epoch_millis, rhs.max_epoch_millis);
    }
}

use super::common::BaseStatistics;

#[derive(Debug, Copy, Clone)]
pub struct DoubleStatistics {
    pub num_values: u64,
    pub num_present: u64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub sum: f64,
}

fn merge_min(x: &mut Option<f64>, y: Option<f64>) {
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

fn merge_max(x: &mut Option<f64>, y: Option<f64>) {
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

impl DoubleStatistics {
    pub fn new() -> DoubleStatistics {
        DoubleStatistics {
            num_values: 0,
            num_present: 0,
            min: None,
            max: None,
            sum: 0.0,
        }
    }

    pub fn update(&mut self, x: Option<f64>) {
        self.num_values += 1;
        self.num_present += x.is_some() as u64;
        merge_min(&mut self.min, x);
        merge_max(&mut self.max, x);
        if let Some(xv) = x { self.sum += xv; }
    }
}

impl BaseStatistics for DoubleStatistics {
    fn num_values(&self) -> u64 { self.num_values }

    fn num_present(&self) -> u64 { self.num_present }

    fn merge(&mut self, rhs: &Self) {
        self.num_values += rhs.num_values;
        self.num_present += rhs.num_present;
        merge_min(&mut self.min, rhs.min);
        merge_max(&mut self.max, rhs.max);
        self.sum += rhs.sum;
    }
}

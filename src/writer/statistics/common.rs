use std::cmp;
use std::ops::Add;

pub trait BaseStatistics {
    fn num_rows(&self) -> u64;
    fn has_null(&self) -> bool;
    fn merge(&mut self, rhs: &Self);
}

pub fn merge_min<T: Ord>(x: Option<T>, y: Option<T>) -> Option<T> {
    match x {
        None => y,
        Some(a) => match y {
            None => None,
            Some(b) => Some(cmp::min(a, b))
        }
    }
}

pub fn merge_max<T: Ord>(x: Option<T>, y: Option<T>) -> Option<T> {
    match x {
        None => y,
        Some(a) => match y {
            None => None,
            Some(b) => Some(cmp::max(a, b))
        }
    }
}

pub fn merge_sum<T: Add<Output=T>>(x: Option<T>, y: Option<T>) -> Option<T> {
    match x {
        None => None,
        Some(a) => match y {
            None => None,
            Some(b) => Some(a + b)
        }
    }
}

pub fn best_root(available: i32, size: i32) -> i32 {
    let avail = available - 2; // reserve a couple of buffers
    if avail <= 1 {
        return 1;
    }
    let mut k = i32::MAX;
    let mut i: f64 = 1.0;
    while k > avail {
        i += 1.0;
        k = (size as f64).powf(1.0 / i).ceil() as i32;
    }

    k
}

pub fn best_factor(available: i32, size: i32) -> i32 {
    let avail = available - 2; // reserve a couple of buffers
    if avail <= 1 {
        return 1;
    }
    let mut k = size;
    let mut i: f64 = 1.0;
    while k > avail {
        i += 1.0;
        k = (size as f64 / i).ceil() as i32;
    }

    k
}

#[cfg(test)]
mod tests {
    use super::{best_factor, best_root};

    #[test]
    fn best_root_unit_test() {
        assert_eq!(best_root(1_010, 1_000_000), 1_000);
        assert_eq!(best_root(500, 1_000_000), 100);
        assert_eq!(best_root(100, 1_000_000), 32);
        assert_eq!(best_root(30, 1_000_000), 16);
        assert_eq!(best_root(15, 1_000_000), 10);
        assert_eq!(best_root(10, 1_000_000), 8);
        assert_eq!(best_root(8, 1_000_000), 6);
        assert_eq!(best_root(7, 1_000_000), 5);
        assert_eq!(best_root(6, 1_000_000), 4);
        assert_eq!(best_root(5, 1_000_000), 3);
        assert_eq!(best_root(4, 1_000_000), 2);
    }

    #[test]
    fn best_factor_unit_test() {
        assert_eq!(best_factor(1_010, 1_000), 1_000);
        assert_eq!(best_factor(1_000, 1_000), 500);
        assert_eq!(best_factor(500, 1_000), 334);
        assert_eq!(best_factor(300, 1_000), 250);
        assert_eq!(best_factor(250, 1_000), 200);
        assert_eq!(best_factor(200, 1_000), 167);
        assert_eq!(best_factor(150, 1_000), 143);
        assert_eq!(best_factor(130, 1_000), 125);
        assert_eq!(best_factor(120, 1_000), 112);
        assert_eq!(best_factor(110, 1_000), 100);
    }
}

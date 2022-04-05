pub fn best_root(available: i32, size: i32) -> i32 {
    let avail = available - 2; // reserve a couple of buffers
    if avail <= 1 {
        return 1;
    }
    let mut k = i32::MAX;
    let mut i: f32 = 1.0;
    while k > avail {
        i += 1.0;
        k = (size as f32).powf(1.0 / i).ceil() as i32;
    }

    k
}

pub fn best_factor(available: i32, size: i32) -> i32 {
    let avail = available - 2; // reserve a couple of buffers
    if avail <= 1 {
        return 1;
    }
    let mut k = size;
    let mut i: f32 = 1.0;
    while k > avail {
        i += 1.0;
        k = (size as f32 / i).ceil() as i32;
    }

    k
}

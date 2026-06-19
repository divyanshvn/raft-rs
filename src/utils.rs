use std::time::{self, UNIX_EPOCH};

pub fn get_current_time() -> u64 {
    time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// 05/26/2026 02:23 PM IST - TODO: i might want to move to mili-seconds later
// 06/14/2026 02:44 PM IST - TODO: Ofcourse, add rand
pub fn rand_election_timeout(min_timeout: u64) -> u64 {
    min_timeout + (rand::random::<u64>() % min_timeout)
}

pub fn exceeds_majority(num: u32, total: usize) -> bool {
    if (num as usize) >= (total + 1).div_ceil(2) {
        return true;
    }
    false
}

pub fn is_majority(num: u32, total: usize) -> bool {
    if (num as usize) == (total + 1).div_ceil(2) {
        return true;
    }
    false
}

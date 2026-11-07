use std::time::{self, Duration, Instant, UNIX_EPOCH};

use tarpc::context::Context;

pub const MIN_TIMEOUT: u64 = 500; // 100 mili-seconds

pub fn get_current_time() -> u64 {
    time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn rand_election_timeout(min_timeout: u64) -> u64 {
    min_timeout + (rand::random::<u64>() % min_timeout)
}

pub fn is_majority(num: u32, total: usize) -> bool {
    if (num as usize) >= (total + 1).div_ceil(2) {
        return true;
    }
    false
}

pub fn just_majority(num: u32, total: usize) -> bool {
    if (num as usize) == (total + 1).div_ceil(2) {
        return true;
    }
    false
}

pub fn context_with_deadline(deadline_duration: Duration) -> Context {
    let mut context = Context::current();
    context.deadline = Instant::now() + deadline_duration;
    context
}

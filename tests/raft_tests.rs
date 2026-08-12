mod common;

use std::thread::current;
use std::time::{self, Duration};

use common::config::Config;
use tarpc::{context, trace};
use time::Instant;
use tokio::time::sleep;
use tracing::{debug, info};

#[tokio::test]
async fn test_initial_election() {
    common::tracing::init();

    let cfg = Config::new(3).await;

    let (leader, term) = cfg.check_one_leader(None).await;
    info!(leader, term, "elected leader");
}

#[tokio::test]
async fn test_sleep() {
    let current_time = time::SystemTime::now();

    sleep(Duration::from_secs(1)).await;
    let now = time::SystemTime::now();

    println!("{}", now.duration_since(current_time).unwrap().as_millis());
}

#[tokio::test]
async fn test_re_election() {
    common::tracing::init();
    // console_subscriber::init();

    let cfg = Config::new(5).await;
    let mut leaders = Vec::new();

    let (mut leader, mut term) = cfg.check_one_leader(None).await;
    leaders.push(leader);
    info!(leader, term, "elected leader");

    for i in 0..10 {
        info!(i, history = format!("{:?}", leaders), "iteration");
        let old_leader = leader;
        cfg.disconnect(leader);
        let current_time = Instant::now();

        info!(previous_leader = old_leader, "waiting for new leader");
        let before_time = Instant::now();
        (leader, term) = cfg.check_one_leader(Some(term)).await;
        info!(
            leader,
            "elected replacement leader in {} ms",
            (Instant::now() - before_time).as_millis()
        );
        leaders.push(leader);
        let mut context = context::Context::current();
        context.deadline = Instant::now() + Duration::from_secs(15);
        let log_str = format!("test-{}", i);
        info!(log_str, leader, "calling reply_log");
        cfg.clients[&leader]
            .reply_log(context, log_str)
            .await
            .unwrap()
            .unwrap();
        info!(
            elapsed_ms = (Instant::now() - current_time).as_millis(),
            "reply_log returned with reelection, with leader {}", leader
        );
        cfg.connect(old_leader);
        info!(old_leader, "previour leader back online");
    }
    // The new leader should remain leader or another election happens, but there should be exactly one leader.
    // let leader3 = cfg.check_one_leader().await;
    // println!("Final elected leader: {}", leader3);
}

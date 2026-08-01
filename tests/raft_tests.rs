mod common;

use std::thread::current;
use std::time::{self, Duration};

use common::config::Config;
use tarpc::context;
use time::Instant;
use tokio::time::sleep;
use tracing::info;

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

    let cfg = Config::new(5).await;

    let (mut leader, mut term) = cfg.check_one_leader(None).await;
    info!(leader, term, "elected leader");

    for i in 0..10 {
        info!(i, "iteration");
        let old_leader = leader;
        cfg.disconnect(leader);
        let current_time = Instant::now();

        info!(previous_leader = old_leader, "waiting for new leader");
        (leader, term) = cfg.check_one_leader(Some(term)).await;
        info!(leader, "elected replacement leader");
        cfg.clients[&leader]
            .reply_log(context::current(), "test".to_string())
            .await
            .unwrap()
            .unwrap();
        info!(
            elapsed_secs = (Instant::now() - current_time).as_secs(),
            "re-election completed"
        );
        cfg.connect(old_leader);
    }
    // The new leader should remain leader or another election happens, but there should be exactly one leader.
    // let leader3 = cfg.check_one_leader().await;
    // println!("Final elected leader: {}", leader3);
}

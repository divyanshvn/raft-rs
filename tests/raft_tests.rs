mod common;

use std::time;

use common::config::Config;
use tarpc::context;
use time::Instant;
use tokio::time::Duration;

#[tokio::test]
async fn test_initial_election() {
    let cfg = Config::new(3).await;

    let (leader, term) = cfg.check_one_leader(None).await;
    println!("Elected leader: {}", leader);
}

#[tokio::test]
async fn test_re_election() {
    let cfg = Config::new(5).await;

    let (mut leader, mut term) = cfg.check_one_leader(None).await;
    println!("Elected leader: {} at the term: {}", leader, term);

    for _ in 0..10 {
        let old_leader = leader;
        cfg.disconnect(leader);
        let current_time = Instant::now();

        println!("Getting new leader, term: {}", term);
        (leader, term) = cfg.check_one_leader(Some(term)).await;
        println!("leader: {}, term: {}", leader, term);
        cfg.clients[&leader]
            .reply_log(context::current(), "test".to_string())
            .await
            .unwrap()
            .unwrap();
        println!(
            "success. time taken in secs: {}",
            (Instant::now() - current_time).as_secs()
        );
        cfg.connect(old_leader);
    }
    // The new leader should remain leader or another election happens, but there should be exactly one leader.
    // let leader3 = cfg.check_one_leader().await;
    // println!("Final elected leader: {}", leader3);
}

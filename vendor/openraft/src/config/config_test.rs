use core::time::Duration;

use crate::config::error::ConfigError;
use crate::Config;
use crate::SnapshotPolicy;

#[test]
fn test_config_defaults() {
    let cfg = Config::default();

    assert!(cfg.election_timeout_min >= 150);
    assert!(cfg.election_timeout_max <= 300);

    assert_eq!(50, cfg.heartbeat_interval);
    assert_eq!(300, cfg.max_payload_entries);
    assert_eq!(5000, cfg.replication_lag_threshold);

    assert_eq!(3 * 1024 * 1024, cfg.snapshot_max_chunk_size);
    assert_eq!(SnapshotPolicy::LogsSinceLast(5000), cfg.snapshot_policy);
}

#[test]
fn test_invalid_election_timeout_config_produces_expected_error() {
    let config = Config {
        election_timeout_min: 1000,
        election_timeout_max: 700,
        ..Default::default()
    };

    let res = config.validate();
    let err = res.unwrap_err();
    assert_eq!(err, ConfigError::ElectionTimeout { min: 1000, max: 700 });

    let config = Config {
        election_timeout_min: 1000,
        election_timeout_max: 2000,
        heartbeat_interval: 1500,
        ..Default::default()
    };

    let res = config.validate();
    let err = res.unwrap_err();
    assert_eq!(err, ConfigError::ElectionTimeoutLTHeartBeat {
        election_timeout_min: 1000,
        heartbeat_interval: 1500
    });
}

#[test]
fn test_build() -> anyhow::Result<()> {
    let config = Config::build(&[
        "foo",
        "--cluster-name=bar",
        "--election-timeout-min=10",
        "--election-timeout-max=20",
        "--heartbeat-interval=5",
        "--send-snapshot-timeout=199",
        "--install-snapshot-timeout=200",
        "--max-payload-entries=201",
        "--snapshot-policy=since_last:202",
        "--replication-lag-threshold=203",
        "--snapshot-max-chunk-size=204",
        "--max-in-snapshot-log-to-keep=205",
        "--purge-batch-size=207",
    ])?;

    assert_eq!("bar", config.cluster_name);
    assert_eq!(10, config.election_timeout_min);
    assert_eq!(20, config.election_timeout_max);
    assert_eq!(5, config.heartbeat_interval);

    #[allow(deprecated)]
    {
        assert_eq!(199, config.send_snapshot_timeout);
    }
    assert_eq!(200, config.install_snapshot_timeout);
    assert_eq!(201, config.max_payload_entries);
    assert_eq!(SnapshotPolicy::LogsSinceLast(202), config.snapshot_policy);
    assert_eq!(203, config.replication_lag_threshold);
    assert_eq!(204, config.snapshot_max_chunk_size);
    assert_eq!(205, config.max_in_snapshot_log_to_keep);
    assert_eq!(207, config.purge_batch_size);

    // Test config methods
    #[allow(deprecated)]
    {
        let mut c = config;
        assert_eq!(Duration::from_millis(199), c.send_snapshot_timeout());
        assert_eq!(Duration::from_millis(200), c.install_snapshot_timeout());

        c.send_snapshot_timeout = 0;
        assert_eq!(
            Duration::from_millis(200),
            c.send_snapshot_timeout(),
            "by default send_snapshot_timeout is install_snapshot_timeout"
        );
    }
    Ok(())
}

#[test]
fn test_config_snapshot_policy() -> anyhow::Result<()> {
    let config = Config::build(&["foo", "--snapshot-policy=never"])?;
    assert_eq!(SnapshotPolicy::Never, config.snapshot_policy);

    let config = Config::build(&["foo", "--snapshot-policy=since_last:3"])?;
    assert_eq!(SnapshotPolicy::LogsSinceLast(3), config.snapshot_policy);

    let res = Config::build(&["foo", "--snapshot-policy=bar:3"]);
    assert!(res.is_err());

    Ok(())
}

#[test]
fn test_config_enable_tick() -> anyhow::Result<()> {
    let config = Config::build(&["foo", "--enable-tick=false"])?;
    assert_eq!(false, config.enable_tick);

    let config = Config::build(&["foo", "--enable-tick=true"])?;
    assert_eq!(true, config.enable_tick);

    let config = Config::build(&["foo", "--enable-tick"])?;
    assert_eq!(true, config.enable_tick);

    let config = Config::build(&["foo"])?;
    assert_eq!(true, config.enable_tick);

    Ok(())
}

#[test]
fn test_config_enable_heartbeat() -> anyhow::Result<()> {
    let config = Config::build(&["foo", "--enable-heartbeat=false"])?;
    assert_eq!(false, config.enable_heartbeat);

    let config = Config::build(&["foo", "--enable-heartbeat=true"])?;
    assert_eq!(true, config.enable_heartbeat);

    let config = Config::build(&["foo", "--enable-heartbeat"])?;
    assert_eq!(true, config.enable_heartbeat);

    let config = Config::build(&["foo"])?;
    assert_eq!(true, config.enable_heartbeat);

    Ok(())
}

#[test]
fn test_config_enable_elect() -> anyhow::Result<()> {
    let config = Config::build(&["foo", "--enable-elect=false"])?;
    assert_eq!(false, config.enable_elect);

    let config = Config::build(&["foo", "--enable-elect=true"])?;
    assert_eq!(true, config.enable_elect);

    let config = Config::build(&["foo", "--enable-elect"])?;
    assert_eq!(true, config.enable_elect);

    let config = Config::build(&["foo"])?;
    assert_eq!(true, config.enable_elect);

    Ok(())
}

/// W3.1 — `enable_pre_vote` is a Config field, default off (upstream-compatible),
/// settable via CLI / clap.
#[test]
fn test_config_enable_pre_vote() -> anyhow::Result<()> {
    let config = Config::default();
    assert!(!config.enable_pre_vote, "default must match upstream openraft 0.9 behavior");

    let config = Config::build(&["foo", "--enable-pre-vote=true"])?;
    assert!(config.enable_pre_vote);

    let config = Config::build(&["foo", "--enable-pre-vote"])?;
    assert!(config.enable_pre_vote);

    Ok(())
}

/// W3.5 — `check_quorum_ratio` is a Config field. Default 0.0 means upstream-compatible
/// (CheckQuorum disabled). Ferrosa builds set 0.75 per ADR-012.
#[test]
fn test_config_check_quorum_ratio_default_disabled() -> anyhow::Result<()> {
    let config = Config::default();
    assert_eq!(config.check_quorum_ratio, 0.0);
    assert!(!config.is_check_quorum_enabled());
    assert_eq!(config.check_quorum_deadline_ms(), None);
    Ok(())
}

#[test]
fn test_config_check_quorum_ratio_ferrosa_default() -> anyhow::Result<()> {
    // The ferrosa default per ADR-012.
    let config = Config::build(&[
        "foo",
        "--election-timeout-min=3000",
        "--election-timeout-max=6000",
        "--heartbeat-interval=300",
        "--check-quorum-ratio=0.75",
    ])?
    .validate()?;
    assert_eq!(config.check_quorum_ratio, 0.75);
    assert!(config.is_check_quorum_enabled());
    // 6000 * 0.75 = 4500 ms — see ADR-012 rationale.
    assert_eq!(config.check_quorum_deadline_ms(), Some(4500));
    Ok(())
}

#[test]
fn test_config_check_quorum_ratio_validation_rejects_out_of_range() {
    let config = Config {
        check_quorum_ratio: -0.1,
        ..Default::default()
    };
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidCheckQuorumRatio { .. })
    ));

    let config = Config {
        check_quorum_ratio: 2.5,
        ..Default::default()
    };
    assert!(matches!(
        config.validate(),
        Err(ConfigError::InvalidCheckQuorumRatio { .. })
    ));
}

// Scaffold phase: types are defined before their consumers exist.
#![allow(dead_code)]

pub mod alert;
pub mod archive;
pub mod chaos;
pub mod checker;
pub mod cluster;
pub mod config;
pub mod cql_session;
pub mod docker_provision;
pub mod driver;
pub mod endurance;
/// W8.9 — sim-equivalent endurance run (used when Fly.io is
/// unavailable; ADR-016 layered verification stack).
pub mod endurance_sim;
pub mod firecracker;
pub mod flyio;
pub mod history;
pub mod orchestrator;
pub mod report;
pub mod ssh;
pub mod test_env;
pub mod workload;

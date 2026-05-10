//! Engine is an abstracted, complete raft consensus algorithm implementation.
//!
//! The internal `state` inside `Engine` contains complete information to run raft, i.e., it is a in
//! memory shadow of the underlying storage state.
//! Every time `Engine` receives an event, such as handle-vote-request, or elect, it updates its
//! internal state and emits several `Command` to let the underlying `Runtime` to take place the
//! state changes, e.g. `AppendInputEntries` or `LeaderCommit`.
//! I.e., `Runtime` is an adaptor connecting raft algorithm and concrete storage/network
//! implementation.
//!
//! ```text
//!                      Runtime         Engine
//!                      |               |
//!  Application ------> + ------------> +      // event: app-write, change-membership
//!                      |               |
//!  Timer ------------> + ------------> +      // event: election, heartbeat
//!                      |               |
//!  Storage <---------- + <------------ +      // cmd: append, truncate, purge, commit
//!                      |               |
//!       .------------- + <------------ +      // cmd: replicate-log, vote, install-snapshot
//!       v              |               |
//!  Network ----------> + ------------> +      // event: replication result, vote result
//!                      |               |
//!
//!
//!  ------->: event to Engine
//!  <-------: command to run
//! ```

mod check_quorum;
mod command;
mod command_kind;
mod engine_config;
mod engine_impl;
mod engine_output;
mod leader_lease;
mod log_id_list;
mod pre_vote_decision;

pub(crate) mod handler;
pub(crate) mod time_state;

#[cfg(test)]
mod tests {
    mod append_entries_test;
    mod elect_test;
    mod handle_pre_vote_req_test;
    mod handle_timeout_now_test;
    mod handle_vote_req_test;
    mod handle_vote_resp_test;
    mod initialize_test;
    mod install_full_snapshot_test;
    mod log_id_list_test;
    mod startup_test;
    mod trigger_purge_log_test;
}
#[cfg(test)]
pub(crate) mod testing;

pub(crate) use check_quorum::CheckQuorum;
pub(crate) use check_quorum::CheckQuorumDecision;
#[allow(unused_imports)] // Used once `following_handler::handle_pre_vote_req` lands.
pub(crate) use leader_lease::LeaderLease;
#[allow(unused_imports)]
pub(crate) use leader_lease::LeaseStatus;
#[allow(unused_imports)] // Used once `following_handler::handle_pre_vote_req` lands.
pub(crate) use pre_vote_decision::evaluate_pre_vote;
#[allow(unused_imports)]
pub(crate) use pre_vote_decision::is_log_up_to_date;
#[allow(unused_imports)]
pub(crate) use pre_vote_decision::PreVoteDecision;
pub(crate) use command::Command;
pub(crate) use command::Condition;
pub(crate) use command::Respond;
pub(crate) use command::ValueSender;
pub(crate) use command_kind::CommandKind;
pub(crate) use engine_config::EngineConfig;
pub(crate) use engine_impl::Engine;
pub(crate) use engine_output::EngineOutput;
pub use log_id_list::LogIdList;

//! Distributed planning, scheduling, workers, and transport.

pub mod control_plane;
pub mod coordinator;
pub mod data_plane;
pub mod distributed;
pub(crate) mod flight_exchange;
pub(crate) mod hash_exchange;
pub mod plan_codec;
pub mod protocol;
pub mod remote;
pub mod worker;

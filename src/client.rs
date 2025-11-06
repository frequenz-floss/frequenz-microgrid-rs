// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A clonable client for the microgrid API.

mod instruction;
mod microgrid_client_actor;
mod retry_tracker;

mod microgrid_api_client;
pub use microgrid_api_client::MicrogridApiClient;

mod microgrid_client_handle;
pub use microgrid_client_handle::MicrogridClientHandle;

#[cfg(test)]
pub(crate) mod test_utils;

// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{counters, PeerId};
//use admission_control_proto::proto::admission_control::{
////    admission_control_msg::Message as AdmissionControlMsg_oneof,
////    submit_transaction_response::Status,
////AdmissionControlMsg,
////SubmitTransactionRequest,
////    SubmitTransactionResponse,
//};
//use admission_control_proto::AdmissionControlStatus;
//use anyhow::{format_err, Result};
//use bounded_executor::BoundedExecutor;
//use bytes::Bytes;
use futures::stream::{select_all, StreamExt};
use libra_config::config::{AdmissionControlConfig, RoleType};
use libra_logger::prelude::*;
use network::validator_network::{
    AdmissionControlNetworkEvents, AdmissionControlNetworkSender, Event,
};
use std::collections::HashMap;
use std::sync::Arc;
use storage_client::StorageRead;

/// UpstreamProxyData is the set of data needed for a full node to send transaction write
/// requests to their upstream validator.
/// UpstreamProxyData is instantiated in AC Runtime.
#[derive(Clone)]
pub struct UpstreamProxyData<M, V> {
    ac_config: AdmissionControlConfig,
    network_sender: AdmissionControlNetworkSender,
    role: RoleType,
    /// gRPC client connecting Mempool.
    mempool_client: Option<M>,
    /// gRPC client to send read requests to Storage.
    storage_read_client: Arc<dyn StorageRead>,
    /// VM validator instance to validate transactions sent from wallets.
    vm_validator: Arc<V>,
    /// Flag indicating whether we need to check mempool before validation, drop txn if check
    /// fails.
    need_to_check_mempool_before_validation: bool,
}

/// Main routine for proxying write request. Starts a coordinator that listens for AdmissionControlMsg
#[allow(clippy::implicit_hasher)]
pub async fn process_network_messages(
    network_events: Vec<AdmissionControlNetworkEvents>,
    mut peer_info: HashMap<PeerId, bool>,
) {
    let mut events = select_all(network_events).fuse();
    loop {
        ::futures::select! {
            network_event = events.select_next_some() => {
                match network_event {
                    Ok(event) => {
                        match event {
                            Event::NewPeer(peer_id) => {
                                debug!("[admission control] new peer {}", peer_id);
                                new_peer(&mut peer_info, peer_id);
                                counters::UPSTREAM_PEERS.set(count_active_peers(&peer_info) as i64);
                            }
                            Event::LostPeer(peer_id) => {
                                debug!("[admission control] lost peer {}", peer_id);
                                lost_peer(&mut peer_info, peer_id);
                                counters::UPSTREAM_PEERS.set(count_active_peers(&peer_info) as i64);
                            }
                            _ => {},
                        }
                    },
                    Err(err) => { error!("[admission control] network error {:?}", err); },
                }
            }
        }
    }
}

/// new peer discovery handler
/// adds new entry to `peer_info`
fn new_peer(peer_info: &mut HashMap<PeerId, bool>, peer_id: PeerId) {
    if let Some(state) = peer_info.get_mut(&peer_id) {
        *state = true;
    }
}

/// lost peer handler. Marks connection as dead
fn lost_peer(peer_info: &mut HashMap<PeerId, bool>, peer_id: PeerId) {
    if let Some(state) = peer_info.get_mut(&peer_id) {
        *state = false;
    }
}

fn count_active_peers(peer_info: &HashMap<PeerId, bool>) -> usize {
    peer_info.iter().filter(|(_, &is_alive)| is_alive).count()
}

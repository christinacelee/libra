// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use channel::libra_channel;
use libra_config::config::{PeerNetworkId, UpstreamConfig};
use libra_logger::prelude::*;
use rand::seq::IteratorRandom;
use std::collections::{HashMap, HashSet};

/// stores only peers that receive txns from this node
pub(crate) type PeerInfo = HashMap<PeerNetworkId, PeerSyncState>;

/// state of last sync with peer
/// `timeline_id` is position in log of ready transactions
/// `is_alive` - is connection healthy
#[derive(Clone)]
pub(crate) struct PeerSyncState {
    pub timeline_id: u64,
    pub is_alive: bool,
}

pub(crate) struct PeerManager {
    upstream_config: UpstreamConfig,
    peer_info: PeerInfo,
    min_broadcast_recipient_count: usize,
    schedule_broadcast_tx: libra_channel::Sender<PeerNetworkId, PeerNetworkId>,
    picked_fallback_peers: HashSet<PeerNetworkId>,
}

impl PeerManager {
    pub fn new(
        upstream_config: UpstreamConfig,
        schedule_broadcast_tx: libra_channel::Sender<PeerNetworkId, PeerNetworkId>,
        min_broadcast_recipient_count: usize,
    ) -> Self {
        Self {
            upstream_config,
            peer_info: PeerInfo::new(),
            min_broadcast_recipient_count,
            schedule_broadcast_tx,
            picked_fallback_peers: HashSet::new(),
        }
    }

    pub fn add_peer(&mut self, peer: PeerNetworkId) {
        if self.is_upstream_peer(peer) {
            self.peer_info
                .entry(peer)
                .or_insert(PeerSyncState {
                    timeline_id: 0,
                    is_alive: true,
                })
                .is_alive = true;
            self.update_picked_peers(peer);
        }
    }

    pub fn disable_peer(&mut self, peer: PeerNetworkId) {
        if let Some(state) = self.peer_info.get_mut(&peer) {
            state.is_alive = false;
            self.update_picked_peers(peer);
        }
    }

    // based on liveness info, checks whether
    fn update_picked_peers(&mut self, peer: PeerNetworkId) {
        let mut new_picked_peers = vec![];
        let peer_state = self.peer_info.get(&peer).expect("missing peer state");

        if self.is_primary_upstream_peer(peer) && peer_state.is_alive {
            new_picked_peers.push(peer);
        } else if self.min_broadcast_recipient_count > 0 {
            // attempt to pick more fallback peers if k-policy is enabled
            let alive_primaries = self
                .peer_info
                .iter()
                .filter(|(peer, state)| self.is_primary_upstream_peer(**peer) && state.is_alive)
                .count();

            if !peer_state.is_alive {
                self.picked_fallback_peers.remove(&peer);
            }

            let broadcast_peers_count = alive_primaries + self.picked_fallback_peers.len();
            if broadcast_peers_count < self.min_broadcast_recipient_count {
                // pick more fallback peers
                let new_fallback_peers: Vec<PeerNetworkId> = self
                    .peer_info
                    .iter()
                    .filter(|(peer, state)| {
                        !self.is_primary_upstream_peer(**peer)
                            && !self.picked_fallback_peers.contains(&peer)
                            && state.is_alive
                    })
                    .map(|(peer, _state)| *peer)
                    .choose_multiple(
                        &mut rand::thread_rng(),
                        self.min_broadcast_recipient_count - broadcast_peers_count,
                    )
                    .into_iter()
                    .collect();

                self.picked_fallback_peers
                    .extend(new_fallback_peers.clone());
                new_picked_peers.extend(new_fallback_peers);
            }
        }

        // schedule broadcast for newly picked peers
        for peer in new_picked_peers {
            if let Err(_e) = &self.schedule_broadcast_tx.push(peer, peer) {
                error!("failed to schedule broadcast for new peer");
            }
        }
    }

    pub fn update_peer_broadcast(&mut self, peer_broadcasts: Vec<(PeerNetworkId, u64)>) {
        let peer_info = &mut self.peer_info;
        for (peer, new_timeline_id) in peer_broadcasts {
            peer_info.entry(peer).and_modify(|t| {
                t.timeline_id = new_timeline_id;
            });
        }
    }

    pub fn is_upstream_peer(&self, peer: PeerNetworkId) -> bool {
        self.upstream_config.is_upstream_peer(peer)
    }

    fn is_primary_upstream_peer(&self, peer: PeerNetworkId) -> bool {
        self.upstream_config.is_primary_upstream_peer(peer)
    }

    // returns a peer's sync state if the peer qualifies as broadcast recipient
    pub fn should_broadcast(&self, peer: PeerNetworkId) -> Option<PeerSyncState> {
        if self.is_primary_upstream_peer(peer) || self.picked_fallback_peers.contains(&peer) {
            self.peer_info.get(&peer).cloned()
        } else {
            None
        }
    }
}

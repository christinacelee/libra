// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::core_mempool::{CoreMempool, TimelineState};
use anyhow::{format_err, Result};
use bounded_executor::BoundedExecutor;
use bytes05::Bytes;
use channel::{
    libra_channel,
    libra_channel::{Receiver, Sender},
    message_queues::QueueStyle,
};
use futures::{
    channel::{mpsc::UnboundedSender, oneshot},
    executor::ThreadPoolBuilder,
    future::join_all,
    StreamExt,
};
use libra_config::config::{MempoolConfig, NodeConfig};
use libra_logger::prelude::*;
use libra_prost_ext::MessageExt;
use libra_types::{transaction::SignedTransaction, PeerId};
use network::{
    proto::{
        BroadcastTransactionsRequest, BroadcastTransactionsResponse, MempoolSyncMsg,
        MempoolSyncMsg_oneof,
    },
    validator_network::{Event, MempoolNetworkEvents, MempoolNetworkSender, RpcError},
};
use num_cpus;
use std::{
    collections::{HashMap, HashSet},
    convert::{TryFrom, TryInto},
    sync::{Arc, Mutex},
    thread::sleep,
    time::{Duration, Instant},
};
use storage_client::StorageRead;
use tokio::{
    runtime::{Builder, Handle, Runtime},
    time::interval,
};
use vm_validator::vm_validator::{get_account_state, TransactionValidation};

const WORKER_CHANNEL_BUFFER_SIZE: usize = 1;
const MASTER_CHANNEL_KEY: i64 = 1;

/// state of last sync with peer
/// `timeline_id`: the timeline ID of the last transaction broadcast to and successfully processed this peer
/// `is_alive`: is the worker process for this peer still alive
/// `to_worker`: the sending end of the channel for sending activity status to the worker process for this peer
#[derive(Clone, Debug)]
struct PeerSyncState {
    timeline_id: u64,
    is_alive: bool,
    to_worker: Sender<i64, WorkerState>,
}

#[derive(Clone, Hash)]
struct PeerSyncUpdate {
    peer_id: PeerId,
    timeline_id: u64,
}

#[derive(Clone, Debug, PartialEq)]
enum WorkerState {
    START,
    PAUSE,
    KILL,
}

type PeerInfo = HashMap<PeerId, PeerSyncState>;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum SharedMempoolNotification {
    PeerStateChange,
    NewTransactions,
    SyncUpdate { worker_peer: PeerId },
}

/// Struct that owns all dependencies required by shared mempool routines
#[derive(Clone)]
struct SharedMempool<V>
where
    V: TransactionValidation + 'static,
{
    mempool: Arc<Mutex<CoreMempool>>,
    network_sender: MempoolNetworkSender,
    config: MempoolConfig,
    storage_read_client: Arc<dyn StorageRead>,
    validator: Arc<V>,
    validator_peers: HashSet<PeerId>, // read-only and doesn't change, so thread-safe
    subscribers: Vec<UnboundedSender<SharedMempoolNotification>>,
}

fn notify_subscribers(
    event: SharedMempoolNotification,
    subscribers: &[UnboundedSender<SharedMempoolNotification>],
) {
    for subscriber in subscribers {
        let _ = subscriber.unbounded_send(event);
    }
}

/// send callback here
async fn process_rpc_submit_transactions_request<V>(
    smp: SharedMempool<V>,
    peer_id: PeerId,
    request: BroadcastTransactionsRequest,
    callback: oneshot::Sender<Result<Bytes, RpcError>>,
) where
    V: TransactionValidation,
{
    // the RPC response object that will be returned to sender via callback
    debug!(
        "[shared mempool] in process_rpc_submit_transactions_req from {:?}",
        peer_id
    );
    let start_submit = Instant::now();
    let resp = match submit_transactions_to_mempool(smp.clone(), peer_id, request).await {
        Ok(response) => response,
        Err(e) => {
            error!(
                "[shared mempool] Error occurred in submitting transactions to local mempool: {:?}",
                e
            );
            // TODO return error in response
            let mut response = BroadcastTransactionsResponse::default();
            response.backpressure_ms = 0; // TODO this is placeholder
            response
        }
    };
    let finish_submit = start_submit.elapsed().as_millis();
    debug!(
        "[shared mempool] finished submit_transactions_to_mempool for peer {:?} in {:?} ms",
        peer_id, finish_submit
    );
    let response_msg = MempoolSyncMsg {
        message: Some(MempoolSyncMsg_oneof::BroadcastTransactionsResponse(resp)),
    };

    // send response to callback
    let response_data = response_msg.to_bytes().expect("failed to serialize proto");
    let start_callback = Instant::now();
    if let Err(err) = callback
        .send(Ok(response_data))
        .map_err(|_| format_err!("[shared mempool] handling inbound RPC call timed out"))
    {
        error!(
            "[shared mempool] failed to process batched transaction request, error: {:?}",
            err
        );
    }
    let end_callback = start_callback.elapsed().as_millis();
    debug!(
        "[shared mempool] finished callback to peer {:?} in {:?} ms",
        peer_id, end_callback
    );
    notify_subscribers(SharedMempoolNotification::NewTransactions, &smp.subscribers);
}

/// Tries to add transactions to local mempool
/// returns RPC response that can be sent to callback
/// peer_id is the PeerId of the node that sent the request
async fn submit_transactions_to_mempool<V>(
    smp: SharedMempool<V>,
    peer_id: PeerId,
    request: BroadcastTransactionsRequest,
) -> Result<BroadcastTransactionsResponse>
where
    V: TransactionValidation,
{
    debug!(
        "[shared mempool] submit {:?} txns to mempool for {:?}",
        request.transactions.len(),
        peer_id
    );
    /////////////////////////////////////////////
    // convert from proto to SignedTransaction //
    /////////////////////////////////////////////
    let start_proto = Instant::now();
    let transactions: Vec<_> = request
        .transactions
        .clone()
        .into_iter()
        .filter_map(|txn| match SignedTransaction::try_from(txn.clone()) {
            Ok(t) => Some(t),
            Err(e) => {
                // TODO make RPC response for this invalid transaction
                // log error
                security_log(SecurityEvent::InvalidTransactionMP)
                    .error(&e)
                    .data(&txn)
                    .log();
                None
            }
        })
        .collect();
    let end_proto = start_proto.elapsed().as_millis();
    debug!(
        "[shared mempool] finished proto convert for {:?} in {:?} ms",
        peer_id, end_proto
    );

    //////////////////////////////////
    // validate transactions via VM //
    //////////////////////////////////

    let start_get_account_state = Instant::now();
    let account_states = join_all(
        transactions
            .iter()
            .map(|t| get_account_state(smp.storage_read_client.clone(), t.sender())),
    )
    .await;
    let finish_get_account_state = start_get_account_state.elapsed().as_millis();
    debug!(
        "[shared mempool] finished get_account_state for {:?} in {:?} ms",
        peer_id, finish_get_account_state
    );

    let transactions: Vec<_> = transactions
        .into_iter()
        .enumerate()
        .filter_map(|(idx, t)| {
            if let Ok((sequence_number, balance)) = account_states[idx] {
                return Some((t, sequence_number, balance));
            }
            None
        })
        .collect();

    debug!("[shared mempool] finished txn filter for {:?}", peer_id);

    let start_validate = Instant::now();
    let validations = join_all(
        transactions
            .iter()
            .map(|t| smp.validator.validate_transaction(t.0.clone())),
    )
    .await;
    let finish_validate = start_validate.elapsed().as_millis();
    debug!(
        "[shared mempool] finished vm validate for {:?} in {:?} ms",
        peer_id, finish_validate
    );

    ///////////////////////////////
    // add txns to local mempool //
    ///////////////////////////////

    // The [`TimelineState`] for adding txns to mempool should only be TimelineState::NonQualified,
    // (i.e. this node should never try to broadcast it) if this MempoolSyncMsg came from one validator
    // to another validator in the same validator network.
    // TODO later get TimelineState from network analysis, to accommodate dynamic configs/network
    let timeline_state = if smp.validator_peers.contains(&peer_id) {
        TimelineState::NonQualified
    } else {
        debug!("[shared mempool] timelinestate error");
        TimelineState::NotReady
    };

    let mut mempool = smp
        .mempool
        .lock()
        .expect("[shared mempool] failed to acquire mempool lock");

    for (idx, (transaction, sequence_number, balance)) in transactions.into_iter().enumerate() {
        if let Ok(None) = validations[idx] {
            let gas_cost = transaction.max_gas_amount();
            let status = mempool.add_txn(
                transaction,
                gas_cost,
                sequence_number,
                balance,
                // peer validator SMP network or from FN
                timeline_state,
            );
            debug!(
                "[shared mempool] mp add txn status for peer {:?}: {:?}",
                peer_id, status
            );
        // TODO log/update counters for MempoolAddTransactionStatus
        // TODO check for MempoolAddTransactionStatus::MempoolIsFull and calculate backpressure
        } else {
            // txn vm validation failed
            debug!(
                "[shared mempool] txn vm validation failed for peer {:?}",
                peer_id
            );
            // TODO log/update counters for failed vm validation VMStatus
        }
    }

    // return RPC response for this request
    // TODO currently this is a dummy response - need to add real backpressure
    // and potentially more info on individual txn failures
    let mut response = BroadcastTransactionsResponse::default();
    response.backpressure_ms = 0;
    Ok(response)
}

/// This task handles inbound network events.
async fn inbound_network_task<V>(
    smp: SharedMempool<V>,
    executor: Handle,
    mut network_events: MempoolNetworkEvents,
) where
    V: TransactionValidation,
{
    let mut peer_info: PeerInfo = HashMap::new();
    let subscribers = smp.subscribers.clone();

    // Use a BoundedExecutor to restrict only `workers_available` concurrent
    // worker tasks that can process incoming transactions.
    let workers_available = smp.config.shared_mempool_max_concurrent_inbound_syncs;
    let bounded_executor = BoundedExecutor::new(workers_available, executor.clone());

    // Create a threadpool that manages concurrent worker processes
    //    let worker_pool = ThreadPool::new()
    //        .expect("[shared mempool] failed to create threadpool for worker processes");
    let worker_pool = ThreadPoolBuilder::new()
        .pool_size(99)
        .create()
        .expect("[shared mempool] failed to create threadpool for worker processes");

    debug!("old worker pool size: {:?}", num_cpus::get());
    let (worker_sender, mut receiver) =
        libra_channel::new(QueueStyle::LIFO, WORKER_CHANNEL_BUFFER_SIZE, None);
    let batch_size = smp.config.shared_mempool_batch_size;
    loop {
        ::futures::select! {
            // network events
            maybe_network_event = network_events.next() => {
                match maybe_network_event {
                    None => {
                        // TODO log termination of network events stream
                        debug!("[shared mempool] network events stream terminated");
                        return;
                    }
                    Some(network_event) => match network_event {
                        Ok(event) => match event {
                            Event::NewPeer(peer_id) => {
                                // TODO log event
                                debug!("[shared mempool] received NewPeer notif from peer {:?}", peer_id);
                                notify_subscribers(SharedMempoolNotification::PeerStateChange, &subscribers);
                                let mut timeline_id = 0;

                                // start outbound rpc sync process for this new peer
                                // check if this brand new peer
                                if let Some(mut peer_sync_state) = peer_info.get_mut(&peer_id) {
                                    // try restarting paused worker
                                    debug!("[shared mempool] restarting paused peer {:?}", peer_id);
                                    let start_restart_peer = Instant::now();
                                    if peer_sync_state.to_worker.push(MASTER_CHANNEL_KEY, WorkerState::START).is_err() {
                                        // TODO log this
                                        debug!("[shared mempool] failed to restart paused peer {:?} in {:?} ms", peer_id, start_restart_peer.elapsed().as_millis());
                                        // restart worker process for this peer
                                        timeline_id = peer_sync_state.timeline_id;
                                    } else {
                                        continue;
                                    }
                                }

                                // start brand new worker
                                let (master_sender, worker_receiver) = libra_channel::new(
                                    QueueStyle::LIFO,
                                    WORKER_CHANNEL_BUFFER_SIZE,
                                    None,
                                );
                                let smp_outbound = smp.clone();
                                let mempool = smp_outbound.mempool.clone();
                                let mut network_sender = smp_outbound.network_sender.clone();
                                peer_info.insert(peer_id, PeerSyncState {
                                    timeline_id,
                                    is_alive: true,
                                    to_worker: master_sender,
                                });
                                let worker_sender_clone = worker_sender.clone();

                                // TODO log
                                debug!("[shared mempool] starting new worker for {:?}", peer_id);
                                let start_worker = Instant::now();
                                worker_pool.spawn_ok(async move {
                                    worker(
                                        worker_receiver,
                                        worker_sender_clone,
                                        peer_id,
                                        &mempool,
                                         &mut network_sender,
                                        timeline_id,
                                        batch_size,
                                    )
                                    .await;
                                });
                                let finished_worker_spawn = start_worker.elapsed().as_millis();
                                debug!("[shared mempool] finished spawning new worker for {:?} in {:?} ms", peer_id, finished_worker_spawn);
                            }
                            Event::LostPeer(peer_id) => {
                                // TODO log event
                                debug!("[shared mempool] received LostPeer for {:?}", peer_id);
                                notify_subscribers(SharedMempoolNotification::PeerStateChange, &subscribers);

                                if let Some(peer_sync_state) = peer_info.get_mut(&peer_id) {
                                    // check state
                                    if peer_sync_state.is_alive {
                                        if peer_sync_state.to_worker.push(MASTER_CHANNEL_KEY, WorkerState::PAUSE).is_err() {
                                            // TODO log this
                                            // disconnected channels will just treat worker process as dead
                                        }
                                        peer_sync_state.is_alive = false;
                                    } else {
                                        debug!(
                                            "[shared mempool] received LostPeer event for peer {} which was already lost",
                                            peer_id
                                        );
                                    }
                                } else {
                                    debug!(
                                        "[shared mempool] received LostPeer event for non-existent peer {}",
                                        peer_id
                                    );
                                }

                            }
                            Event::RpcRequest((peer_id, msg, callback)) => {
                                // handle rpc events for transactions
                                debug!("[shared mempool] received Broadcast from {:?}", peer_id);
                                if let Some(MempoolSyncMsg_oneof::BroadcastTransactionsRequest(request)) =
                                    msg.message
                                {
                                    let spawn_start = Instant::now();
                                    bounded_executor
                                        .spawn(process_rpc_submit_transactions_request(
                                            smp.clone(),
                                            peer_id,
                                            request,
                                            callback,
                                        ))
                                        .await;

                                    let spawn_finish = spawn_start.elapsed().as_millis();
                                    debug!("[shared mempool] spawned process_rpc_submit_transactions_request from peer {:?} in {:?} ms",
                                    peer_id, spawn_finish);
                                }
                            }
                            _ => {}
                        }
                        Err(e) => {
                            security_log(SecurityEvent::InvalidNetworkEventMP)
                                .error(&e)
                                .log();
                        }
                    }
                }
            },
            maybe_peer_sync_update = receiver.next() => {
                match maybe_peer_sync_update {
                    None => { }, // TODO log
                    Some(peer_sync_update) => {
                        notify_subscribers(SharedMempoolNotification::SyncUpdate {worker_peer: peer_sync_update.peer_id}, &subscribers);
                        if let Some(sync_state) = peer_info.get_mut(&peer_sync_update.peer_id) {
                            debug!("[shared mempool] received worker update from peer {:?}", peer_sync_update.peer_id);
                            sync_state.timeline_id = peer_sync_update.timeline_id;
                        } else {
                            error!("[shared mempool] unexpectedly got peer sync update from a worker for unknown peer");
                        }
                    }
                }
            },
            complete => {
                // TODO log
                debug!("[shared mempool] worker update channel terminated");
                break;
            }
        }
    }
    crit!("[shared_mempool] finished listening to inbound event");
}

/// GC all expired transactions by SystemTTL
async fn gc_task(mempool: Arc<Mutex<CoreMempool>>, gc_interval_ms: u64) {
    let mut interval = interval(Duration::from_millis(gc_interval_ms));
    while let Some(_interval) = interval.next().await {
        mempool
            .lock()
            .expect("[shared mempool] failed to acquire mempool lock")
            .gc_by_system_ttl();
    }
    crit!("SharedMempool gc_task terminated");
}

async fn worker<'a>(
    mut from_master: Receiver<i64, WorkerState>,
    to_master: Sender<PeerId, PeerSyncUpdate>,
    peer_id: PeerId,
    mempool: &'a Arc<Mutex<CoreMempool>>,
    network_sender: &'a mut MempoolNetworkSender,
    timeline_id: u64,
    batch_size: usize,
) {
    debug!("[shared mempool] start new worker for {:?}", peer_id);
    let mut state = WorkerState::START;
    let current_sleep_duration_ms = 50; // TODO this is hard coded, get from config instead
    let timeout = Duration::from_secs(1); // TODO hardcoded, get from config instead
    let mut curr_timeline_id = timeline_id; // TODO check for valid timeline_id (e.g. no negative)

    while state != WorkerState::KILL {
        ::futures::select! {
            new_state = from_master.select_next_some() => {
                debug!("[shared mempool] got received new state for peer {:?}: {:?}", peer_id, new_state);
                state = new_state;
            },
            default => {}
            complete => {
                // channel `from_master` terminated, so should close
                // TODO log
                debug!("[shared mempool] channel from master terminated for worker {:?}", peer_id);
                state = WorkerState::KILL;
            }
        }
        if state == WorkerState::START {
            let to_master_clone = to_master.clone();

            debug!("[shared mempool] worker {:?} broadcasting txns", peer_id);
            let start_broadcast = Instant::now();
            let (result_timeline_id, result_state) = broadcast_transactions(
                mempool,
                network_sender,
                to_master_clone,
                curr_timeline_id,
                peer_id,
                current_sleep_duration_ms,
                batch_size,
                timeout,
            )
            .await;
            let end_broadcast = start_broadcast.elapsed().as_millis();
            debug!(
                "[shared mempool] worker {:?} finished broadcast in {:?} ms",
                peer_id, end_broadcast
            );
            state = result_state;
            curr_timeline_id = result_timeline_id;
        }
    }
}

async fn broadcast_transactions(
    mempool: &Arc<Mutex<CoreMempool>>,
    network_sender: &mut MempoolNetworkSender,
    mut to_master: Sender<PeerId, PeerSyncUpdate>,
    mut curr_timeline_id: u64,
    peer_id: PeerId,
    current_sleep_duration_ms: i32,
    batch_size: usize,
    timeout: Duration,
) -> (u64, WorkerState) {
    // broadcast transactions to peer

    let start_lock = Instant::now();
    let (transactions, new_timeline_id) = mempool
        .lock()
        .expect("[shared mempool] failed to acquire mempool lock")
        .read_timeline(curr_timeline_id, batch_size);
    let end_lock = start_lock.elapsed().as_millis();
    debug!(
        "[shared mempool] read_timeline for worker {:?} took {:?} ms",
        peer_id, end_lock
    );

    if !transactions.is_empty() {
        let mut req = BroadcastTransactionsRequest::default();
        req.peer_id = peer_id.into();
        req.transactions = transactions
            .into_iter()
            .map(|txn| txn.try_into().unwrap())
            .collect();

        debug!("[shared mempool] start RPC send to {:?}", peer_id);
        let start_broadcast = Instant::now();
        let result = network_sender
            .broadcast_transactions(peer_id.clone(), req, timeout)
            .await;
        let end_broadcast = start_broadcast.elapsed().as_millis();
        debug!(
            "[shared mempool] broadcast to {:?} took {:?} ms",
            peer_id, end_broadcast
        );
        match result {
            Ok(_res) => {
                // TODO update timeline_id/sleep from response
                // TODO log
                curr_timeline_id = new_timeline_id;
            }
            Err(_e) => {
                // TODO log and handle RPC error
            }
        }

        // send back timeline_id to master
        let start_master_push = Instant::now();
        if to_master
            .push(
                peer_id,
                PeerSyncUpdate {
                    peer_id,
                    timeline_id: curr_timeline_id,
                },
            )
            .is_err()
        {
            debug!(
                "[shared mempool] failed to send to master from {:?}",
                peer_id
            );
            // TODO log this
            // this will shut down channels to/from this worker thread, so master will know
            // this is dead and needs to start new worker for this peer
            return (curr_timeline_id, WorkerState::KILL);
        } else {
            // TODO log
        }
        let end_master_push = start_master_push.elapsed().as_millis();
        debug!(
            "[shared mempool] master push from {:?} took {:?} ms",
            peer_id, end_master_push,
        );
    } else {
        // TODO log
    }
    sleep(Duration::from_millis(
        current_sleep_duration_ms.try_into().unwrap(),
    ));
    (curr_timeline_id, WorkerState::START)
}

/// bootstrap of SharedMempool
/// creates separate Tokio Runtime that runs following routines:
///   - outbound_sync_task (task that periodically broadcasts transactions to peers)
///   - inbound_network_task (task that handles inbound mempool messages and network events)
///   - gc_task (task that performs GC of all expired transactions by SystemTTL)
pub(crate) fn start_shared_mempool<V>(
    config: &NodeConfig,
    mempool: Arc<Mutex<CoreMempool>>,
    network_sender: MempoolNetworkSender,
    network_events: MempoolNetworkEvents,
    storage_read_client: Arc<dyn StorageRead>,
    validator: Arc<V>,
    subscribers: Vec<UnboundedSender<SharedMempoolNotification>>,
) -> Runtime
where
    V: TransactionValidation + 'static,
{
    let runtime = Builder::new()
        .thread_name("shared-mem-")
        .threaded_scheduler()
        .enable_all()
        .build()
        .expect("[shared mempool] failed to create runtime");
    let executor = runtime.handle();

    // TODO deprecate this way of obtaining validator peers from config - dynamic config is coming
    let validator_peers;
    match &config.validator_network {
        Some(v) => {
            validator_peers = v.network_peers.peers.iter().map(|(key, _)| *key).collect();
        }
        None => {
            validator_peers = HashSet::new();
        }
    }

    let smp = SharedMempool {
        mempool: mempool.clone(),
        config: config.mempool.clone(),
        network_sender,
        storage_read_client,
        validator,
        validator_peers,
        subscribers,
    };

    executor.spawn(inbound_network_task(smp, executor.clone(), network_events));

    executor.spawn(gc_task(
        mempool,
        config.mempool.system_transaction_gc_interval_ms,
    ));

    runtime
}

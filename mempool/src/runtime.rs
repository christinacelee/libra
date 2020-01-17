// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    core_mempool::CoreMempool, mempool_service::MempoolService, proto::mempool,
    shared_mempool::start_shared_mempool,
};
use admission_control_proto::proto::admission_control::{
    SubmitTransactionRequest, SubmitTransactionResponse,
};
use anyhow::Result;
use futures::channel::{mpsc::Receiver, oneshot};
use libra_config::config::NodeConfig;
use network::validator_network::{MempoolNetworkEvents, MempoolNetworkSender};
use std::{
    collections::HashMap,
    net::ToSocketAddrs,
    sync::{Arc, Mutex},
};
use storage_client::{StorageRead, StorageReadServiceClient};
use tokio::runtime::{Builder, Runtime};
use vm_validator::vm_validator::VMValidator;

/// Markers for the types of networks mempool operates in
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum MempoolNetworkType {
    /// Validator network
    Validator,
    /// Full node network
    FullNode,
}

/// Handle for Mempool Runtime
pub struct MempoolRuntime {
    /// mempool service runtime
    pub mempool_service_rt: Runtime,
    /// separate shared mempool runtime
    pub shared_mempool: Runtime,
}

impl MempoolRuntime {
    /// setup Mempool runtime
    pub fn bootstrap(
        config: &NodeConfig,
        network_handles: HashMap<
            MempoolNetworkType,
            Vec<(MempoolNetworkSender, MempoolNetworkEvents)>,
        >,
        ac_endpoint_listener: Receiver<(
            SubmitTransactionRequest,
            oneshot::Sender<Result<SubmitTransactionResponse>>,
        )>,
    ) -> Self {
        let mempool_service_rt = Builder::new()
            .thread_name("mempool-service-")
            .threaded_scheduler()
            .enable_all()
            .build()
            .unwrap();
        let mempool = Arc::new(Mutex::new(CoreMempool::new(&config)));
        let mempool_service = MempoolService {
            core_mempool: Arc::clone(&mempool),
        };
        // setup shared mempool
        let storage_client: Arc<dyn StorageRead> = Arc::new(StorageReadServiceClient::new(
            "localhost",
            config.storage.port,
        ));
        let vm_validator = Arc::new(VMValidator::new(
            &config,
            Arc::clone(&storage_client),
            mempool_service_rt.handle().clone(),
        ));
        let shared_mempool = start_shared_mempool(
            config,
            mempool,
            network_handles,
            ac_endpoint_listener,
            storage_client,
            vm_validator,
            vec![],
            None,
        );

        let addr = format!(
            "{}:{}",
            config.mempool.address, config.mempool.mempool_service_port,
        )
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
        mempool_service_rt.spawn(
            tonic::transport::Server::builder()
                .add_service(mempool::mempool_server::MempoolServer::new(mempool_service))
                .serve(addr),
        );

        Self {
            mempool_service_rt,
            shared_mempool,
        }
    }
}

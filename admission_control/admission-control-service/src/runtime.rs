// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    admission_control_service::AdmissionControlService, upstream_proxy::process_network_messages,
};
use admission_control_proto::proto::admission_control::{
    admission_control_server::AdmissionControlServer, SubmitTransactionRequest,
    SubmitTransactionResponse,
};
use anyhow::Result;
use futures::channel::{mpsc, oneshot};
use grpcio::EnvBuilder;
use libra_config::config::NodeConfig;
use network::validator_network::AdmissionControlNetworkEvents;
use std::{collections::HashMap, net::ToSocketAddrs, sync::Arc};
use storage_client::{StorageRead, StorageReadServiceClient};
use tokio::runtime::{Builder, Runtime};

/// Handle for AdmissionControl Runtime
pub struct AdmissionControlRuntime {
    /// gRPC server to serve request between client and AC
    _ac_service_rt: Runtime,
    /// separate AC runtime
    _upstream_proxy: Runtime,
}

impl AdmissionControlRuntime {
    /// setup Admission Control runtime
    pub fn bootstrap(
        config: &NodeConfig,
        _network_events: Vec<AdmissionControlNetworkEvents>,
        ac_sender: mpsc::Sender<(
            SubmitTransactionRequest,
            oneshot::Sender<Result<SubmitTransactionResponse>>,
        )>,
    ) -> Self {
        let ac_service_rt = Builder::new()
            .thread_name("ac-service-")
            .threaded_scheduler()
            .enable_all()
            .build()
            .expect("[admission control] failed to create runtime");

        let port = config.admission_control.admission_control_service_port;

        // Create storage read client
        let storage_client: Arc<dyn StorageRead> = Arc::new(StorageReadServiceClient::new(
            Arc::new(EnvBuilder::new().name_prefix("grpc-ac-sto-").build()),
            "localhost",
            config.storage.port,
        ));

        let admission_control_service =
            AdmissionControlService::new(ac_sender, Arc::clone(&storage_client));

        let addr = format!("{}:{}", config.admission_control.address, port)
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        ac_service_rt.spawn(
            tonic::transport::Server::builder()
                .add_service(AdmissionControlServer::new(admission_control_service))
                .serve(addr),
        );

        let upstream_proxy_runtime = Builder::new()
            .thread_name("ac-upstream-proxy-")
            .threaded_scheduler()
            .enable_all()
            .build()
            .expect("[admission control] failed to create runtime");

        let upstream_peer_ids = &config.state_sync.upstream_peers.upstream_peers;
        let peer_info: HashMap<_, _> = upstream_peer_ids
            .iter()
            .map(|peer_id| (*peer_id, true))
            .collect();

        let executor = upstream_proxy_runtime.handle();

        executor.spawn(process_network_messages(_network_events, peer_info));

        Self {
            _ac_service_rt: ac_service_rt,
            _upstream_proxy: upstream_proxy_runtime,
        }
    }
}

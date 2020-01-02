// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::admission_control_service::AdmissionControlService;
use futures::channel::mpsc::{channel, unbounded};
use libra_config::config::NodeConfig;
use libra_mempool::{start_shared_mempool, MempoolTrait, TimelineState};
use libra_mempool_shared_proto::{
    proto::mempool_status::MempoolAddTransactionStatusCode, MempoolAddTransactionStatus,
};
use libra_types::{account_address::ADDRESS_LENGTH, transaction::SignedTransaction};
use network::{
    interface::{NetworkNotification, NetworkRequest},
    validator_network::{MempoolNetworkEvents, MempoolNetworkSender},
};
use std::sync::{Arc, Mutex};
use storage_service::mocks::mock_storage_client::MockStorageReadClient;
use tokio::runtime::Runtime;
use vm_validator::mocks::mock_vm_validator::MockVMValidator;

pub(crate) struct CoupledAcSmp {
    pub ac_service: AdmissionControlService,
    _smp: Runtime,
    _smp_network_reqs_rx: channel::Receiver<NetworkRequest>,
    _smp_network_notifs_tx: channel::Sender<NetworkNotification>,
}

pub(crate) struct MockMempool {}

impl MempoolTrait for MockMempool {
    fn add_txn(
        &mut self,
        txn: SignedTransaction,
        _gas_amount: u64,
        _db_sequence_number: u64,
        _balance: u64,
        _timeline_state: TimelineState,
    ) -> MempoolAddTransactionStatus {
        let insufficient_balance_add = [100_u8; ADDRESS_LENGTH];
        let invalid_seq_add = [101_u8; ADDRESS_LENGTH];
        let sys_error_add = [102_u8; ADDRESS_LENGTH];
        let accepted_add = [103_u8; ADDRESS_LENGTH];
        let mempool_full = [104_u8; ADDRESS_LENGTH];

        let sender = txn.sender();
        let code;
        if sender.as_ref() == insufficient_balance_add {
            code = MempoolAddTransactionStatusCode::InsufficientBalance;
        } else if sender.as_ref() == invalid_seq_add {
            code = MempoolAddTransactionStatusCode::InvalidSeqNumber;
        } else if sender.as_ref() == sys_error_add {
            code = MempoolAddTransactionStatusCode::InvalidUpdate;
        } else if sender.as_ref() == accepted_add {
            code = MempoolAddTransactionStatusCode::Valid;
        } else if sender.as_ref() == mempool_full {
            code = MempoolAddTransactionStatusCode::MempoolIsFull;
        } else {
            code = MempoolAddTransactionStatusCode::Valid;
        }

        MempoolAddTransactionStatus::new(code, "".to_string())
    }

    fn read_timeline(&mut self, _timeline_id: u64, _count: usize) -> (Vec<SignedTransaction>, u64) {
        (vec![], 0)
    }

    fn gc_by_system_ttl(&mut self) {}
}

impl CoupledAcSmp {
    pub(crate) fn bootstrap() -> Self {
        let config = NodeConfig::random();

        // shared mempool
        let mempool = Arc::new(Mutex::new(MockMempool {}));
        let (smp_network_reqs_tx, _smp_network_reqs_rx) = channel::new_test(8);
        let (_smp_network_notifs_tx, smp_network_notifs_rx) = channel::new_test(8);
        let smp_network_sender = MempoolNetworkSender::new(smp_network_reqs_tx);
        let smp_network_events = MempoolNetworkEvents::new(smp_network_notifs_rx);
        let (smp_sender, _smp_subscriber) = unbounded();
        let (ac_sender, smp_receiver) = channel(1_024);

        let _smp = start_shared_mempool(
            &config,
            mempool,
            smp_network_sender,
            vec![smp_network_events],
            smp_receiver,
            Arc::new(MockStorageReadClient),
            Arc::new(MockVMValidator),
            vec![smp_sender],
        );

        let ac_service = AdmissionControlService::new(ac_sender, Arc::new(MockStorageReadClient));

        Self {
            ac_service,
            _smp,
            _smp_network_reqs_rx,
            _smp_network_notifs_tx,
        }
    }
}

// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::mocks::coupled_ac_smp::CoupledAcSmp;
use admission_control_proto::proto::admission_control::{
    admission_control_server::AdmissionControl, SubmitTransactionRequest,
};
use futures::executor::block_on;
use libra_proptest_helpers::ValueGenerator;
use libra_prost_ext::MessageExt;
use libra_types::transaction::SignedTransaction;
use proptest;
use prost::Message;
use tonic::Request;

#[test]
fn test_fuzzer() {
    let mut gen = ValueGenerator::new();
    let data = generate_corpus(&mut gen);
    fuzzer(&data);
}

/// generate_corpus produces an arbitrary SubmitTransactionRequest for admission control
pub fn generate_corpus(gen: &mut ValueGenerator) -> Vec<u8> {
    // use proptest to generate a SignedTransaction
    let signed_txn = gen.generate(proptest::arbitrary::any::<SignedTransaction>());
    // wrap it in a SubmitTransactionRequest
    let mut req = SubmitTransactionRequest::default();
    req.transaction = Some(signed_txn.into());

    req.to_vec().unwrap()
}

/// fuzzer takes a serialized SubmitTransactionRequest an process it with an admission control
/// service
pub fn fuzzer(data: &[u8]) {
    // parse SubmitTransactionRequest
    let req = match SubmitTransactionRequest::decode(data) {
        Ok(value) => value,
        Err(_) => {
            if cfg!(test) {
                panic!();
            }
            return;
        }
    };

    let ac_smp = CoupledAcSmp::bootstrap();

    // process the request
    let res = block_on(ac_smp.ac_service.submit_transaction(Request::new(req)));
    if cfg!(test) && res.is_err() {
        panic!();
    }
}

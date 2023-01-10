/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;
use serde_json;

use crate::internals::urns;
use crate::proto::fn_execution::v1::{ProcessBundleDescriptor, RemoteGrpcPort};
use crate::proto::pipeline::v1::PTransform;

use crate::worker::data::MultiplexingDataChannel;
use crate::worker::sdk_worker::BundleProcessor;
use crate::worker::test_utils::RECORDING_OPERATOR_LOGS;

type OperatorMap = HashMap<&'static str, OperatorDiscriminants>;

static OPERATORS_BY_URN: Lazy<Mutex<OperatorMap>> = Lazy::new(|| {
    // TODO: these will have to be parameterized depending on things such as the runner used
    let m: OperatorMap = HashMap::from([
        // Test operators
        (urns::CREATE_URN, OperatorDiscriminants::Create),
        (urns::RECORDING_URN, OperatorDiscriminants::Recording),
        (urns::PARTITION_URN, OperatorDiscriminants::Partitioning),
        // Production operators
        (urns::DATA_INPUT_URN, OperatorDiscriminants::DataSource),
    ]);

    Mutex::new(m)
});

pub trait OperatorI {
    fn new(
        transform_id: Arc<String>,
        transform: Arc<PTransform>,
        context: Arc<OperatorContext>,
        operator_discriminant: OperatorDiscriminants,
    ) -> Self
    where
        Self: Sized;

    fn start_bundle(&self);

    fn process(&self, value: WindowedValue);

    fn finish_bundle(&self) {
        todo!()
    }
}

#[derive(fmt::Debug, EnumDiscriminants)]
pub enum Operator {
    // Test operators
    Create(CreateOperator),
    Recording(RecordingOperator),
    Partitioning,

    // Production operators
    DataSource,
}

impl OperatorI for Operator {
    fn new(
        transform_id: Arc<String>,
        transform: Arc<PTransform>,
        context: Arc<OperatorContext>,
        operator_discriminant: OperatorDiscriminants,
    ) -> Self {
        match operator_discriminant {
            OperatorDiscriminants::Create => Operator::Create(CreateOperator::new(
                transform_id,
                transform,
                context,
                operator_discriminant,
            )),
            _ => todo!(),
        }
    }

    fn start_bundle(&self) {
        match self {
            Operator::Create(create_op) => create_op.start_bundle(),
            Operator::Recording(recording_op) => recording_op.start_bundle(),
            _ => todo!(),
        };
    }

    fn process(&self, value: WindowedValue) {
        match self {
            Operator::Create(create_op) => {
                create_op.process(value);
            }
            Operator::Recording(recording_op) => {
                recording_op.process(value);
            }
            _ => todo!(),
        };
    }

    fn finish_bundle(&self) {
        match self {
            Operator::Create(create_op) => create_op.finish_bundle(),
            Operator::Recording(recording_op) => recording_op.finish_bundle(),
            _ => todo!(),
        };
    }
}

pub fn create_operator(transform_id: &str, context: Arc<OperatorContext>) -> Operator {
    let descriptor: &ProcessBundleDescriptor = context.descriptor.as_ref();

    let transform = descriptor
        .transforms
        .get(transform_id)
        .expect("Transform ID not found");

    for pcoll_id in transform.outputs.values() {
        (context.get_receiver)(context.bundle_processor.clone(), pcoll_id.clone());
    }

    let operators_by_urn = OPERATORS_BY_URN.lock().unwrap();

    let spec = transform
        .spec
        .as_ref()
        .unwrap_or_else(|| panic!("Transform {} has no spec", transform_id));

    let op_discriminant = operators_by_urn
        .get(spec.urn.as_str())
        .unwrap_or_else(|| panic!("Unknown transform type: {}", spec.urn));

    match op_discriminant {
        OperatorDiscriminants::Create => Operator::Create(CreateOperator::new(
            Arc::new(transform_id.to_string()),
            Arc::new(transform.clone()),
            context.clone(),
            OperatorDiscriminants::Create,
        )),
        OperatorDiscriminants::Recording => Operator::Recording(RecordingOperator::new(
            Arc::new(transform_id.to_string()),
            Arc::new(transform.clone()),
            context.clone(),
            OperatorDiscriminants::Recording,
        )),
        _ => todo!(),
    }
}

#[derive(Debug)]
pub struct Receiver {
    operators: Vec<Arc<Operator>>,
}

impl Receiver {
    pub fn new(operators: Vec<Arc<Operator>>) -> Self {
        Receiver { operators }
    }

    pub fn receive(&self, value: WindowedValue) {
        for op in &self.operators {
            op.process(value.clone());
        }
    }
}

pub struct OperatorContext {
    pub descriptor: Arc<ProcessBundleDescriptor>,
    pub get_receiver: Box<dyn Fn(Arc<BundleProcessor>, String) -> Arc<Receiver> + Send + Sync>,
    // get_data_channel: fn(&str) -> MultiplexingDataChannel,
    // get_bundle_id: String,
    pub bundle_processor: Arc<BundleProcessor>,
}

impl fmt::Debug for OperatorContext {
    fn fmt<'a>(&'a self, o: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        o.debug_struct("OperatorContext")
            .field("descriptor", &self.descriptor)
            .field("bundle_processor", &self.bundle_processor)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum WindowedValue {
    Null,
    Array(Vec<Arc<WindowedValue>>),
    String(String),
    // Bool(bool),
    // Number(Number),
    // Object(Map<String, Value>),
}

// ******* Operator definitions *******

#[derive(Debug)]
pub struct CreateOperator {
    transform_id: Arc<String>,
    transform: Arc<PTransform>,
    context: Arc<OperatorContext>,
    operator_discriminant: OperatorDiscriminants,

    receivers: Vec<Arc<Receiver>>,
    data: WindowedValue,
}

impl OperatorI for CreateOperator {
    fn new(
        transform_id: Arc<String>,
        transform: Arc<PTransform>,
        context: Arc<OperatorContext>,
        operator_discriminant: OperatorDiscriminants,
    ) -> Self {
        let payload = transform
            .as_ref()
            .spec
            .as_ref()
            .expect("No spec found for transform")
            .payload
            .clone();

        let decoded: Vec<String> = serde_json::from_slice(&payload).unwrap();
        let mut vals: Vec<Arc<WindowedValue>> = Vec::new();
        for d in decoded.iter() {
            vals.push(Arc::new(WindowedValue::String(d.clone())));
        }
        let data = WindowedValue::Array(vals);

        let receivers = transform
            .outputs
            .values()
            .map(|pcollection_id: &String| {
                let bp = context.bundle_processor.clone();
                (context.get_receiver)(bp, pcollection_id.clone())
            })
            .collect();

        Self {
            transform_id,
            transform,
            context,
            operator_discriminant,
            receivers,
            data,
        }
    }

    fn start_bundle(&self) {
        //TODO: make WindowedValue iterable and refactor this
        if let WindowedValue::Array(v) = &self.data {
            for wv in v {
                for rec in self.receivers.iter() {
                    rec.receive(wv.as_ref().clone());
                }
            }
        } else {
            for rec in self.receivers.iter() {
                rec.receive(self.data.clone());
            }
        }
    }

    fn process(&self, value: WindowedValue) {
        ()
    }

    fn finish_bundle(&self) {
        ()
    }
}

#[derive(Debug)]
pub struct RecordingOperator {
    transform_id: Arc<String>,
    transform: Arc<PTransform>,
    context: Arc<OperatorContext>,
    operator_discriminant: OperatorDiscriminants,

    receivers: Vec<Arc<Receiver>>,
}

impl OperatorI for RecordingOperator {
    fn new(
        transform_id: Arc<String>,
        transform: Arc<PTransform>,
        context: Arc<OperatorContext>,
        operator_discriminant: OperatorDiscriminants,
    ) -> Self {
        let receivers = transform
            .outputs
            .values()
            .map(|pcollection_id: &String| {
                let bp = context.bundle_processor.clone();
                (context.get_receiver)(bp, pcollection_id.clone())
            })
            .collect();

        Self {
            transform_id,
            transform,
            context,
            operator_discriminant: operator_discriminant,
            receivers,
        }
    }

    fn start_bundle(&self) {
        unsafe {
            let mut log = RECORDING_OPERATOR_LOGS.lock().unwrap();
            log.push(format!("{}.start_bundle()", self.transform_id));
        }
    }

    fn process(&self, value: WindowedValue) {
        unsafe {
            let mut log = RECORDING_OPERATOR_LOGS.lock().unwrap();
            log.push(format!("{}.process({:?})", self.transform_id, value));
        }

        for rec in self.receivers.iter() {
            rec.receive(value.clone());
        }
    }

    fn finish_bundle(&self) {
        unsafe {
            let mut log = RECORDING_OPERATOR_LOGS.lock().unwrap();
            log.push(format!("{}.finish_bundle()", self.transform_id));
        }
    }
}

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

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;
use tonic::codegen::InterceptedService;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Uri};
use tonic::Status;

use crate::proto::beam_api::fn_execution::{
    beam_fn_control_client::BeamFnControlClient, FinalizeBundleRequest,
    GetProcessBundleDescriptorRequest, HarnessMonitoringInfosRequest, InstructionRequest,
    InstructionResponse, MonitoringInfosMetadataRequest, ProcessBundleDescriptor,
    ProcessBundleProgressRequest, ProcessBundleRequest, ProcessBundleResponse,
    ProcessBundleSplitRequest, ProcessBundleSplitResponse, RegisterRequest, RegisterResponse,
};
use crate::proto::beam_api::fn_execution::{instruction_request, instruction_response};
use crate::proto::beam_api::pipeline::PTransform;

use crate::worker::operators::{create_operator, Operator, OperatorContext, OperatorI, Receiver};

#[derive(Clone)]
struct WorkerIdInterceptor {
    id: MetadataValue<Ascii>,
}

impl WorkerIdInterceptor {
    fn new(id: String) -> Self {
        Self {
            id: id.parse().unwrap(),
        }
    }
}

impl Interceptor for WorkerIdInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        request.metadata_mut().insert("worker_id", self.id.clone());
        Ok(request)
    }
}

type BundleDescriptorId = String;
type InstructionId = String;

// TODO(sjvanrossum): Convert simple map caches to concurrent caches.
// Using concurrent caches removes the need to synchronize on the worker instance in every context.
#[derive(Debug)]
pub struct Worker {
    // Cheap and safe to clone
    control_client: BeamFnControlClient<InterceptedService<Channel, WorkerIdInterceptor>>,
    // Cheap and safe to clone
    control_tx: mpsc::Sender<InstructionResponse>,
    control_rx: Arc<TokioMutex<mpsc::Receiver<InstructionResponse>>>,
    // Cheap and safe to clone
    process_bundle_descriptors:
        moka::future::Cache<BundleDescriptorId, Arc<ProcessBundleDescriptor>>,
    bundle_processors: HashMap<String, BundleProcessor>,
    active_bundle_processors: HashMap<String, BundleProcessor>,
    id: String,
    endpoints: WorkerEndpoints,
    options: HashMap<String, String>,
}

impl Worker {
    // concurrent data structures and/or finer grained locks.
    pub async fn new(id: String, endpoints: WorkerEndpoints) -> Self {
        // TODO: parse URIs in the endpoint struct
        let channel = Channel::builder(endpoints.get_endpoint().parse::<Uri>().unwrap())
            .connect()
            .await
            .expect("Failed to connect to control service");
        let client =
            BeamFnControlClient::with_interceptor(channel, WorkerIdInterceptor::new(id.clone()));
        let (tx, rx) = mpsc::channel::<InstructionResponse>(100);

        Self {
            control_client: client,
            control_tx: tx,
            control_rx: Arc::new(TokioMutex::new(rx)),
            // TODO(sjvanrossum): Maybe define the eviction policy
            process_bundle_descriptors: moka::future::Cache::builder().build(),
            bundle_processors: HashMap::new(),
            active_bundle_processors: HashMap::new(),
            id,
            endpoints,
            options: HashMap::new(),
        }
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let rx = self.control_rx.clone();
        let outbound = async_stream::stream! {
            while let Some(control_res) = rx.lock().await.recv().await {
                yield control_res
            }
        };

        let response = self.control_client.control(outbound).await?;
        let mut inbound = response.into_inner();

        while let Some(control_req) = inbound.message().await? {
            match control_req.request {
                Some(instruction_request::Request::ProcessBundle(instr_req)) => {
                    self.process_bundle(control_req.instruction_id, instr_req);
                }
                Some(instruction_request::Request::ProcessBundleProgress(instr_req)) => {
                    self.process_bundle_progress(control_req.instruction_id, instr_req);
                }
                Some(instruction_request::Request::ProcessBundleSplit(instr_req)) => {
                    self.process_bundle_split(control_req.instruction_id, instr_req);
                }
                Some(instruction_request::Request::FinalizeBundle(instr_req)) => {
                    self.finalize_bundle(control_req.instruction_id, instr_req);
                }
                Some(instruction_request::Request::MonitoringInfos(instr_req)) => {
                    self.monitoring_infos(control_req.instruction_id, instr_req);
                }
                Some(instruction_request::Request::HarnessMonitoringInfos(instr_req)) => {
                    self.harness_monitoring_infos(control_req.instruction_id, instr_req);
                }
                Some(instruction_request::Request::Register(instr_req)) => {
                    self.register(control_req.instruction_id, instr_req);
                }
                _ => {
                    self.fail(
                        control_req.instruction_id.clone(),
                        format!("Unexpected request: {:?}", control_req),
                        self.control_tx.clone(),
                    )
                    .await?;
                }
            };
        }
        Ok(())
    }

    pub async fn stop(&self) {
        self.control_rx.lock().await.close()
    }

    fn process_bundle(&self, instruction_id: InstructionId, request: ProcessBundleRequest) -> () {
        let mut client = self.control_client.clone();
        let descriptor_cache = self.process_bundle_descriptors.clone();
        tokio::spawn(async move {
            let descriptor = descriptor_cache
                .try_get_with::<_, Status>(request.process_bundle_descriptor_id.clone(), async {
                    let res = client
                        .get_process_bundle_descriptor(GetProcessBundleDescriptorRequest {
                            process_bundle_descriptor_id: request
                                .process_bundle_descriptor_id
                                .clone(),
                        })
                        .await?;
                    Ok(Arc::new(res.into_inner()))
                })
                .await
                .unwrap();
            // TODO(sjvanrossum): Fetch bundle processor and process bundle
        });
    }

    fn process_bundle_progress(
        &self,
        instruction_id: InstructionId,
        request: ProcessBundleProgressRequest,
    ) -> () {
        // TODO(sjvanrossum): Flesh out after process_bundle is sufficiently implemented
    }

    fn process_bundle_split(
        &self,
        instruction_id: InstructionId,
        request: ProcessBundleSplitRequest,
    ) -> () {
        // TODO(sjvanrossum): Flesh out after process_bundle is sufficiently implemented
    }

    fn finalize_bundle(&self, instruction_id: InstructionId, request: FinalizeBundleRequest) -> () {
        // TODO(sjvanrossum): Flesh out after process_bundle is sufficiently implemented.
    }

    fn monitoring_infos(
        &self,
        instruction_id: InstructionId,
        request: MonitoringInfosMetadataRequest,
    ) -> () {
        // TODO: Implement
    }

    fn harness_monitoring_infos(
        &self,
        instruction_id: InstructionId,
        request: HarnessMonitoringInfosRequest,
    ) -> () {
        // TODO: Implement
    }

    fn register(&self, instruction_id: InstructionId, request: RegisterRequest) -> () {
        let descriptor_cache = self.process_bundle_descriptors.clone();
        let tx = self.control_tx.clone();
        tokio::spawn(async move {
            for descriptor in request.process_bundle_descriptor {
                descriptor_cache
                    .insert(descriptor.id.clone(), Arc::new(descriptor))
                    .await;
            }

            tx.send(InstructionResponse {
                instruction_id,
                error: String::default(),
                response: Some(instruction_response::Response::Register(
                    RegisterResponse::default(),
                )),
            })
            .await
            .unwrap()
        });
    }

    async fn fail(
        &self,
        instruction_id: InstructionId,
        error: String,
        tx: mpsc::Sender<InstructionResponse>,
    ) -> Result<(), mpsc::error::SendError<InstructionResponse>> {
        tx.send(InstructionResponse {
            instruction_id: instruction_id,
            error: error,
            response: None,
        })
        .await
    }
}

#[derive(Clone, Debug)]
pub struct WorkerEndpoints {
    control_endpoint_url: Option<String>,
}

impl WorkerEndpoints {
    pub fn new(control_endpoint_url: Option<String>) -> Self {
        Self {
            control_endpoint_url,
        }
    }

    pub fn get_endpoint(&self) -> &str {
        self.control_endpoint_url.as_ref().unwrap()
    }
}

#[derive(Debug)]
pub struct BundleProcessor {
    descriptor: Arc<ProcessBundleDescriptor>,
    creation_ordered_operators: Mutex<Vec<Arc<Operator>>>,
    topologically_ordered_operators: RwLock<Vec<Arc<Operator>>>,

    operators: RwLock<HashMap<String, Arc<Operator>>>,
    receivers: RwLock<HashMap<String, Arc<Receiver>>>,
    consumers: RwLock<HashMap<String, Vec<String>>>,

    current_bundle_id: Mutex<Option<String>>,
}

impl BundleProcessor {
    pub fn new(descriptor: Arc<ProcessBundleDescriptor>, root_urns: &[&'static str]) -> Arc<Self> {
        let mut consumers: HashMap<String, Vec<String>> = HashMap::new();
        for (transform_id, ptransform) in descriptor.transforms.iter() {
            if is_primitive(ptransform) {
                ptransform
                    .inputs
                    .values()
                    .for_each(|pcollection_id: &String| {
                        if consumers.get(pcollection_id).is_none() {
                            consumers.insert(pcollection_id.clone(), Vec::new());
                        }

                        consumers
                            .get_mut(pcollection_id)
                            .unwrap()
                            .push(transform_id.clone());
                    })
            }
        }

        let mut _instance = Arc::new(Self {
            descriptor: descriptor.clone(),
            creation_ordered_operators: Mutex::new(Vec::new()),
            topologically_ordered_operators: RwLock::new(Vec::new()),
            operators: RwLock::new(HashMap::new()),
            receivers: RwLock::new(HashMap::new()),
            consumers: RwLock::new(consumers),
            current_bundle_id: Mutex::new(None),
        });

        descriptor
            .transforms
            .iter()
            .for_each(|(transform_id, transform)| {
                if let Some(spec) = &transform.spec {
                    if root_urns.contains(&spec.urn.as_str()) {
                        BundleProcessor::get_operator(_instance.clone(), transform_id.clone());
                    }
                }
            });

        _instance.create_topologically_ordered_operators();

        _instance
    }

    // TODO: the bundle processors are being be passed around by value as parameters
    // to avoid closures with temp references in an async context. However, this
    // should be revisited later.
    fn get_receiver(
        bundle_processor: Arc<BundleProcessor>,
        pcollection_id: String,
    ) -> Arc<Receiver> {
        let receivers = bundle_processor.receivers.read().unwrap();

        let receiver = match receivers.get(&pcollection_id) {
            Some(rec) => rec.clone(),
            None => {
                let consumers = bundle_processor.consumers.read().unwrap();

                let pcoll_operators: Vec<Arc<Operator>> = match consumers.get(&pcollection_id) {
                    Some(v) => {
                        drop(receivers);

                        let mut oprs = Vec::new();
                        for transform_id in v {
                            let cl = bundle_processor.clone();
                            oprs.push(BundleProcessor::get_operator(cl, transform_id.clone()))
                        }
                        oprs
                    }
                    None => Vec::new(),
                };

                let receiver = Receiver::new(pcoll_operators);

                let mut receivers = bundle_processor.receivers.write().unwrap();
                receivers.insert(pcollection_id.clone(), Arc::new(receiver));
                receivers[&pcollection_id].clone()
            }
        };

        receiver
    }

    fn get_operator(bundle_processor: Arc<BundleProcessor>, transform_id: String) -> Arc<Operator> {
        let get_receiver = |b: Arc<BundleProcessor>, pcollection_id: String| {
            BundleProcessor::get_receiver(b, pcollection_id)
        };

        let operators = bundle_processor.operators.read().unwrap();

        let operator_opt = operators.get(&transform_id);

        if operator_opt.is_some() {
            operator_opt.unwrap().clone()
        } else {
            let descriptor = bundle_processor.descriptor.clone();
            drop(operators);

            let op = create_operator(
                &transform_id,
                Arc::new(OperatorContext {
                    descriptor,
                    get_receiver: Box::new(get_receiver),

                    bundle_processor: bundle_processor.clone(),
                }),
            );

            let mut operators = bundle_processor.operators.write().unwrap();
            operators.insert(transform_id.clone(), Arc::new(op));

            let mut creation_ordered_operators =
                bundle_processor.creation_ordered_operators.lock().unwrap();
            creation_ordered_operators.push(operators[&transform_id].clone());

            operators[&transform_id].clone()
        }
    }

    fn create_topologically_ordered_operators(&self) {
        let creation_ordered = self.creation_ordered_operators.lock().unwrap();

        let mut instance_operators = self.topologically_ordered_operators.write().unwrap();

        creation_ordered
            .iter()
            .rev()
            .for_each(|op: &Arc<Operator>| {
                instance_operators.push(op.clone());
            });
    }

    pub async fn process(&self, instruction_id: String) {
        let mut current_bundle_id = self.current_bundle_id.lock().unwrap();
        current_bundle_id.replace(instruction_id);
        drop(current_bundle_id);

        let topologically_ordered_operators = self.topologically_ordered_operators.read().unwrap();
        for o in topologically_ordered_operators.iter().rev() {
            o.start_bundle();
        }

        for o in topologically_ordered_operators.iter() {
            o.finish_bundle();
        }
    }
}

// TODO
fn is_primitive(transform: &PTransform) -> bool {
    true
}

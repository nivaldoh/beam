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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::proto::{
    fn_execution_v1,
    fn_execution_v1::beam_fn_external_worker_pool_server as beam_fn_external_worker_pool_server_v1,
};
use crate::worker::sdk_worker::{Worker, WorkerEndpoints};

#[derive(Debug)]
struct BeamFnExternalWorkerPoolV1Service {
    workers: Arc<RwLock<HashMap<String, Arc<Mutex<Worker>>>>>,
}

#[tonic::async_trait]
impl beam_fn_external_worker_pool_server_v1::BeamFnExternalWorkerPool
    for BeamFnExternalWorkerPoolV1Service
{
    async fn start_worker(
        &self,
        request: Request<fn_execution_v1::StartWorkerRequest>,
    ) -> Result<Response<fn_execution_v1::StartWorkerResponse>, Status> {
        let req = request.into_inner();

        // Avoid creating duplicate workers
        if let Entry::Vacant(entry) = self.workers.write().await.entry(req.worker_id.clone()) {
            let worker = Arc::new(Mutex::new(
                Worker::new(
                    entry.key().clone(),
                    WorkerEndpoints::new(req.control_endpoint.map(|descriptor| descriptor.url)),
                )
                .await,
            ));
            let inserted_worker = entry.insert(worker).clone();

            tokio::spawn(async move {
                inserted_worker.lock().await.start().await.unwrap();
            });
        }

        Ok(Response::new(
            fn_execution_v1::StartWorkerResponse::default(),
        ))
    }

    async fn stop_worker(
        &self,
        request: Request<fn_execution_v1::StopWorkerRequest>,
    ) -> Result<Response<fn_execution_v1::StopWorkerResponse>, Status> {
        if let Some(worker) = self
            .workers
            .write()
            .await
            .remove(&request.into_inner().worker_id)
        {
            worker.lock().await.stop().await;
        }

        Ok(Response::new(fn_execution_v1::StopWorkerResponse::default()))
    }
}

pub struct ExternalWorkerPool {
    address: SocketAddr,
    cancellation_token: CancellationToken,
}

impl ExternalWorkerPool {
    pub fn new(ip: &str, port: u16) -> Self {
        let parsed_ip = ip.parse().expect("Invalid IP address");

        Self {
            address: SocketAddr::new(parsed_ip, port),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: add logging
        println!("Starting loopback workers at {}", self.address);

        let svc = beam_fn_external_worker_pool_server_v1::BeamFnExternalWorkerPoolServer::new(
            BeamFnExternalWorkerPoolV1Service {
                workers: Arc::new(RwLock::new(HashMap::new())),
            },
        );

        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(self.address, self.cancellation_token.cancelled())
            .await?;

        Ok(())
    }

    // TODO: implement timeout for graceful shutdown
    pub async fn stop(&self, timeout: Duration) {
        // TODO: add logging
        println!("Shutting down external workers.");

        self.cancellation_token.cancel();
    }
}

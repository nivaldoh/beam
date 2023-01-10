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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &[
            "org/apache/beam/model/fn_execution/v1/beam_fn_api.proto",
            "org/apache/beam/model/fn_execution/v1/beam_provision_api.proto",
            "org/apache/beam/model/interactive/v1/beam_interactive_api.proto",
            "org/apache/beam/model/job_management/v1/beam_artifact_api.proto",
            "org/apache/beam/model/job_management/v1/beam_expansion_api.proto",
            "org/apache/beam/model/job_management/v1/beam_job_api.proto",
            "org/apache/beam/model/pipeline/v1/beam_runner_api.proto",
            "org/apache/beam/model/pipeline/v1/endpoints.proto",
            "org/apache/beam/model/pipeline/v1/external_transforms.proto",
            "org/apache/beam/model/pipeline/v1/metrics.proto",
            "org/apache/beam/model/pipeline/v1/schema.proto",
            "org/apache/beam/model/pipeline/v1/standard_window_fns.proto",
        ],
        &[
            "../../model/fn-execution/src/main/proto",
            "../../model/interactive/src/main/proto",
            "../../model/job-management/src/main/proto",
            "../../model/pipeline/src/main/proto",
        ],
    )?;
    Ok(())
}

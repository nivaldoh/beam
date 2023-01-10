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

pub mod expansion {
    pub mod v1 {
        tonic::include_proto!("org.apache.beam.model.expansion.v1");
    }
}
pub mod fn_execution {
    pub mod v1 {
        tonic::include_proto!("org.apache.beam.model.fn_execution.v1");
    }
}
pub mod interactive {
    pub mod v1 {
        tonic::include_proto!("org.apache.beam.model.interactive.v1");
    }
}
pub mod job_management {
    pub mod v1 {
        tonic::include_proto!("org.apache.beam.model.job_management.v1");
    }
}
pub mod pipeline {
    pub mod v1 {
        tonic::include_proto!("org.apache.beam.model.pipeline.v1");
    }
}

pub(crate) use expansion::v1 as expansion_v1;
pub(crate) use fn_execution::v1 as fn_execution_v1;
pub(crate) use interactive::v1 as interactive_v1;
pub(crate) use job_management::v1 as job_management_v1;
pub(crate) use pipeline::v1 as pipeline_v1;

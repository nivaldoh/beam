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

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::coders::coders::CoderI;
use crate::proto::pipeline::v1 as pipeline_v1;

use crate::internals::pvalue::{flatten_pvalue, PTransform, PValue};

const _CODER_ID_PREFIX: &str = "coder_";

pub struct PipelineContext {
    component_prefix: String,
    counter: Arc<Mutex<usize>>,
}

impl PipelineContext {
    pub fn new(component_prefix: String) -> Self {
        Self {
            component_prefix,
            counter: Arc::new(Mutex::new(0)),
        }
    }

    pub fn create_unique_name(&self, prefix: String) -> String {
        format!(
            "{}{}_{}",
            self.component_prefix,
            prefix,
            self.get_and_increment_counter()
        )
    }

    fn get_and_increment_counter(&self) -> usize {
        let mut counter = self.counter.lock().unwrap();
        *counter += 1;

        *counter - 1
    }
}

// TODO: move coders to PipelineContext
pub struct Pipeline {
    context: PipelineContext,
    default_environment: String,
    proto: Arc<Mutex<pipeline_v1::Pipeline>>,
    transform_stack: Arc<Mutex<Vec<String>>>,
    used_stage_names: Arc<Mutex<HashSet<String>>>,

    // TODO: stop using TypeId as a key
    // TODO: use something better than Any (maybe use CoderI with an "as_any" method)
    coders: Mutex<HashMap<TypeId, Box<dyn Any + Send>>>,

    coder_proto_counter: Mutex<usize>,
}

impl<'a> Pipeline {
    pub fn new(component_prefix: String) -> Self {
        let proto = pipeline_v1::Pipeline {
            components: Some(pipeline_v1::Components {
                transforms: HashMap::with_capacity(0),
                pcollections: HashMap::with_capacity(0),
                windowing_strategies: HashMap::with_capacity(0),
                coders: HashMap::with_capacity(0),
                environments: HashMap::with_capacity(0),
            }),
            root_transform_ids: Vec::with_capacity(0),
            display_data: Vec::with_capacity(0),
            requirements: Vec::with_capacity(0),
        };

        Pipeline {
            context: PipelineContext::new(component_prefix.clone()),
            default_environment: format!("{}rustEnvironment", component_prefix),
            proto: Arc::new(Mutex::new(proto)),
            transform_stack: Arc::new(Mutex::new(Vec::with_capacity(0))),
            used_stage_names: Arc::new(Mutex::new(HashSet::with_capacity(0))),

            coders: Mutex::new(HashMap::new()),
            coder_proto_counter: Mutex::new(0),
        }
    }

    pub fn get_proto(&self) -> Arc<std::sync::Mutex<pipeline_v1::Pipeline>> {
        self.proto.clone()
    }

    pub fn get_coder<C: CoderI<E> + Clone + 'static, E>(&self, coder_type: &TypeId) -> C {
        let pipeline_coders = self.coders.lock().unwrap();

        let coder = pipeline_coders.get(coder_type).unwrap();
        coder.downcast_ref::<C>().unwrap().clone()
    }

    pub fn register_coder<C: CoderI<E> + 'a, E>(&self, coder: Box<dyn Any + Send + 'a>) -> TypeId {
        let mut coders = self.coders.lock().unwrap();
        let concrete_coder = coder.downcast_ref::<C>().unwrap();
        let concrete_coder_type_id = concrete_coder.type_id();

        for registered_type_id in coders.keys() {
            if *registered_type_id == concrete_coder_type_id {
                return *registered_type_id;
            }
        }

        coders.insert(concrete_coder_type_id, coder);

        concrete_coder_type_id
    }

    // TODO: review need for separate function vs register_coder
    pub fn register_coder_proto(&self, coder_proto: pipeline_v1::Coder) -> String {
        let mut pipeline_proto = self.proto.lock().unwrap();

        let proto_coders = &mut pipeline_proto.components.as_mut().unwrap().coders;

        for (coder_id, coder) in proto_coders.iter() {
            if *coder == coder_proto {
                return coder_id.clone();
            }
        }

        let mut coder_counter = self.coder_proto_counter.lock().unwrap();
        *coder_counter += 1;
        let new_coder_id = format!("{}{}", _CODER_ID_PREFIX, *coder_counter);
        proto_coders.insert(new_coder_id.clone(), coder_proto);

        new_coder_id
    }

    pub fn register_proto_transform(&self, transform: pipeline_v1::PTransform) {
        let mut pipeline_proto = self.proto.lock().unwrap();

        pipeline_proto
            .components
            .as_mut()
            .unwrap()
            .transforms
            .insert(transform.unique_name.clone(), transform);
    }

    pub fn pre_apply_transform<In, Out, F>(
        &self,
        transform: &F,
        input: &PValue<In>,
    ) -> (String, pipeline_v1::PTransform)
    where
        In: Clone + Send,
        Out: Clone + Send,
        F: PTransform<In, Out> + Send,
    {
        let transform_id = self.context.create_unique_name("transform".to_string());
        let mut parent: Option<&pipeline_v1::PTransform> = None;

        let mut pipeline_proto = self.proto.lock().unwrap();
        let transform_stack = self.transform_stack.lock().unwrap();

        if transform_stack.is_empty() {
            pipeline_proto.root_transform_ids.push(transform_id.clone());
        } else {
            let p = pipeline_proto
                .components
                .as_mut()
                .expect("No components on pipeline proto")
                .transforms
                .get_mut(transform_stack.last().expect("Transform stack is empty"))
                .expect("Transform ID not registered on pipeline proto");

            let p_subtransforms: &mut Vec<String> = p.subtransforms.as_mut();
            p_subtransforms.push(transform_id.clone());

            parent = Some(p);
        }
        drop(transform_stack);

        let parent_name = match parent {
            Some(p) => {
                format!("{}/", p.unique_name.clone())
            }
            None => "".to_string(),
        };

        // TODO: extract unique transform name properly
        let bad_transform_name = crate::internals::utils::get_bad_id();
        let unique_name = format!("{}{}", parent_name, bad_transform_name);

        {
            let mut used_stage_names = self.used_stage_names.lock().unwrap();

            if used_stage_names.contains(&unique_name) {
                panic!("Duplicate stage name: {}", unique_name)
            }
            used_stage_names.insert(unique_name.clone());
        }

        let flattened = flatten_pvalue(input.clone(), None);
        let mut inputs: HashMap<String, String> = HashMap::new();
        for (name, pvalue) in flattened {
            inputs.insert(name.clone(), pvalue.get_id());
        }

        let transform_proto = pipeline_v1::PTransform {
            unique_name,
            spec: None,
            subtransforms: Vec::with_capacity(0),
            inputs,
            outputs: HashMap::with_capacity(0),
            display_data: Vec::with_capacity(0),
            environment_id: self.default_environment.clone(),
            annotations: HashMap::with_capacity(0),
        };

        pipeline_proto
            .components
            .as_mut()
            .unwrap()
            .transforms
            .insert(transform_id.clone(), transform_proto.clone());

        (transform_id, transform_proto)
    }

    pub fn apply_transform<In, Out, F>(
        &self,
        transform: F,
        input: &PValue<In>,
        pipeline: Arc<Pipeline>,
    ) -> PValue<Out>
    where
        In: Clone + Send,
        Out: Clone + Send,
        F: PTransform<In, Out> + Send,
    {
        let (transform_id, transform_proto) = self.pre_apply_transform(&transform, &input);

        let mut transform_stack = self.transform_stack.lock().unwrap();

        transform_stack.push(transform_id);

        let result = transform.expand_internal(input, pipeline, transform_proto.clone());

        // TODO: ensure this happens even if an error takes place above
        transform_stack.pop();

        drop(transform_stack);

        self.post_apply_transform(transform, transform_proto, result)
    }

    // TODO: deal with bounds and windows
    pub fn post_apply_transform<In, Out, F>(
        &self,
        transform: F,
        transform_proto: pipeline_v1::PTransform,
        result: PValue<Out>,
    ) -> PValue<Out>
    where
        In: Clone + Send,
        Out: Clone + Send,
        F: PTransform<In, Out> + Send,
    {
        result
    }

    pub fn create_pcollection_internal<Out>(
        &self,
        coder_id: String,
        pipeline: Arc<Pipeline>,
    ) -> PValue<Out>
    where
        Out: Clone + Send,
    {
        // TODO: remove pcoll_proto arg
        PValue::new(
            crate::internals::pvalue::PType::PCollection,
            pipeline_v1::PCollection::default(),
            pipeline,
            self.create_pcollection_id_internal(coder_id),
        )
    }

    pub fn create_pcollection_id_internal(&self, coder_id: String) -> String {
        let pcoll_id = self.context.create_unique_name("pc".to_string());
        let mut pcoll_proto: pipeline_v1::PCollection = pipeline_v1::PCollection::default();
        pcoll_proto.unique_name = pcoll_id.clone();
        pcoll_proto.coder_id = coder_id;

        let mut pipeline_proto = self.proto.lock().unwrap();
        pipeline_proto
            .components
            .as_mut()
            .unwrap()
            .pcollections
            .insert(pcoll_id.clone(), pcoll_proto);

        pcoll_id
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new("".to_string())
    }
}

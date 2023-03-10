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

use std::fmt;
use std::marker::PhantomData;

use crate::coders::coders::{CoderI, CoderTypeDiscriminants};
use crate::coders::standard_coders::*;
use crate::coders::urns::*;

#[derive(Eq, PartialEq)]
pub struct GeneralObjectCoder<T> {
    coder_type: CoderTypeDiscriminants,
    urn: &'static str,

    phantom: PhantomData<T>,
}

impl<T> GeneralObjectCoder<T> {
    pub fn new() -> Self {
        Self {
            coder_type: CoderTypeDiscriminants::GeneralObject,
            urn: GENERAL_OBJECT_CODER_URN,

            phantom: PhantomData::default(),
        }
    }
}

impl CoderI<String> for GeneralObjectCoder<String> {
    fn get_coder_type(&self) -> &CoderTypeDiscriminants {
        &self.coder_type
    }

    fn encode(
        &self,
        element: String,
        writer: &mut dyn std::io::Write,
        context: &crate::coders::coders::Context,
    ) -> Result<usize, std::io::Error> {
        let marker = "S".as_bytes();
        writer.write_all(marker).unwrap();

        StrUtf8Coder::new().encode(element, writer, context)
    }

    fn decode(
        &self,
        reader: &mut dyn std::io::Read,
        context: &crate::coders::coders::Context,
    ) -> Result<String, std::io::Error> {
        let marker: &mut [u8; 1] = &mut [0; 1];
        reader.read_exact(marker)?;

        if marker == "Z".as_bytes() {
            todo!()
        }

        StrUtf8Coder::new().decode(reader, context)
    }
}

impl<T> Default for GeneralObjectCoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for GeneralObjectCoder<T> {
    #[allow(clippy::needless_lifetimes)] // FIXME
    fn fmt<'a>(&'a self, o: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        o.debug_struct("GeneralObjectCoder")
            .field("urn", &self.urn)
            .finish()
    }
}

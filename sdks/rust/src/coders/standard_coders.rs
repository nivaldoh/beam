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

//! These are the coders necessary for encoding the data types required by
//! the Apache Beam model. They provide standardized ways of encode data for
//! communication between the runner, the Beam workers, and the user's code.
//! For example for any aggregations the runner and the SDK need to agree on
//! the encoding of key-value pairs; so that the SDK will encode keys properly,
//! and the runner will be able to group elements of the
//! same key together.
//!
//! The formal specifications for these coders can be found in
//! model/pipeline/src/main/proto/beam_runner_api.proto

use std::fmt;
use std::io::{self, ErrorKind, Read, Write};

use integer_encoding::{VarInt, VarIntReader, VarIntWriter};

use crate::coders::coders::{CoderI, CoderTypeDiscriminants, Context};
use crate::coders::required_coders::BytesCoder;
use crate::coders::urns::*;

#[derive(Clone)]
pub struct StrUtf8Coder {
    coder_type: CoderTypeDiscriminants,
    urn: &'static str,
}

impl StrUtf8Coder {
    pub fn new() -> Self {
        Self {
            coder_type: CoderTypeDiscriminants::StrUtf8,
            urn: STR_UTF8_CODER_URN,
        }
    }
}

// TODO: accept string references as well?
impl CoderI<String> for StrUtf8Coder {
    fn get_coder_type(&self) -> &CoderTypeDiscriminants {
        &self.coder_type
    }

    fn encode(
        &self,
        element: String,
        writer: &mut dyn Write,
        context: &Context,
    ) -> Result<usize, io::Error> {
        let bytes = element.as_bytes().to_vec();
        let coder = BytesCoder::new();
        coder.encode(bytes, writer, context)
    }

    fn decode(&self, reader: &mut dyn Read, context: &Context) -> Result<String, io::Error> {
        let coder = BytesCoder::new();
        let bytes = coder.decode(reader, context)?;

        let res = String::from_utf8(bytes);

        //TODO: improve error handling
        match res {
            Ok(s) => Ok(s),
            Err(_) => Result::Err(io::Error::new(
                ErrorKind::Other,
                "Unable to convert bytes to string",
            )),
        }
    }
}

impl Default for StrUtf8Coder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for StrUtf8Coder {
    #[allow(clippy::needless_lifetimes)] // FIXME
    fn fmt<'a>(&'a self, o: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        o.debug_struct("StrUtf8Coder")
            .field("urn", &self.urn)
            .finish()
    }
}

#[derive(Clone)]
pub struct VarIntCoder {
    coder_type: CoderTypeDiscriminants,
    urn: &'static str,
}

impl VarIntCoder {
    pub fn new() -> Self {
        Self {
            coder_type: CoderTypeDiscriminants::VarIntCoder,
            urn: VARINT_CODER_URN,
        }
    }
}

// TODO: passes tests for -1 if it gets casted to u64 and encoded as such.
// Revisit this later
impl<N> CoderI<N> for VarIntCoder
where
    N: fmt::Debug + VarInt,
{
    fn get_coder_type(&self) -> &CoderTypeDiscriminants {
        &self.coder_type
    }

    // TODO: try to adapt CoderI such that the context arg is not mandatory
    fn encode(
        &self,
        element: N,
        mut writer: &mut dyn Write,
        _context: &Context,
    ) -> Result<usize, io::Error> {
        writer.write_varint(element)
    }

    fn decode(&self, mut reader: &mut dyn Read, _context: &Context) -> Result<N, io::Error> {
        reader.read_varint()
    }
}

impl Default for VarIntCoder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for VarIntCoder {
    #[allow(clippy::needless_lifetimes)] // FIXME
    fn fmt<'a>(&'a self, o: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        o.debug_struct("VarIntCoder")
            .field("urn", &self.urn)
            .finish()
    }
}

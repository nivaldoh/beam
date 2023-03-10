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

//! Defines all of the Apache Beam required coders.
//!
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
use std::io::{self, Read, Write};
use std::marker::PhantomData;

use integer_encoding::{VarIntReader, VarIntWriter};

use crate::coders::coders::{CoderI, CoderTypeDiscriminants, Context};
use crate::coders::urns::*;

/// Coder for byte-array data types
#[derive(Clone)]
pub struct BytesCoder {
    coder_type: CoderTypeDiscriminants,
    urn: &'static str,
}

impl BytesCoder {
    pub fn new() -> Self {
        BytesCoder {
            coder_type: CoderTypeDiscriminants::Bytes,
            urn: BYTES_CODER_URN,
        }
    }
}

impl CoderI<Vec<u8>> for BytesCoder {
    fn get_coder_type(&self) -> &CoderTypeDiscriminants {
        &self.coder_type
    }

    /// Encode the input element (a byte-string) into the output byte stream from `writer`.
    /// If context is `NeedsDelimiters`, the byte string is encoded prefixed with a
    /// varint representing its length.
    ///
    /// If the context is `WholeStream`, the byte string is encoded as-is.
    fn encode(
        &self,
        element: Vec<u8>,
        mut writer: &mut dyn Write,
        context: &Context,
    ) -> Result<usize, io::Error> {
        match context {
            Context::WholeStream => writer.write(&element),
            Context::NeedsDelimiters => {
                // TODO: confirm that usize gets decoded correctly by production runners
                let delimiter: usize = element.len();
                writer
                    .write_varint(delimiter)
                    .expect("Unable to write delimiter to buffer");

                writer.write(&element)
            }
        }
    }

    /// Decode the input byte stream into a byte array.
    /// If context is `NeedsDelimiters`, the first bytes will be interpreted as a var-int32 encoding
    /// the length of the data.
    ///
    /// If the context is `WholeStream`, the whole input stream is decoded as-is.
    fn decode(&self, mut reader: &mut dyn Read, context: &Context) -> Result<Vec<u8>, io::Error> {
        match context {
            Context::WholeStream => {
                let mut buf: Vec<u8> = Vec::new();
                reader.read_to_end(&mut buf)?;
                Ok(buf)
            }

            Context::NeedsDelimiters => {
                let delimiter: usize = reader
                    .read_varint()
                    .expect("Unable to read delimiter from buffer");

                let mut buf: Vec<u8> = Vec::new();

                let mut num_bytes_read = 0;
                for b in reader.bytes() {
                    if num_bytes_read == delimiter {
                        break;
                    }

                    if b.is_err() {
                        return Err(io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    }

                    buf.push(b.expect("Unable to read byte"));
                    num_bytes_read += 1;
                }

                if num_bytes_read < delimiter {
                    return Err(io::Error::from(std::io::ErrorKind::UnexpectedEof));
                }

                Ok(buf)
            }
        }
    }
}

impl Default for BytesCoder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for BytesCoder {
    #[allow(clippy::needless_lifetimes)] // FIXME
    fn fmt<'a>(&'a self, o: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        o.debug_struct("BytesCoder")
            .field("urn", &self.urn)
            .finish()
    }
}

#[derive(Clone, fmt::Debug)]
pub struct KV<K, V> {
    k: PhantomData<K>,
    v: PhantomData<V>,
}

impl<K, V> KV<K, V>
where
    K: Clone + fmt::Debug,
    V: Clone + fmt::Debug,
{
    pub fn new() -> Self {
        KV {
            k: PhantomData::default(),
            v: PhantomData::default(),
        }
    }
}

impl<K, V> Default for KV<K, V>
where
    K: Clone + fmt::Debug,
    V: Clone + fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A coder for a key-value pair
#[derive(Clone)]
pub struct KVCoder<KV> {
    coder_type: CoderTypeDiscriminants,
    #[allow(unused)] // FIXME
    urn: &'static str,

    phantom: PhantomData<KV>,
}

impl<K, V> CoderI<KV<K, V>> for KVCoder<KV<K, V>> {
    fn get_coder_type(&self) -> &CoderTypeDiscriminants {
        &self.coder_type
    }

    /// Encode the input element (a key-value pair) into a byte output stream. They key and value are encoded one after the
    /// other (first key, then value). The key is encoded with `Context::NeedsDelimiters`, while the value is encoded with
    /// the input context of the `KVCoder`.
    #[allow(unused)] // FIXME
    fn encode(
        &self,
        element: KV<K, V>,
        writer: &mut dyn Write,
        context: &Context,
    ) -> Result<usize, io::Error> {
        todo!()
    }

    /// Decode the input byte stream into a `KV` element
    #[allow(unused)] // FIXME
    fn decode(&self, reader: &mut dyn Read, context: &Context) -> Result<KV<K, V>, io::Error> {
        todo!()
    }
}

/// A coder for a 'list' or a series of elements of the same type
#[derive(Clone)]
pub struct IterableCoder<T> {
    coder_type: CoderTypeDiscriminants,
    #[allow(unused)] // FIXME
    urn: &'static str,

    phantom: PhantomData<T>,
}

pub struct Iterable<T> {
    phantom: PhantomData<T>,
}

impl<T> CoderI<Iterable<T>> for IterableCoder<T> {
    fn get_coder_type(&self) -> &CoderTypeDiscriminants {
        &self.coder_type
    }

    /// Encode the input iterable into a byte output stream. Elements can be encoded in two different ways:
    ///
    /// - If the length of the input iterable is known a-priori, then the length is encoded with a 32-bit
    ///     fixed-length integer.
    /// - If the length of the input iterable is not known a-priori, then a 32-bit integer with a value
    ///     of `-1` is encoded in the first position (instead of the length), and
    ///
    /// Then, each element is encoded individually in `Context::NeedsDelimiters`.
    #[allow(unused)] // FIXME
    fn encode(
        &self,
        element: Iterable<T>,
        writer: &mut dyn Write,
        context: &Context,
    ) -> Result<usize, io::Error> {
        todo!()
    }

    /// Decode the input byte stream into a `Iterable` element
    #[allow(unused)] // FIXME
    fn decode(&self, reader: &mut dyn Read, context: &Context) -> Result<Iterable<T>, io::Error> {
        todo!()
    }
}

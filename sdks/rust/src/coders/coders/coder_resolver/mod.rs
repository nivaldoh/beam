use std::marker::PhantomData;

use crate::{
    coders::{
        coders::CoderI,
        required_coders::{BytesCoder, KVCoder, KV},
        urns::{BYTES_CODER_URN, KV_CODER_URN},
    },
    elem_types::ElemType,
};

/// Resolve a coder (implementing `CoderI) from a coder URN and an `ElemType`.
pub trait CoderResolver {
    type E: ElemType;

    /// Resolve a coder from a coder URN.
    ///
    /// # Returns
    ///
    /// `Some(C)` if the coder was resolved, `None` otherwise.
    fn resolve(&self, coder_urn: &str) -> Option<Box<dyn CoderI<E = Self::E>>>;
}

/// `Vec<u8>` -> `BytesCoder`.
#[derive(Debug)]
pub struct BytesCoderResolverDefault;

impl CoderResolver for BytesCoderResolverDefault {
    type E = Vec<u8>;

    fn resolve(&self, coder_urn: &str) -> Option<Box<dyn CoderI<E = Self::E>>> {
        (coder_urn == BYTES_CODER_URN).then_some(Box::<BytesCoder>::default())
    }
}

/// `KV` -> `KVCoder`.
#[derive(Debug)]
pub struct KVCoderResolverDefault<K, V> {
    phantom: PhantomData<KV<K, V>>,
}

impl<K, V> CoderResolver for KVCoderResolverDefault<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    type E = KV<K, V>;

    fn resolve(&self, coder_urn: &str) -> Option<Box<dyn CoderI<E = Self::E>>> {
        (coder_urn == KV_CODER_URN).then_some(Box::<KVCoder<KV<K, V>>>::default())
    }
}

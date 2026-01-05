use crate as arken;

use arken::{Arken, Error, Field, MergeMap, MergeRootRef, Reader, Writer};
use bytes::BytesMut;
use ordered_float::NotNan;
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashSet, VecDeque},
    io::{Seek, Write},
    marker::PhantomData,
};

pub struct ByteTrigramIter<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> From<&'a [u8]> for ByteTrigramIter<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 3 }
    }
}

impl<'a> Iterator for ByteTrigramIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index > self.bytes.len() {
            return None;
        }

        let bytes = &self.bytes[self.index - 3..self.index];
        self.index += 1;

        Some(bytes)
    }
}

pub struct StringTrigramIter<'a> {
    bytes: &'a [u8],
    iter: std::str::CharIndices<'a>,
    queue: VecDeque<usize>,
}

impl<'a> From<&'a str> for StringTrigramIter<'a> {
    fn from(s: &'a str) -> Self {
        let mut iter = Self {
            bytes: s.as_bytes(),
            iter: s.char_indices(),
            queue: VecDeque::new(),
        };

        for _ in 0..2 {
            let Some((next, _)) = iter.iter.next() else {
                return iter;
            };

            iter.queue.push_back(next);
        }

        iter
    }
}

impl<'a> From<&'a [u8]> for StringTrigramIter<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self::from(std::str::from_utf8(&bytes[..]).unwrap())
    }
}

impl<'a> Iterator for StringTrigramIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let (next, c) = self.iter.next()?;
        self.queue.push_back(next);
        let end = next + c.len_utf8();

        let start = self.queue.pop_front()?;

        Some(&self.bytes[start..end])
    }
}

pub trait TrigramIter {
    fn trigrams<'a>(bytes: &'a [u8]) -> impl Iterator<Item = &'a [u8]>;
}

impl TrigramIter for ByteTrigramIter<'_> {
    fn trigrams<'a>(bytes: &'a [u8]) -> impl Iterator<Item = &'a [u8]> {
        ByteTrigramIter::from(bytes)
    }
}

impl TrigramIter for StringTrigramIter<'_> {
    fn trigrams<'a>(bytes: &'a [u8]) -> impl Iterator<Item = &'a [u8]> {
        StringTrigramIter::from(bytes)
    }
}

#[derive(Arken, Clone, Debug)]
pub struct KeyValue<'a, V: Field<'a>> {
    key: Cow<'a, [u8]>,
    value: V,
    #[arken(skip_with = &PhantomData)]
    _value_lifetime: &'a PhantomData<V>,
}

pub type TrigramRootRef<'a, V> = MergeRootRef<'a, Cow<'a, [u8]>, Cow<'a, [KeyValue<'a, V>]>>;

pub struct TrigramMap<'a, V: Clone + Field<'a>, T: TrigramIter> {
    trigram_map: MergeMap<'a, Cow<'a, [u8]>, Cow<'a, [KeyValue<'a, V>]>>,
    _marker: PhantomData<T>,
}

impl<'a, V: Clone + Field<'a>, T: TrigramIter> TrigramMap<'a, V, T> {
    pub fn open(reader: Reader<'a>, root_reference: Option<TrigramRootRef<'a, V>>) -> Self {
        let trigram_map = MergeMap::open(reader, root_reference);

        Self {
            trigram_map,
            _marker: PhantomData,
        }
    }

    pub fn contains_key(&self, key: &'a [u8]) -> bool {
        let Some(trigram) = T::trigrams(key).next() else {
            return false;
        };

        let Some(values) = self.trigram_map.get(&Cow::Borrowed(trigram)) else {
            return false;
        };

        for key_value in values.as_ref().iter() {
            if key_value.key == key {
                return true;
            }
        }

        false
    }

    pub fn get<'b>(&'b self, key: &'a [u8]) -> Option<Cow<'b, V>> {
        let trigram = T::trigrams(key).next()?;
        let values = self.trigram_map.get(&Cow::Borrowed(trigram))?;

        for key_value in values.as_ref().iter() {
            if key_value.key == key {
                return Some(Cow::Owned(key_value.value.clone()));
            }
        }

        None
    }

    pub fn query(&self, key: &'a [u8]) -> BTreeSet<(NotNan<f32>, Vec<u8>)> {
        let mut results = HashSet::new();
        let mut key_set = HashSet::new();

        for trigram in T::trigrams(key) {
            key_set.insert(trigram);

            let Some(values) = self.trigram_map.get(&Cow::Borrowed(trigram)) else {
                continue;
            };

            for key_value in values.as_ref().iter() {
                let key = key_value.key.clone().into_owned();

                results.insert(key);
            }
        }

        let results: BTreeSet<(NotNan<f32>, Vec<u8>)> = results
            .into_iter()
            .map(|key| {
                let mut set = HashSet::new();

                for trigram in T::trigrams(&key[..]) {
                    set.insert(trigram);
                }

                let intersection = set.intersection(&key_set).count();
                let union = set.union(&key_set).count();
                let similarity =
                    NotNan::new(intersection as f32).unwrap() / NotNan::new(union as f32).unwrap();

                (similarity, key)
            })
            .collect();

        results
    }

    pub fn insert(&mut self, key: &'a [u8], value: V) -> Option<V> {
        if self.contains_key(key) {
            return self.remove(key);
        }

        for trigram in T::trigrams(key) {
            let mut values = self
                .trigram_map
                .get(&Cow::Borrowed(trigram))
                .unwrap_or_default()
                .into_owned()
                .to_vec();

            values.push(KeyValue {
                key: Cow::Borrowed(key),
                value: value.clone(),
                _value_lifetime: &PhantomData,
            });

            self.trigram_map
                .insert(Cow::Borrowed(trigram), Cow::Owned(values));
        }

        None
    }

    pub fn remove(&mut self, key: &'a [u8]) -> Option<V> {
        if !self.contains_key(key) {
            return None;
        }

        let mut value = None;

        for trigram in T::trigrams(key) {
            let mut values = self
                .trigram_map
                .get(&Cow::Borrowed(trigram))
                .unwrap_or_default()
                .into_owned()
                .to_vec();

            let Some(index) = values.iter().position(|key_value| key_value.key == key) else {
                continue;
            };

            let key_value = values.remove(index);
            value = Some(key_value.value);

            if values.is_empty() {
                self.trigram_map.remove(&Cow::Borrowed(trigram));
            } else {
                self.trigram_map
                    .insert(Cow::Borrowed(trigram), Cow::Owned(values));
            }
        }

        value
    }

    pub fn commit<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
    ) -> Result<Option<TrigramRootRef<'a, V>>, Error> {
        self.trigram_map.commit(bytes, writer)
    }
}

pub struct TrigramSet<'a, T: TrigramIter>(TrigramMap<'a, (), T>);

impl<'a, T: TrigramIter> TrigramSet<'a, T> {
    pub fn open(reader: Reader<'a>, root_reference: Option<TrigramRootRef<'a, ()>>) -> Self {
        Self(TrigramMap::open(reader, root_reference))
    }

    pub fn contains(&self, key: &'a [u8]) -> bool {
        self.0.contains_key(key)
    }

    pub fn query(&self, key: &'a [u8]) -> BTreeSet<(NotNan<f32>, Vec<u8>)> {
        self.0.query(key)
    }

    pub fn insert(&mut self, key: &'a [u8]) {
        self.0.insert(key, ());
    }

    pub fn remove(&mut self, key: &'a [u8]) -> bool {
        self.0.remove(key).is_some()
    }

    pub fn commit<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
    ) -> Result<Option<TrigramRootRef<'a, ()>>, Error> {
        self.0.commit(bytes, writer)
    }
}

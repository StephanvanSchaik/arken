use crate as arken;

use arken::{Arken, Error, Field, Reader, Ref, Writer};
use bytes::BytesMut;
use std::{
    borrow::Cow,
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap},
    io::{Seek, Write},
    marker::PhantomData,
};

#[derive(Arken, Clone, Debug)]
pub struct KeyValue<'a, K: Field<'a>, V: Field<'a>> {
    key: K,
    value: Option<V>,
    #[arken(skip_with = &PhantomData)]
    _key_lifetime: &'a PhantomData<K>,
    #[arken(skip_with = &PhantomData)]
    _value_lifetime: &'a PhantomData<V>,
}

pub type KeyValueRef<'a, K, V> = Ref<'a, KeyValue<'a, K, V>>;

#[derive(Arken, Clone, Debug)]
pub struct Node<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    values: Cow<'a, [KeyValueRef<'a, K, V>]>,
}

pub type NodeRef<'a, K, V> = Ref<'a, Node<'a, K, V>>;

#[derive(Arken, Clone, Debug)]
pub struct MergeRoot<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    nodes: Cow<'a, [NodeRef<'a, K, V>]>,
    count: usize,
}

pub type MergeRootRef<'a, K, V> = Ref<'a, MergeRoot<'a, K, V>>;

#[derive(Debug)]
pub struct Keys<'a, 'b, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    map: &'b MergeMap<'a, K, V>,
    heap: BinaryHeap<Reverse<(K, usize, usize)>>,
    iter: std::collections::btree_map::Iter<'b, K, Option<V>>,
}

impl<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> Iterator for Keys<'a, 'b, K, V> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((key, table, n)) = self.heap.pop()?;

        if table == usize::MAX {
            if let Some((key, _)) = self.iter.next() {
                self.heap.push(Reverse((key.clone(), table, n + 1)));
            }
        }

        if let Some(root_reference) = self.map.root_reference.as_ref()
            && let Ok(root) = self.map.reader.read::<MergeRoot<K, V>>(root_reference)
            && let Some(reference) = root.nodes.get(table)
            && let Ok(node) = self.map.reader.read::<Node<'a, K, V>>(reference)
            && let Some(reference) = node.values.get(n + 1)
            && let Ok(key_value) = self.map.reader.read::<KeyValue<'a, K, V>>(reference)
        {
            self.heap.push(Reverse((key_value.key, table, n + 1)));
        }

        while let Some(Reverse((new_key, _, _))) = self.heap.peek() {
            if *new_key != key {
                break;
            }

            let Reverse((_, table, n)) = self.heap.pop()?;

            if table == usize::MAX {
                if let Some((key, _)) = self.iter.next() {
                    self.heap.push(Reverse((key.clone(), table, n + 1)));
                }
            }

            if let Some(root_reference) = self.map.root_reference.as_ref()
                && let Ok(root) = self.map.reader.read::<MergeRoot<K, V>>(root_reference)
                && let Some(reference) = root.nodes.get(table)
                && let Ok(node) = self.map.reader.read::<Node<'a, K, V>>(reference)
                && let Some(reference) = node.values.get(n + 1)
                && let Ok(key_value) = self.map.reader.read::<KeyValue<'a, K, V>>(reference)
            {
                self.heap.push(Reverse((key_value.key, table, n + 1)));
            }
        }

        Some(key)
    }
}

#[derive(Debug)]
pub struct MergeMap<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    pub reader: Reader<'a>,
    pub mem_table: BTreeMap<K, Option<V>>,
    pub root_reference: Option<MergeRootRef<'a, K, V>>,
    pub count: Option<usize>,
}

impl<'a, K: 'a + Clone + Field<'a> + Ord, V: 'a + Clone + Field<'a>> MergeMap<'a, K, V> {
    pub fn open(reader: Reader<'a>, root_reference: Option<MergeRootRef<'a, K, V>>) -> Self {
        Self {
            reader,
            mem_table: BTreeMap::new(),
            root_reference,
            count: None,
        }
    }

    pub fn len(&self) -> usize {
        if let Some(count) = self.count {
            return count;
        }

        if let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<MergeRoot<K, V>>(root_reference)
        {
            return root.count;
        }

        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        if let Some(value) = self.mem_table.get(key) {
            return value.as_ref().map(|value| Cow::Borrowed(value));
        }

        let root_reference = self.root_reference.as_ref()?;
        let root = self.reader.read::<MergeRoot<K, V>>(root_reference).ok()?;

        for reference in root.nodes.iter().rev() {
            let Ok(node) = self.reader.read::<Node<'a, K, V>>(reference) else {
                continue;
            };

            let result = node.values.binary_search_by(|reference| {
                let Ok(key_value) = self.reader.read::<KeyValue<'a, K, V>>(reference) else {
                    return Ordering::Less;
                };

                key_value.key.cmp(key)
            });

            let Ok(index) = result else {
                continue;
            };

            let reference = &node.values[index];

            let Ok(key_value) = self.reader.read::<KeyValue<'a, K, V>>(reference) else {
                continue;
            };

            let value = key_value.value?;

            return Some(Cow::Owned(value));
        }

        None
    }

    pub fn keys<'b>(&'b self) -> Keys<'a, 'b, K, V> {
        let mut heap = BinaryHeap::new();

        let mut iter = self.mem_table.iter();

        if let Some((key, _)) = iter.next() {
            heap.push(Reverse((key.clone(), usize::MAX, 0)));
        }

        if let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<MergeRoot<K, V>>(root_reference)
        {
            for (index, reference) in root.nodes.iter().enumerate() {
                let Ok(node) = self.reader.read::<Node<'a, K, V>>(reference) else {
                    continue;
                };

                let Some(reference) = node.values.first() else {
                    continue;
                };

                let Ok(key_value) = self.reader.read::<KeyValue<'a, K, V>>(reference) else {
                    continue;
                };

                heap.push(Reverse((key_value.key, index, 0)));
            }
        }

        Keys {
            map: self,
            heap,
            iter,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.count.is_none()
            && let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<MergeRoot<K, V>>(root_reference)
        {
            self.count = Some(root.count);
        }

        let has_key = self.contains_key(&key);
        self.mem_table.insert(key, Some(value));

        if !has_key {
            self.count = Some(self.count.unwrap_or(0) + 1);
        }

        None
    }

    pub fn remove(&mut self, key: &K) -> bool {
        if self.count.is_none()
            && let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<MergeRoot<K, V>>(root_reference)
        {
            self.count = Some(root.count);
        }

        if !self.contains_key(key) {
            return false;
        }

        self.mem_table.insert(key.clone(), None);
        self.count = self.count.map(|count| count - 1);

        true
    }

    pub fn commit<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
    ) -> Result<Option<MergeRootRef<'a, K, V>>, Error> {
        if self.mem_table.is_empty() {
            return Ok(self.root_reference.clone());
        }

        let mut values = Vec::with_capacity(self.mem_table.len());

        for (key, value) in std::mem::take(&mut self.mem_table) {
            let key_value = KeyValue {
                key,
                value,
                _key_lifetime: &PhantomData,
                _value_lifetime: &PhantomData,
            };

            let reference = writer.append(bytes, &key_value)?;
            values.push(reference);
        }

        let node = Node {
            values: Cow::Owned(values),
        };

        let reference = writer.append(bytes, &node)?;

        let mut nodes = if let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<MergeRoot<K, V>>(root_reference)
        {
            root.nodes.into_owned()
        } else {
            vec![]
        };

        nodes.push(reference);

        let root = MergeRoot {
            nodes: nodes.into(),
            count: self.count.unwrap_or(0),
        };

        let reference = writer.append(bytes, &root)?;

        Ok(Some(reference))
    }
}

pub struct MergeSet<'a, K: Clone + Field<'a>>(MergeMap<'a, K, ()>);

impl<'a, K: Clone + Field<'a> + Ord> MergeSet<'a, K> {
    pub fn open(reader: Reader<'a>, root_reference: Option<MergeRootRef<'a, K, ()>>) -> Self {
        Self(MergeMap::open(reader, root_reference))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn keys<'b>(&'b self) -> Keys<'a, 'b, K, ()> {
        self.0.keys()
    }

    pub fn remove(&mut self, key: &K) -> bool {
        self.0.remove(key)
    }

    pub fn insert(&mut self, key: K) -> bool {
        self.0.insert(key, ()).is_some()
    }

    pub fn contains(&self, key: &K) -> bool {
        self.0.get(key).is_some()
    }

    pub fn commit<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
    ) -> Result<Option<MergeRootRef<'a, K, ()>>, Error> {
        self.0.commit(bytes, writer)
    }
}

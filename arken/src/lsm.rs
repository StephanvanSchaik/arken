use crate as arken;

use arken::{Arken, Error, Field, Reader, Ref, Writer};
use bytes::BytesMut;
use std::{
    borrow::Cow,
    cmp::Ordering,
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
struct Element<'a, K: Clone + Ord, V: Clone> {
    key: Cow<'a, K>,
    value: Option<Cow<'a, V>>,
    table: usize,
    next: usize,
}

impl<K: Clone + Ord, V: Clone> PartialEq for Element<'_, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K: Clone + Ord, V: Clone> Eq for Element<'_, K, V> {}

impl<K: Clone + Ord, V: Clone> PartialOrd for Element<'_, K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Clone + Ord, V: Clone> Ord for Element<'_, K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key.cmp(&self.key)
    }
}

#[derive(Debug)]
pub struct Iter<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> {
    map: &'b MergeMap<'a, K, V>,
    heap: BinaryHeap<Element<'b, K, V>>,
    iter: std::collections::btree_map::Iter<'b, K, Option<V>>,
}

impl<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> Iterator for Iter<'a, 'b, K, V> {
    type Item = (Cow<'b, K>, Cow<'b, V>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = None;
        let mut value = None;

        while value.is_none() {
            let element = self.heap.pop()?;
            key = Some(element.key);
            value = element.value;

            if element.table == usize::MAX {
                if let Some((key, value)) = self.iter.next() {
                    self.heap.push(Element {
                        key: Cow::Borrowed(key),
                        value: value.as_ref().map(Cow::Borrowed),
                        table: element.table,
                        next: element.next + 1,
                    });
                }
            }

            if let Some(root_reference) = self.map.root_reference.as_ref()
                && let Ok(root) = self.map.reader.read::<MergeRoot<K, V>>(root_reference)
                && let Some(reference) = root.nodes.get(element.table)
                && let Ok(node) = self.map.reader.read::<Node<'a, K, V>>(reference)
                && let Some(reference) = node.values.get(element.next + 1)
                && let Ok(key_value) = self.map.reader.read::<KeyValue<'a, K, V>>(reference)
            {
                self.heap.push(Element {
                    key: Cow::Owned(key_value.key),
                    value: key_value.value.map(Cow::Owned),
                    table: element.table,
                    next: element.next + 1,
                });
            }

            while let Some(element) = self.heap.peek() {
                if key.as_ref().map(|key| *key != element.key).unwrap_or(false) {
                    break;
                }

                let Some(element) = self.heap.pop() else {
                    break;
                };

                value = element.value;

                if element.table == usize::MAX {
                    if let Some((key, value)) = self.iter.next() {
                        self.heap.push(Element {
                            key: Cow::Borrowed(key),
                            value: value.as_ref().map(Cow::Borrowed),
                            table: element.table,
                            next: element.next + 1,
                        });
                    }
                }

                if let Some(root_reference) = self.map.root_reference.as_ref()
                    && let Ok(root) = self.map.reader.read::<MergeRoot<K, V>>(root_reference)
                    && let Some(reference) = root.nodes.get(element.table)
                    && let Ok(node) = self.map.reader.read::<Node<'a, K, V>>(reference)
                    && let Some(reference) = node.values.get(element.next + 1)
                    && let Ok(key_value) = self.map.reader.read::<KeyValue<'a, K, V>>(reference)
                {
                    self.heap.push(Element {
                        key: Cow::Owned(key_value.key),
                        value: key_value.value.map(Cow::Owned),
                        table: element.table,
                        next: element.next + 1,
                    });
                }
            }
        }

        let key = key?;
        let value = value?;

        Some((key, value))
    }
}

#[derive(Debug)]
pub struct Keys<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> {
    iter: Iter<'a, 'b, K, V>,
}

impl<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> Iterator for Keys<'a, 'b, K, V> {
    type Item = Cow<'b, K>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

#[derive(Debug)]
pub struct Values<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> {
    iter: Iter<'a, 'b, K, V>,
}

impl<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> Iterator for Values<'a, 'b, K, V> {
    type Item = Cow<'b, K>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, _)| k)
    }
}

/// An ordered map based on Log-Structured Merge (LSM). Key-value pairs are first collected in a
/// sorted table in memory until committed to the append-only log file. Committing writes the
/// memory table in order by appending it to the append-only log file and updating the root to
/// reference the newly written table. This means that insertion and removal are effectively O(1),
/// and as such LSM is capable of achieving high write throughput.
///
/// Retrieval involves checking the memory table first as well as the committed sorted tables in
/// reverse order until the most recent key-value pair is found for a given key. As such, it is
/// important to keep the number of committed sorted tables to a minimum.
///
/// Iterating the key-value pairs in order involves the use of a binary heap over the
/// minimum/maximum values in the memory table as well as the committed sorted tables to
/// essentially perform a merge sort. This is also used in compaction to reduce the number of
/// committed sorted tables by merging multiple tables into a single table.
///
/// Given a key type with a total order, an ordered map stores its entries in key order. That means
/// that keys must be of a type that implements the [std::cmp::Ord] trait, such that two keys can
/// always be compared to determing their [std::cmp::Ordering]. Examples of keys with a total order
/// are strings with lexicographical order, and numbers with their natural order.
#[derive(Debug)]
pub struct MergeMap<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    reader: Reader<'a>,
    mem_table: BTreeMap<K, Option<V>>,
    root_reference: Option<MergeRootRef<'a, K, V>>,
    root: Option<MergeRoot<'a, K, V>>,
}

impl<'a, K: 'a + Clone + Field<'a> + Ord, V: 'a + Clone + Field<'a>> MergeMap<'a, K, V> {
    fn read_root(&self) -> Option<MergeRoot<'a, K, V>> {
        let root_reference = self.root_reference.as_ref()?;
        let root = self.reader.read(root_reference).ok()?;

        Some(root)
    }

    fn prepare_root(&mut self) {
        // There is nothing to do if the root has already been cached.
        if self.root.is_some() {
            return;
        }

        // Try reading the root from disk. If there is no root, prepare an empty root.
        let Some(mut root) = self.read_root() else {
            self.root = Some(MergeRoot {
                nodes: Cow::Borrowed(&[]),
                count: 0,
            });

            return;
        };

        // Check if there are previously written nodes that do not have a sufficient number of
        // elements. Read their key-value pairs into the memory table, such that we can coalesce
        // these nodes with the newly written node to keep the number of small tables reasonable.
        while self.mem_table.len() < 4096 {
            let Some(reference) = root.nodes.last() else {
                break;
            };

            let Ok(node) = self.reader.read::<Node<'a, K, V>>(reference) else {
                break;
            };

            if node.values.len() >= 4096 {
                break;
            }

            for reference in node.values.as_ref() {
                let Ok(key_value) = self.reader.read::<KeyValue<'a, K, V>>(reference) else {
                    continue;
                };

                self.mem_table.insert(key_value.key, key_value.value);
            }

            let mut nodes = root.nodes.into_owned();
            nodes.pop();
            root.nodes = Cow::Owned(nodes);
        }

        self.root = Some(root);
    }

    pub fn open(reader: Reader<'a>, root_reference: Option<MergeRootRef<'a, K, V>>) -> Self {
        Self {
            reader,
            mem_table: BTreeMap::new(),
            root_reference,
            root: None,
        }
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.root
            .as_ref()
            .map(|root| root.count)
            .or(self.read_root().map(|root| root.count))
            .unwrap_or(0)
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the map contains a value for the specified key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        if let Some(value) = self.mem_table.get(key) {
            return value.as_ref().map(|value| Cow::Borrowed(value));
        }

        let root = self.read_root()?;

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

    /// Gets an iterator over the entries of the map, sorted by key.
    pub fn iter<'b>(&'b self) -> Iter<'a, 'b, K, V> {
        let mut heap = BinaryHeap::new();

        let mut iter = self.mem_table.iter();

        if let Some((key, value)) = iter.next() {
            heap.push(Element {
                key: Cow::Borrowed(key),
                value: value.as_ref().map(Cow::Borrowed),
                table: usize::MAX,
                next: 0,
            });
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

                heap.push(Element {
                    key: Cow::Owned(key_value.key),
                    value: key_value.value.map(Cow::Owned),
                    table: index,
                    next: 0,
                });
            }
        }

        Iter {
            map: self,
            heap,
            iter,
        }
    }

    /// Gets an iterator over the keys of the map, in sorted order.
    #[inline]
    pub fn keys<'b>(&'b self) -> Keys<'a, 'b, K, V> {
        Keys { iter: self.iter() }
    }

    /// Gets an iterator over the values of the map, in order by key.
    #[inline]
    pub fn values<'b>(&'b self) -> Values<'a, 'b, K, V> {
        Values { iter: self.iter() }
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, `None` is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old value is returned.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.prepare_root();

        let has_key = self.contains_key(&key);
        self.mem_table.insert(key, Some(value));

        if !has_key && let Some(root) = &mut self.root {
            root.count += 1;
        }

        None
    }

    pub fn remove(&mut self, key: &K) -> bool {
        self.prepare_root();

        if !self.contains_key(key) {
            return false;
        }

        self.mem_table.insert(key.clone(), None);

        if let Some(root) = &mut self.root {
            root.count -= 1;
        }

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

        let Some(root) = self.root.take() else {
            return Ok(self.root_reference.clone());
        };

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

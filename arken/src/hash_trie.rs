use crate as arken;

use arken::{Arken, Error, Field, Reader, Ref, Writer};
use bytes::BytesMut;
use std::{
    borrow::Cow,
    hash::{DefaultHasher, Hash, Hasher},
    io::{Seek, Write},
    marker::PhantomData,
};

#[derive(Arken, Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Mask(u64);

impl Mask {
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn get_dense_index(&self, index: usize) -> Option<usize> {
        if (self.0 & (1 << index)) != (1 << index) {
            return None;
        }

        Some((self.0 & ((1 << index) - 1)).count_ones() as usize)
    }

    #[inline]
    pub fn last_index(&self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }

        Some(63 - self.0.leading_zeros() as usize)
    }

    #[inline]
    pub fn clear(&mut self, index: usize) {
        self.0 &= !(1 << index);
    }

    #[inline]
    pub fn set(&mut self, index: usize) {
        self.0 |= 1 << index;
    }
}

#[derive(Arken, Clone, Debug)]
pub struct KeyValue<'a, K: Field<'a>, V: Field<'a>> {
    key: K,
    value: V,
    #[arken(skip_with = &PhantomData)]
    _key_lifetime: &'a PhantomData<K>,
    #[arken(skip_with = &PhantomData)]
    _value_lifetime: &'a PhantomData<V>,
}

pub type KeyValueRef<'a, K, V> = Ref<'a, KeyValue<'a, K, V>>;

#[derive(Arken, Clone, Debug)]
pub struct Node<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    value_mask: Mask,
    values: Cow<'a, [KeyValueRef<'a, K, V>]>,
    node_mask: Mask,
    nodes: Cow<'a, [NodeRef<'a, K, V>]>,
}

pub type NodeRef<'a, K, V> = Ref<'a, Node<'a, K, V>>;

#[derive(Arken, Clone, Debug)]
pub struct HashRoot<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    node: NodeRef<'a, K, V>,
    count: usize,
}

pub type HashRootRef<'a, K, V> = Ref<'a, HashRoot<'a, K, V>>;

#[derive(Clone, Debug)]
pub struct MemNode<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    value_mask: Mask,
    values: Cow<'a, [KeyValueRef<'a, K, V>]>,
    node_mask: Mask,
    nodes: Cow<'a, [NodeRef<'a, K, V>]>,
    mem_value_mask: Mask,
    mem_values: Vec<KeyValue<'a, K, V>>,
    mem_node_mask: Mask,
    mem_nodes: Vec<MemNode<'a, K, V>>,
}

impl<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> Default for MemNode<'a, K, V> {
    fn default() -> Self {
        Self {
            value_mask: Mask::default(),
            values: vec![].into(),
            node_mask: Mask::default(),
            nodes: vec![].into(),
            mem_value_mask: Default::default(),
            mem_values: vec![],
            mem_node_mask: Default::default(),
            mem_nodes: vec![],
        }
    }
}

impl<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> From<Node<'a, K, V>> for MemNode<'a, K, V> {
    fn from(node: Node<'a, K, V>) -> MemNode<'a, K, V> {
        MemNode::<'a, K, V> {
            value_mask: node.value_mask,
            values: node.values,
            node_mask: node.node_mask,
            nodes: node.nodes,
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub enum AnyNode<'a, 'b, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    Disk(Node<'a, K, V>),
    Memory(&'b MemNode<'a, K, V>),
}

#[derive(Debug)]
pub struct Keys<'a, 'b, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    map: &'b HashMap<'a, K, V>,
    stack: Vec<(AnyNode<'a, 'b, K, V>, usize)>,
}

impl<'a, 'b, K: Clone + Field<'a> + Ord, V: Clone + Field<'a>> Iterator for Keys<'a, 'b, K, V> {
    type Item = Cow<'b, K>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: while !self.stack.is_empty() {
            let Some((node, index)) = self.stack.last_mut() else {
                break;
            };

            match node {
                AnyNode::Disk(node) => {
                    for i in *index..64 {
                        if let Some(dense_index) = node.value_mask.get_dense_index(i) {
                            if let Some(reference) = node.values.get(dense_index)
                                && let Ok(key_value) =
                                    self.map.reader.read::<KeyValue<'a, K, V>>(reference)
                            {
                                *index = i + 1;
                                return Some(Cow::Owned(key_value.key));
                            }
                        }

                        if let Some(dense_index) = node.node_mask.get_dense_index(i) {
                            if let Some(reference) = node.nodes.get(dense_index)
                                && let Ok(node) = self.map.reader.read::<Node<'a, K, V>>(reference)
                            {
                                *index = i + 1;
                                self.stack.push((AnyNode::Disk(node), 0));
                                continue 'outer;
                            }
                        }
                    }
                }
                AnyNode::Memory(node) => {
                    for i in *index..64 {
                        if let Some(dense_index) = node.mem_value_mask.get_dense_index(i) {
                            if let Some(key_value) = node.mem_values.get(dense_index) {
                                *index = i + 1;
                                return Some(Cow::Borrowed(&key_value.key));
                            }
                        }

                        if let Some(dense_index) = node.value_mask.get_dense_index(i) {
                            if let Some(reference) = node.values.get(dense_index)
                                && let Ok(key_value) =
                                    self.map.reader.read::<KeyValue<'a, K, V>>(reference)
                            {
                                *index = i + 1;
                                return Some(Cow::Owned(key_value.key));
                            }
                        }

                        if let Some(dense_index) = node.mem_node_mask.get_dense_index(i) {
                            if let Some(node) = node.mem_nodes.get(dense_index) {
                                *index = i + 1;
                                self.stack.push((AnyNode::Memory(node), 0));
                                continue 'outer;
                            }
                        }

                        if let Some(dense_index) = node.node_mask.get_dense_index(i) {
                            if let Some(reference) = node.nodes.get(dense_index)
                                && let Ok(node) = self.map.reader.read::<Node<'a, K, V>>(reference)
                            {
                                *index = i + 1;
                                self.stack.push((AnyNode::Disk(node), 0));
                                continue 'outer;
                            }
                        }
                    }
                }
            }

            self.stack.pop();
        }

        None
    }
}

#[derive(Debug)]
pub struct HashMap<'a, K: Clone + Field<'a>, V: Clone + Field<'a>> {
    pub reader: Reader<'a>,
    pub root: Option<MemNode<'a, K, V>>,
    pub root_reference: Option<HashRootRef<'a, K, V>>,
    pub count: usize,
}

impl<'a, K: 'a + Clone + Field<'a> + Hash + PartialEq, V: 'a + Clone + Field<'a>>
    HashMap<'a, K, V>
{
    fn hash(key: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn open(reader: Reader<'a>, root_reference: Option<HashRootRef<'a, K, V>>) -> Self {
        Self {
            reader,
            root: None,
            root_reference,
            count: 0,
        }
    }

    pub fn len(&self) -> usize {
        if self.root.is_none()
            && let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<HashRoot<K, V>>(root_reference)
        {
            return root.count;
        }

        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn keys<'b>(&'b self) -> Keys<'a, 'b, K, V> {
        if let Some(node) = self.root.as_ref() {
            let node = AnyNode::Memory(node);

            return Keys {
                map: self,
                stack: vec![(node, 0)],
            };
        }

        if let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<HashRoot<K, V>>(root_reference)
            && let Ok(node) = self.reader.read::<Node<'a, K, V>>(&root.node)
        {
            let node = AnyNode::Disk(node);

            return Keys {
                map: self,
                stack: vec![(node, 0)],
            };
        }

        return Keys {
            map: self,
            stack: vec![],
        };
    }

    fn remove_node(
        reader: &Reader<'a>,
        count: &mut usize,
        mem_node: &mut MemNode<'a, K, V>,
        hash: u64,
        shift: usize,
        key: &K,
    ) -> Option<bool> {
        let mut result = false;

        if shift >= 64 {
            if let Some(index) = mem_node
                .mem_values
                .iter()
                .position(|key_value| key_value.key == *key)
            {
                mem_node.mem_values.remove(index);
                *count -= 1;

                return Some(true);
            }

            let mut found = None;

            for (index, reference) in mem_node.values.as_ref().iter().enumerate() {
                let key_value = reader.read::<KeyValue<K, V>>(reference).ok()?;

                if key_value.key == *key {
                    found = Some(index);
                    break;
                }
            }

            if let Some(index) = found {
                let mut values = std::mem::take(&mut mem_node.values).into_owned();
                values.remove(index);
                *count -= 1;
                mem_node.values = Cow::Owned(values);

                return Some(true);
            }

            return Some(false);
        }

        let mut removed_value = false;
        let index = ((hash >> shift) & 0b111111) as usize;

        if let Some(dense_index) = mem_node.mem_node_mask.get_dense_index(index) {
            let child = mem_node.mem_nodes.get_mut(dense_index)?;

            result |= Self::remove_node(reader, count, child, hash, shift + 6, key)?;

            if result && child.value_mask.is_empty() && child.mem_value_mask.is_empty() {
                mem_node.mem_nodes.remove(dense_index);
                mem_node.mem_node_mask.clear(index);
            }
        }

        if let Some(dense_index) = mem_node.mem_value_mask.get_dense_index(index)
            && mem_node
                .mem_values
                .get(dense_index)
                .map(|key_value| key_value.key == *key)
                .unwrap_or(false)
        {
            mem_node.mem_values.remove(dense_index);
            mem_node.mem_value_mask.clear(index);
            removed_value = true;
        }

        if let Some(dense_index) = mem_node.node_mask.get_dense_index(index) {
            let node = reader
                .read::<Node<K, V>>(&mem_node.nodes.as_ref()[dense_index])
                .ok()?;

            mem_node.mem_node_mask.set(index);

            let dense_index = mem_node.mem_node_mask.get_dense_index(index)?;
            mem_node.mem_nodes.insert(dense_index, MemNode::from(node));
            let child = mem_node.mem_nodes.get_mut(dense_index)?;

            result |= Self::remove_node(reader, count, child, hash, shift + 6, key)?;

            if result && child.value_mask.is_empty() && child.mem_value_mask.is_empty() {
                let mut nodes = std::mem::take(&mut mem_node.nodes).into_owned();
                nodes.remove(dense_index);
                mem_node.nodes = Cow::Owned(nodes);

                mem_node.mem_node_mask.clear(index);
            }
        }

        if let Some(dense_index) = mem_node.value_mask.get_dense_index(index)
            && let Some(reference) = mem_node.values.get(dense_index)
        {
            let key_value = reader.read::<KeyValue<K, V>>(reference).ok()?;

            if key_value.key == *key {
                let mut values = std::mem::take(&mut mem_node.values).into_owned();
                values.remove(dense_index);
                mem_node.values = Cow::Owned(values);
                mem_node.value_mask.clear(index);
                removed_value = true;
            }
        }

        if removed_value {
            *count -= 1;
        }

        result |= removed_value;

        Some(result)
    }

    pub fn remove(&mut self, key: &K) -> bool {
        let hash = Self::hash(key);

        if self.root.is_none()
            && let Some(root_reference) = self.root_reference.as_ref()
            && let Ok(root) = self.reader.read::<HashRoot<K, V>>(root_reference)
            && let Ok(node) = self.reader.read::<Node<K, V>>(&root.node)
        {
            self.root = Some(MemNode::from(node));
            self.count = root.count;
        }

        let Some(child) = self.root.as_mut() else {
            return false;
        };

        Self::remove_node(&self.reader, &mut self.count, child, hash, 0, key).unwrap_or(false)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let hash = Self::hash(&key);
        let mut shift = 0;

        let key_value = KeyValue {
            key,
            value,
            _key_lifetime: &PhantomData,
            _value_lifetime: &PhantomData,
        };

        if self.root.is_none() {
            if let Some(root_reference) = self.root_reference.as_ref()
                && let Ok(root) = self.reader.read::<HashRoot<K, V>>(root_reference)
                && let Ok(node) = self.reader.read::<Node<K, V>>(&root.node)
            {
                self.root = Some(MemNode::from(node));
                self.count = root.count;
            } else {
                let mut mem_node = MemNode::default();

                let index = (hash & 0b111111) as usize;

                mem_node.mem_value_mask.set(index);
                mem_node.mem_values.push(key_value);

                self.root = Some(mem_node);
                self.count = 1;

                return None;
            }
        }

        let mut mem_node = self.root.as_mut()?;
        let mut reinsert = None;

        while shift < 64 {
            let index = ((hash >> shift) & 0b111111) as usize;
            shift += 6;

            if let Some(dense_index) = mem_node.mem_node_mask.get_dense_index(index) {
                mem_node = mem_node.mem_nodes.get_mut(dense_index)?;

                continue;
            }

            if let Some(dense_index) = mem_node.mem_value_mask.get_dense_index(index) {
                let old_key_value = mem_node.mem_values.remove(dense_index);
                mem_node.mem_value_mask.clear(index);

                let old_hash = Self::hash(&old_key_value.key);

                if hash == old_hash && old_key_value.key == key_value.key {
                    mem_node.mem_value_mask.set(index);
                    let dense_index = mem_node.mem_value_mask.get_dense_index(index)?;
                    mem_node.mem_values.insert(dense_index, key_value);

                    return Some(old_key_value.value);
                }

                mem_node.mem_node_mask.set(index);
                let dense_index = mem_node.mem_node_mask.get_dense_index(index)?;
                mem_node.mem_nodes.insert(dense_index, MemNode::default());
                mem_node = mem_node.mem_nodes.get_mut(dense_index)?;

                while shift < 64 {
                    let index = ((hash >> shift) & 0b111111) as usize;
                    let old_index = ((old_hash >> shift) & 0b111111) as usize;
                    shift += 6;

                    if index != old_index {
                        mem_node.mem_value_mask.set(index);
                        let dense_index = mem_node.mem_value_mask.get_dense_index(index)?;
                        mem_node.mem_values.insert(dense_index, key_value);

                        mem_node.mem_value_mask.set(old_index);
                        let dense_index = mem_node.mem_value_mask.get_dense_index(old_index)?;
                        mem_node.mem_values.insert(dense_index, old_key_value);

                        self.count += 1;

                        return None;
                    }

                    mem_node.mem_node_mask.set(index);
                    let dense_index = mem_node.mem_node_mask.get_dense_index(index)?;
                    mem_node.mem_nodes.insert(dense_index, MemNode::default());
                    mem_node = mem_node.mem_nodes.get_mut(dense_index)?;
                }

                reinsert = Some(old_key_value);

                break;
            }

            if let Some(dense_index) = mem_node.node_mask.get_dense_index(index) {
                let node = self
                    .reader
                    .read::<Node<K, V>>(&mem_node.nodes.as_ref()[dense_index])
                    .ok()?;

                mem_node.mem_node_mask.set(index);

                let dense_index = mem_node.mem_node_mask.get_dense_index(index)?;
                mem_node.mem_nodes.insert(dense_index, MemNode::from(node));
                mem_node = mem_node.mem_nodes.get_mut(dense_index)?;

                continue;
            }

            if let Some(dense_index) = mem_node.value_mask.get_dense_index(index) {
                let reference = mem_node.values.get(dense_index)?;

                let old_key_value = self.reader.read::<KeyValue<K, V>>(reference).ok()?;
                let old_hash = Self::hash(&old_key_value.key);

                if hash == old_hash && old_key_value.key == key_value.key {
                    mem_node.mem_value_mask.set(index);
                    let dense_index = mem_node.mem_value_mask.get_dense_index(index)?;
                    mem_node.mem_values.insert(dense_index, key_value);

                    return Some(old_key_value.value);
                }

                mem_node.mem_node_mask.set(index);
                let dense_index = mem_node.mem_node_mask.get_dense_index(index)?;
                mem_node.mem_nodes.insert(dense_index, MemNode::default());
                mem_node = mem_node.mem_nodes.get_mut(dense_index)?;

                while shift < 64 {
                    let index = ((hash >> shift) & 0b111111) as usize;
                    let old_index = ((old_hash >> shift) & 0b111111) as usize;
                    shift += 6;

                    if index != old_index {
                        mem_node.mem_value_mask.set(index);
                        let dense_index = mem_node.mem_value_mask.get_dense_index(index)?;
                        mem_node.mem_values.insert(dense_index, key_value);

                        mem_node.mem_value_mask.set(old_index);
                        let dense_index = mem_node.mem_value_mask.get_dense_index(old_index)?;
                        mem_node.mem_values.insert(dense_index, old_key_value);

                        self.count += 1;

                        return None;
                    }

                    mem_node.mem_node_mask.set(index);
                    let dense_index = mem_node.mem_node_mask.get_dense_index(index)?;
                    mem_node.mem_nodes.insert(dense_index, MemNode::default());
                    mem_node = mem_node.mem_nodes.get_mut(dense_index)?;
                }

                reinsert = Some(old_key_value);

                break;
            }

            mem_node.mem_value_mask.set(index);
            let dense_index = mem_node.mem_value_mask.get_dense_index(index)?;
            mem_node.mem_values.insert(dense_index, key_value);

            self.count += 1;

            return None;
        }

        if let Some(old_key_value) = reinsert {
            mem_node.mem_values.insert(0, old_key_value);
        }

        mem_node.mem_values.insert(0, key_value);

        self.count += 1;

        None
    }

    fn get_from_reader(
        &self,
        mut node: Node<'a, K, V>,
        hash: u64,
        mut shift: usize,
        key: &K,
    ) -> Option<Cow<'_, V>> {
        while shift < 64 {
            let index = ((hash >> shift) & 0b111111) as usize;
            shift += 6;

            if let Some(dense_index) = node.value_mask.get_dense_index(index) {
                let reference = node.values.get(dense_index)?;
                let key_value = self.reader.read::<KeyValue<K, V>>(reference).ok()?;

                if key_value.key != *key {
                    return None;
                }

                return Some(Cow::Owned(key_value.value));
            }

            if let Some(dense_index) = node.node_mask.get_dense_index(index) {
                let reference = node.nodes.get(dense_index)?;
                node = self.reader.read::<Node<K, V>>(reference).ok()?;

                continue;
            }

            return None;
        }

        for reference in node.values.as_ref() {
            let key_value = self.reader.read::<KeyValue<K, V>>(reference).ok()?;

            if key_value.key == *key {
                return Some(Cow::Owned(key_value.value));
            }
        }

        None
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        let hash = Self::hash(key);
        let mut shift = 0;

        let Some(mut mem_node) = self.root.as_ref() else {
            let root_reference = self.root_reference.as_ref()?;
            let root = self.reader.read::<HashRoot<K, V>>(root_reference).ok()?;
            let node = self.reader.read::<Node<K, V>>(&root.node).ok()?;

            return self.get_from_reader(node, hash, shift, key);
        };

        while shift < 64 {
            let index = ((hash >> shift) & 0b111111) as usize;
            shift += 6;

            if let Some(dense_index) = mem_node.mem_value_mask.get_dense_index(index) {
                let key_value = mem_node.mem_values.get(dense_index)?;

                if key_value.key != *key {
                    return None;
                }

                return Some(Cow::Borrowed(&key_value.value));
            }

            if let Some(dense_index) = mem_node.mem_node_mask.get_dense_index(index) {
                mem_node = mem_node.mem_nodes.get(dense_index)?;
                continue;
            }

            if let Some(dense_index) = mem_node.value_mask.get_dense_index(index) {
                let reference = mem_node.values.get(dense_index)?;
                let key_value = self.reader.read::<KeyValue<K, V>>(reference).ok()?;

                if key_value.key != *key {
                    return None;
                }

                return Some(Cow::Owned(key_value.value));
            }

            if let Some(dense_index) = mem_node.node_mask.get_dense_index(index) {
                let reference = mem_node.nodes.get(dense_index)?;
                let node = self.reader.read::<Node<K, V>>(reference).ok()?;

                return self.get_from_reader(node, hash, shift, key);
            }

            return None;
        }

        for key_value in &mem_node.mem_values {
            if key_value.key == *key {
                return Some(Cow::Borrowed(&key_value.value));
            }
        }

        for reference in mem_node.values.as_ref() {
            let key_value = self.reader.read::<KeyValue<K, V>>(reference).ok()?;

            if key_value.key == *key {
                return Some(Cow::Owned(key_value.value));
            }
        }

        None
    }

    fn commit_node<W: Seek + Write>(
        &self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
        mut mem_node: MemNode<'a, K, V>,
        shift: usize,
    ) -> Result<Ref<'a, Node<'a, K, V>>, Error> {
        if shift >= 64 {
            for key_value in mem_node.mem_values {
                let reference = writer.append(bytes, &key_value)?;

                let mut values = mem_node.values.into_owned();
                values.insert(0, reference);
                mem_node.values = Cow::Owned(values);
            }

            let node = Node {
                value_mask: mem_node.value_mask,
                values: mem_node.values,
                node_mask: mem_node.node_mask,
                nodes: mem_node.nodes,
            };

            return writer.append(bytes, &node);
        }

        while let Some(index) = mem_node.mem_node_mask.last_index() {
            let Some(dense_index) = mem_node.mem_node_mask.get_dense_index(index) else {
                continue;
            };

            mem_node.mem_node_mask.clear(index);

            let reference = {
                let mem_node = mem_node.mem_nodes.remove(dense_index);
                self.commit_node(bytes, writer, mem_node, shift + 6)?
            };

            mem_node.value_mask.clear(index);
            mem_node.node_mask.set(index);

            let Some(dense_index) = mem_node.node_mask.get_dense_index(index) else {
                continue;
            };

            let mut nodes = mem_node.nodes.into_owned();
            nodes.insert(dense_index, reference);
            mem_node.nodes = Cow::Owned(nodes);
        }

        while let Some(index) = mem_node.mem_value_mask.last_index() {
            let Some(dense_index) = mem_node.mem_value_mask.get_dense_index(index) else {
                continue;
            };

            mem_node.mem_value_mask.clear(index);

            let reference = {
                let key_value = mem_node.mem_values.remove(dense_index);
                writer.append(bytes, &key_value)?
            };

            mem_node.value_mask.set(index);

            let Some(dense_index) = mem_node.value_mask.get_dense_index(index) else {
                continue;
            };

            let mut values = mem_node.values.into_owned();
            values.insert(dense_index, reference);
            mem_node.values = Cow::Owned(values);
        }

        let node = Node {
            value_mask: mem_node.value_mask,
            values: mem_node.values,
            node_mask: mem_node.node_mask,
            nodes: mem_node.nodes,
        };

        writer.append(bytes, &node)
    }

    pub fn commit<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
    ) -> Result<Option<HashRootRef<'a, K, V>>, Error> {
        let Some(node) = self.root.take() else {
            return Ok(None);
        };

        let node = self.commit_node(bytes, writer, node, 0)?;

        let root = HashRoot {
            node,
            count: self.count,
        };

        let reference = writer.append(bytes, &root)?;

        Ok(Some(reference))
    }
}

pub struct HashSet<'a, K: Clone + Field<'a>>(HashMap<'a, K, ()>);

impl<'a, K: Clone + Field<'a> + Hash + PartialEq> HashSet<'a, K> {
    pub fn open(reader: Reader<'a>, root_reference: Option<HashRootRef<'a, K, ()>>) -> Self {
        Self(HashMap::open(reader, root_reference))
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
    ) -> Result<Option<HashRootRef<'a, K, ()>>, Error> {
        self.0.commit(bytes, writer)
    }
}

//! A Batching Queue or an [Israeli Queue](https://arpitbhayani.me/blogs/israeli-queues) is a queue data structure that keeps elements in a group together.
//!
//! It can be used in situations where we want to deal with all events of a type together.
//!
//! See [`Queue`] for the data type.
//!
//! See [`Groupable`] for the necessary trait.

use std::mem::ManuallyDrop;
use std::num::NonZeroUsize;

/// Trait for defining types that can be grouped.
///
/// It is assumed that the implementation will be symmetric and groups will be mutually exclusive.
/// Without these, the groups could possibly become fragmentated.
///
/// # Implementation
/// Grouping an enum based on its variant:
/// ```
/// # use batching_queue::Groupable;
/// #[derive(Debug, PartialEq, Eq)]
/// enum Group {
///     G1,
///     G2
/// }
///
///  impl Groupable for Group {
///      fn is_same_group(&self, other: &Self) -> bool {
///          match (self, other) {
///              (Self::G1, Self::G1) | (Self::G2, Self::G2) => true,
///              _ => false
///          }
///      }
///  }
/// ```
/// It is not at all necessary that the implementer be an enum. It could easily be a struct and groups could be based on their properties.
pub trait Groupable {
    /// The function returns `true` if `self` and `other` are part of the same group
    fn is_same_group(&self, other: &Self) -> bool;
}

/// Internal type for indexing operations into the queue
///
/// Stores index, offset by 1, to benefit from Null Pointer Optimization
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct Index(NonZeroUsize);

impl Index {
    /// Offset the index and store
    fn new(v: usize) -> Self {
        Self(NonZeroUsize::new(v + 1).unwrap())
    }

    /// Remove the offset and return index
    const fn get(&self) -> usize {
        self.0.get() - 1
    }
}

/// The Queue
///
/// Stores elements in groups as defined by their implementation of [`Groupable`].
/// ```
/// # use batching_queue::{Queue, Groupable};
/// #[derive(Debug, PartialEq, Eq)]
/// enum Group {
///     G1,
///     G2
/// }
/// #
/// # impl Groupable for Group {
/// #     fn is_same_group(&self, other: &Self) -> bool {
/// #         match (self, other) {
/// #             (Self::G1, Self::G1) | (Self::G2, Self::G2) => true,
/// #             _ => false
/// #         }
/// #     }
/// # }
///
/// use Group::{G1, G2};
/// let mut q = Queue::new();
/// q.push(G1);
/// q.push(G2);
/// q.push(G1);
/// assert_eq!(q.pop(), Some(G1));
/// assert_eq!(q.pop(), Some(G1));
/// assert_eq!(q.pop(), Some(G2));
/// assert_eq!(q.pop(), None);
/// ```
#[derive(Debug)]
pub struct Queue<T>
where
    T: Groupable,
{
    head: Option<Index>,
    free: Option<Index>,
    tail: Option<Index>,
    tail_leader: Option<Index>,
    arr: Vec<Node<T>>,
}

/// Internal Node type for linked list
///
/// The queue has 3 linked lists
/// 1. Doubly Linked list of all nodes in the queue
/// 2. Singly Linked list of the free holes
/// 3. Singly Linked list of the first nodes for all nodes
///
/// Nodes in list 3 are of type [`Node::Leader`].
/// The other 2 lists are made of both types of nodes.
#[derive(Debug)]
enum Node<T> {
    Leader {
        next_group: Option<Index>,
        next: Option<Index>,
        prev: Option<Index>,
        val: ManuallyDrop<T>,
    },
    Normal {
        next: Option<Index>,
        prev: Option<Index>,
        val: ManuallyDrop<T>,
    },
}

impl<T> Node<T> {
    /// Gives the index of the next group from this node
    ///
    /// # Panic
    /// This function will panic if not called on a leader
    fn next_group(&self) -> Option<Index> {
        match self {
            Node::Leader { next_group, .. } => *next_group,
            Node::Normal { .. } => panic!("Method must be called on a leader"),
        }
    }

    /// Gives a mutable reference to the index of the next group from this node
    ///
    /// # Panic
    /// This function will panic if not called on a leader
    fn next_group_mut(&mut self) -> &mut Option<Index> {
        match self {
            Node::Leader { next_group, .. } => next_group,
            Node::Normal { .. } => panic!("Method must be called on a leader"),
        }
    }

    /// Gives the index of the next node from this node
    const fn next(&self) -> Option<Index> {
        match self {
            Node::Leader { next, .. } => *next,
            Node::Normal { next, .. } => *next,
        }
    }

    /// Gives a mutable reference to the index of the next node from this node
    fn next_mut(&mut self) -> &mut Option<Index> {
        match self {
            Node::Leader { next, .. } | Node::Normal { next, .. } => next,
        }
    }

    /// Gives the index of the previous node from this node
    const fn prev(&self) -> Option<Index> {
        match self {
            Node::Leader { prev, .. } => *prev,
            Node::Normal { prev, .. } => *prev,
        }
    }

    /// Gives a mutable reference to the index of the previous node from this node
    fn prev_mut(&mut self) -> &mut Option<Index> {
        match self {
            Node::Leader { prev, .. } | Node::Normal { prev, .. } => prev,
        }
    }

    /// Gives a reference of the value in this node
    const fn val(&self) -> &ManuallyDrop<T> {
        match self {
            Node::Leader { val, .. } => val,
            Node::Normal { val, .. } => val,
        }
    }

    /// Gives a mutable reference to the value in this node
    fn val_mut(&mut self) -> &mut ManuallyDrop<T> {
        match self {
            Node::Leader { val, .. } => val,
            Node::Normal { val, .. } => val,
        }
    }
}

impl<T: Groupable> Queue<T> {
    /// Construct an empty queue
    ///
    /// ```
    /// # use batching_queue::{Queue, Groupable};
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Group {
    ///     G1,
    ///     G2
    /// }
    /// #
    /// # impl Groupable for Group {
    /// #     fn is_same_group(&self, other: &Self) -> bool {
    /// #         match (self, other) {
    /// #             (Self::G1, Self::G1) | (Self::G2, Self::G2) => true,
    /// #             _ => false
    /// #         }
    /// #     }
    /// # }
    ///
    /// use Group::{G1, G2};
    /// let q = Queue::<Group>::new();
    /// ```
    pub fn new() -> Self {
        Self {
            head: None,
            free: None,
            tail: None,
            tail_leader: None,
            arr: vec![],
        }
    }

    /// Check if the queue is empty
    ///
    /// ```
    /// # use batching_queue::{Queue, Groupable};
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Group {
    ///     G1,
    ///     G2
    /// }
    /// #
    /// # impl Groupable for Group {
    /// #     fn is_same_group(&self, other: &Self) -> bool {
    /// #         match (self, other) {
    /// #             (Self::G1, Self::G1) | (Self::G2, Self::G2) => true,
    /// #             _ => false
    /// #         }
    /// #     }
    /// # }
    ///
    /// use Group::{G1, G2};
    /// let mut q = Queue::new();
    /// assert!(q.is_empty());
    /// q.push(G1);
    /// assert!(!q.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    /// Push the value into the queue
    ///
    /// ```
    /// # use batching_queue::{Queue, Groupable};
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Group {
    ///     G1,
    ///     G2
    /// }
    /// #
    /// # impl Groupable for Group {
    /// #     fn is_same_group(&self, other: &Self) -> bool {
    /// #         match (self, other) {
    /// #             (Self::G1, Self::G1) | (Self::G2, Self::G2) => true,
    /// #             _ => false
    /// #         }
    /// #     }
    /// # }
    ///
    /// use Group::{G1, G2};
    /// let mut q = Queue::new();
    /// q.push(G1);
    /// q.push(G2);
    /// ```
    pub fn push(&mut self, val: T) {
        let mut cur = self.head;
        let mut leader_idx = None;
        while let Some(idx) = cur {
            assert!(matches!(self.arr[idx.get()], Node::Leader {..}));
            if val.is_same_group(&self.arr[idx.get()].val()) {
                leader_idx = Some(idx);
                break;
            }
            cur = self.arr[idx.get()].next_group();
        }

        let val = ManuallyDrop::new(val);
        if let Some(idx) = leader_idx {
            if let Some(next_gp) = self.arr[idx.get()].next_group() {
                let node = Node::Normal {
                    next: Some(next_gp),
                    prev: self.arr[next_gp.get()].prev(),
                    val,
                };
                if let Some(free_idx) = self.free {
                    self.free = self.arr[free_idx.get()].next();
                    *self.arr[node.prev().unwrap().get()].next_mut() = Some(free_idx);
                    *self.arr[node.next().unwrap().get()].prev_mut() = Some(free_idx);
                    self.arr[free_idx.get()] = node;
                } else {
                    let new_idx = Index::new(self.arr.len());
                    *self.arr[node.prev().unwrap().get()].next_mut() = Some(new_idx);
                    *self.arr[node.next().unwrap().get()].prev_mut() = Some(new_idx);
                    self.arr.push(node);
                }
            } else {
                let node = Node::Normal {
                    next: None,
                    prev: self.tail,
                    val,
                };
                if let Some(free_idx) = self.free {
                    self.free = self.arr[free_idx.get()].next();
                    *self.arr[node.prev().unwrap().get()].next_mut() = Some(free_idx);
                    self.arr[free_idx.get()] = node;
                    self.tail = Some(free_idx);
                } else {
                    let new_idx = Index::new(self.arr.len());
                    *self.arr[node.prev().unwrap().get()].next_mut() = Some(new_idx);
                    self.arr.push(node);
                    self.tail = Some(new_idx);
                }
            }
        } else {
            let node = Node::Leader {
                next_group: None,
                next: None,
                prev: self.tail,
                val,
            };
            if let Some(free_idx) = self.free {
                self.free = self.arr[free_idx.get()].next();
                if let Some(tail_ldr) = self.tail_leader {
                    *self.arr[tail_ldr.get()].next_group_mut() = Some(free_idx);
                    self.tail_leader = Some(free_idx);
                    *self.arr[self.tail.unwrap().get()].next_mut() = Some(free_idx);
                    self.tail = Some(free_idx);
                    self.arr[free_idx.get()] = node;
                } else {
                    self.head = Some(free_idx);
                    self.tail = Some(free_idx);
                    self.tail_leader = Some(free_idx);
                    self.arr[free_idx.get()] = node;
                }
            } else {
                let new_idx = Index::new(self.arr.len());
                if let Some(tail_ldr) = self.tail_leader {
                    *self.arr[tail_ldr.get()].next_group_mut() = Some(new_idx);
                    self.tail_leader = Some(new_idx);
                    *self.arr[self.tail.unwrap().get()].next_mut() = Some(new_idx);
                    self.tail = Some(new_idx);
                    self.arr.push(node);
                } else {
                    self.head = Some(new_idx);
                    self.tail = Some(new_idx);
                    self.tail_leader = Some(new_idx);
                    self.arr.push(node);
                }
            }
        }
    }

    /// Pop the head from the queue
    ///
    /// ```
    /// # use batching_queue::{Queue, Groupable};
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Group {
    ///     G1,
    ///     G2
    /// }
    /// #
    /// # impl Groupable for Group {
    /// #     fn is_same_group(&self, other: &Self) -> bool {
    /// #         match (self, other) {
    /// #             (Self::G1, Self::G1) | (Self::G2, Self::G2) => true,
    /// #             _ => false
    /// #         }
    /// #     }
    /// # }
    ///
    /// use Group::{G1, G2};
    /// let mut q = Queue::new();
    /// q.push(G1);
    /// assert_eq!(q.pop(), Some(G1));
    /// assert_eq!(q.pop(), None);
    /// ```
    pub fn pop(&mut self) -> Option<T> {
        if let Some(head_idx) = self.head {
            if let Some(next_gp) = self.arr[head_idx.get()].next_group() {
                let next_idx = self.arr[head_idx.get()].next().unwrap();
                if next_idx == next_gp {
                    *self.arr[next_gp.get()].prev_mut() = None;
                    self.head = Some(next_gp);
                } else {
                    let new_head = Node::Leader {
                        next_group: Some(next_gp),
                        next: self.arr[next_idx.get()].next(),
                        prev: None,
                        val: unsafe { std::ptr::read(self.arr[next_idx.get()].val()) },
                    };
                    self.arr[next_idx.get()] = new_head;
                    self.head = Some(next_idx);
                }
            } else {
                if let Some(next_idx) = self.arr[head_idx.get()].next() {
                    let new_head = Node::Leader {
                        next_group: None,
                        next: self.arr[next_idx.get()].next(),
                        prev: None,
                        val: unsafe { std::ptr::read(self.arr[next_idx.get()].val()) },
                    };
                    self.arr[next_idx.get()] = new_head;
                    self.head = Some(next_idx);
                } else {
                    self.head = None;
                    self.tail = None;
                    self.tail_leader = None;
                }
            }
            *self.arr[head_idx.get()].next_mut() = self.free.take();
            self.free = Some(head_idx);
            unsafe { Some(ManuallyDrop::take(self.arr[head_idx.get()].val_mut())) }
        } else {
            None
        }
    }

    /// Pop a whole group from the front of the queue and return a vector of the popped elements
    ///
    /// An empty vector is returned if the queue is empty
    /// ```
    /// # use batching_queue::{Queue, Groupable};
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Group {
    ///     G1,
    ///     G2
    /// }
    /// #
    /// # impl Groupable for Group {
    /// #     fn is_same_group(&self, other: &Self) -> bool {
    /// #         match (self, other) {
    /// #             (Self::G1, Self::G1) | (Self::G2, Self::G2) => true,
    /// #             _ => false
    /// #         }
    /// #     }
    /// # }
    ///
    /// use Group::{G1, G2};
    /// let mut q = Queue::new();
    /// q.push(G1);
    /// q.push(G2);
    /// q.push(G1);
    /// assert_eq!(q.pop_group(), [G1, G1]);
    /// ```
    pub fn pop_group(&mut self) -> Vec<T> {
        let mut ret_vals = vec![];
        if let Some(head_idx) = self.head {
            if let Some(nxt_grp) = self.arr[head_idx.get()].next_group() {
                self.head = Some(nxt_grp);
                *self.arr[nxt_grp.get()].prev_mut() = None;
            } else {
                self.head = None;
                self.tail = None;
                self.tail_leader = None;
            }
            let mut cur = Some(head_idx);
            while let Some(idx) = cur {
                if cur == self.head {
                    break;
                } else {
                    cur = self.arr[idx.get()].next();
                }
                let val = unsafe { ManuallyDrop::take(self.arr[idx.get()].val_mut()) };
                ret_vals.push(val);
                *self.arr[idx.get()].next_mut() = self.free.take();
                self.free = Some(idx);
            }
        }
        ret_vals
    }
}

impl<T: Groupable> Drop for Queue<T> {
    fn drop(&mut self) {
        let mut cur = self.head;
        while let Some(idx) = cur {
            cur = self.arr[idx.get()].next();
            unsafe { ManuallyDrop::drop(self.arr[idx.get()].val_mut()) }
        }
        unsafe { self.arr.set_len(0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[derive(Debug)]
    enum MyFoo<'a> {
        Foo1(u32, &'a Cell<i32>),
        Foo2(u32, &'a Cell<i32>),
        Foo3(u32, &'a Cell<i32>),
    }

    impl Groupable for MyFoo<'_> {
        fn is_same_group(&self, other: &Self) -> bool {
            match (self, other) {
                (MyFoo::Foo1(..), MyFoo::Foo1(..))
                | (MyFoo::Foo2(..), MyFoo::Foo2(..))
                | (MyFoo::Foo3(..), MyFoo::Foo3(..)) => true,
                _ => false,
            }
        }
    }

    impl Drop for MyFoo<'_> {
        fn drop(&mut self) {
            match self {
                MyFoo::Foo1(x, c) => {
                    c.set(c.get() - 1);
                    println!("Dropping Foo1 : {}\tref_count: {}", x, c.get())
                }
                MyFoo::Foo2(x, c) => {
                    c.set(c.get() - 1);
                    println!("Dropping Foo2 : {}\tref_count: {}", x, c.get())
                }
                MyFoo::Foo3(x, c) => {
                    c.set(c.get() - 1);
                    println!("Dropping Foo3 : {}\tref_count: {}", x, c.get())
                }
            }
        }
    }

    impl PartialEq for MyFoo<'_> {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (MyFoo::Foo1(x, _), MyFoo::Foo1(y, _)) => x == y,
                (MyFoo::Foo2(x, _), MyFoo::Foo2(y, _)) => x == y,
                (MyFoo::Foo3(x, _), MyFoo::Foo3(y, _)) => x == y,
                _ => false,
            }
        }
    }

    impl<'a> MyFoo<'a> {
        fn new(kind: u8, val: u32, counter: &'a Cell<i32>) -> Self {
            counter.set(counter.get() + 1);
            match kind {
                1 => Self::Foo1(val, counter),
                2 => Self::Foo2(val, counter),
                3 => Self::Foo3(val, counter),
                _ => panic!("Unknown variant"),
            }
        }
    }

    #[test]
    fn grouping() {
        let ref_counter = Cell::new(0);
        let mut q = Queue::<MyFoo>::new();
        q.push(MyFoo::new(1, 1, &ref_counter));
        q.push(MyFoo::new(2, 2, &ref_counter));
        q.push(MyFoo::new(1, 3, &ref_counter));
        assert_eq!(
            q.pop_group(),
            [
                MyFoo::new(1, 1, &ref_counter),
                MyFoo::new(1, 3, &ref_counter)
            ]
        );
        assert_eq!(q.pop_group(), [MyFoo::new(2, 2, &ref_counter),]);
        assert_eq!(q.pop_group(), []);
        drop(q);
        assert_eq!(ref_counter.get(), 0);
    }
}

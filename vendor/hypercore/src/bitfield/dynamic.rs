use super::fixed::{FIXED_BITFIELD_BITS_LENGTH, FIXED_BITFIELD_LENGTH, FixedBitfield};
use crate::{
    Store,
    common::{BitfieldUpdate, StoreInfo, StoreInfoInstruction, StoreInfoType},
};
use futures::future::Either;
use std::{cell::RefCell, convert::TryInto};

const DYNAMIC_BITFIELD_PAGE_SIZE: usize = 32768;

/// Dynamic sized bitfield, uses a map of `FixedBitfield` elements.
/// See:
/// https://github.com/hypercore-protocol/hypercore/blob/master/lib/bitfield.js
/// for reference.
#[derive(Debug)]
pub(crate) struct DynamicBitfield {
    pages: intmap::IntMap<RefCell<FixedBitfield>>,
    biggest_page_index: u64,
    unflushed: Vec<u64>,
}

impl DynamicBitfield {
    pub(crate) fn open(info: Option<StoreInfo>) -> Either<StoreInfoInstruction, Self> {
        match info {
            None => Either::Left(StoreInfoInstruction::new_size(Store::Bitfield, 0)),
            Some(info) => {
                if info.info_type == StoreInfoType::Size {
                    let bitfield_store_length = info.length.unwrap();
                    // Read only multiples of 4 bytes.
                    let length = bitfield_store_length - (bitfield_store_length & 3);
                    return Either::Left(StoreInfoInstruction::new_content(
                        Store::Bitfield,
                        0,
                        length,
                    ));
                }
                let data = info.data.expect("Did not receive bitfield store content");
                let resumed = data.len() >= 4;
                let mut biggest_page_index = 0;
                if resumed {
                    let mut pages: intmap::IntMap<RefCell<FixedBitfield>> = intmap::IntMap::new();
                    let mut data_index = 0;
                    while data_index < data.len() {
                        let parent_index: u64 = (data_index / FIXED_BITFIELD_LENGTH) as u64;
                        pages.insert(
                            parent_index,
                            RefCell::new(FixedBitfield::from_data(data_index, &data)),
                        );
                        if parent_index > biggest_page_index {
                            biggest_page_index = parent_index;
                        }
                        data_index += FIXED_BITFIELD_LENGTH;
                    }
                    Either::Right(Self {
                        pages,
                        unflushed: vec![],
                        biggest_page_index,
                    })
                } else {
                    Either::Right(Self {
                        pages: intmap::IntMap::new(),
                        unflushed: vec![],
                        biggest_page_index,
                    })
                }
            }
        }
    }

    /// Flushes pending changes, returns info slices to write to storage.
    pub(crate) fn flush(&mut self) -> Box<[StoreInfo]> {
        let mut infos_to_flush: Vec<StoreInfo> = Vec::with_capacity(self.unflushed.len());
        for unflushed_id in &self.unflushed {
            let mut p = self.pages.get_mut(*unflushed_id).unwrap().borrow_mut();
            let data = p.to_bytes();
            infos_to_flush.push(StoreInfo::new_content(
                Store::Bitfield,
                *unflushed_id * data.len() as u64,
                &data,
            ));
            p.dirty = false;
        }
        self.unflushed = vec![];
        infos_to_flush.into_boxed_slice()
    }

    pub(crate) fn get(&self, index: u64) -> bool {
        let j = index & (DYNAMIC_BITFIELD_PAGE_SIZE as u64 - 1);
        let i = (index - j) / DYNAMIC_BITFIELD_PAGE_SIZE as u64;

        if !self.pages.contains_key(i) {
            false
        } else {
            let p = self.pages.get(i).unwrap().borrow();
            p.get(j.try_into().expect("Index should have fit into u32"))
        }
    }

    #[allow(dead_code)]
    pub(crate) fn set(&mut self, index: u64, value: bool) -> bool {
        let j = index & (DYNAMIC_BITFIELD_PAGE_SIZE as u64 - 1);
        let i = (index - j) / DYNAMIC_BITFIELD_PAGE_SIZE as u64;

        if !self.pages.contains_key(i) {
            if value {
                self.pages.insert(i, RefCell::new(FixedBitfield::new()));
                if i > self.biggest_page_index {
                    self.biggest_page_index = i;
                }
            } else {
                // The page does not exist, but when setting false, that doesn't matter
                return false;
            }
        }

        let mut p = self.pages.get_mut(i).unwrap().borrow_mut();
        let changed: bool = p.set(j.try_into().expect("Index should have fit into u32"), value);

        if changed && !p.dirty {
            p.dirty = true;
            self.unflushed.push(i);
        }
        changed
    }

    pub(crate) fn update(&mut self, bitfield_update: &BitfieldUpdate) {
        self.set_range(
            bitfield_update.start,
            bitfield_update.length,
            !bitfield_update.drop,
        )
    }

    pub(crate) fn set_range(&mut self, start: u64, length: u64, value: bool) {
        let mut j = start & (DYNAMIC_BITFIELD_PAGE_SIZE as u64 - 1);
        let mut i = (start - j) / (DYNAMIC_BITFIELD_PAGE_SIZE as u64);
        let mut length = length;

        while length > 0 {
            if !self.pages.contains_key(i) {
                self.pages.insert(i, RefCell::new(FixedBitfield::new()));
                if i > self.biggest_page_index {
                    self.biggest_page_index = i;
                }
            }
            let mut p = self.pages.get_mut(i).unwrap().borrow_mut();

            let end = std::cmp::min(j + length, DYNAMIC_BITFIELD_PAGE_SIZE as u64);

            let range_start: u32 = j
                .try_into()
                .expect("Range start should have fit into a u32");
            let range_end: u32 = (end - j)
                .try_into()
                .expect("Range end should have fit into a u32");

            let changed = p.set_range(range_start, range_end, value);
            if changed && !p.dirty {
                p.dirty = true;
                self.unflushed.push(i);
            }

            j = 0;
            i += 1;
            length -= range_end as u64;
        }
    }

    /// Finds the first index of the value after given position. Returns None if not found.
    pub(crate) fn index_of(&self, value: bool, position: u64) -> Option<u64> {
        let first_index = position & (DYNAMIC_BITFIELD_PAGE_SIZE as u64 - 1);
        let first_page = (position - first_index) / (DYNAMIC_BITFIELD_PAGE_SIZE as u64);

        if value {
            // For finding the first positive value, we only care about pages that are set,
            // not pages that don't exist, as they can't possibly contain the value.

            // To keep the common case fast, first try the same page as the position
            if let Some(p) = self.pages.get(first_page)
                && let Some(index) = p.borrow().index_of(value, first_index as u32)
            {
                return Some(first_page * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + index as u64);
            };

            // It wasn't found on the first page, now get the keys that are bigger
            // than the given index and sort them.
            let mut keys: Vec<&u64> = self.pages.keys().filter(|key| **key > first_page).collect();
            keys.sort();
            for key in keys {
                if let Some(p) = self.pages.get(*key)
                    && let Some(index) = p.borrow().index_of(value, 0)
                {
                    return Some(key * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + index as u64);
                };
            }
        } else {
            // Searching for the false value is easier as it is automatically hit on
            // a missing page.
            let mut i = first_page;
            let mut j = first_index as u32;
            while i == first_page || i <= self.biggest_page_index {
                if let Some(p) = self.pages.get(i) {
                    if let Some(index) = p.borrow().index_of(value, j) {
                        return Some(i * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + index as u64);
                    };
                } else {
                    return Some(i * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + j as u64);
                }
                i += 1;
                j = 0; // We start at the beginning of each page
            }
        }
        None
    }

    /// Finds the last index of the value before given position. Returns None if not found.
    pub(crate) fn last_index_of(&self, value: bool, position: u64) -> Option<u64> {
        let last_index = position & (DYNAMIC_BITFIELD_PAGE_SIZE as u64 - 1);
        let last_page = (position - last_index) / (DYNAMIC_BITFIELD_PAGE_SIZE as u64);

        if value {
            // For finding the last positive value, we only care about pages that are set,
            // not pages that don't exist, as they can't possibly contain the value.

            // To keep the common case fast, first try the same page as the position
            if let Some(p) = self.pages.get(last_page)
                && let Some(index) = p.borrow().last_index_of(value, last_index as u32)
            {
                return Some(last_page * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + index as u64);
            };

            // It wasn't found on the last page, now get the keys that are smaller
            // than the given index and sort them.
            let mut keys: Vec<&u64> = self.pages.keys().filter(|key| **key < last_page).collect();
            keys.sort();
            keys.reverse();

            for key in keys {
                if let Some(p) = self.pages.get(*key)
                    && let Some(index) = p
                        .borrow()
                        .last_index_of(value, FIXED_BITFIELD_BITS_LENGTH as u32 - 1)
                {
                    return Some(key * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + index as u64);
                };
            }
        } else {
            // Searching for the false value is easier as it is automatically hit on
            // a missing page.
            let mut i = last_page;
            let mut j = last_index as u32;
            while i == last_page || i == 0 {
                if let Some(p) = self.pages.get(i) {
                    if let Some(index) = p.borrow().last_index_of(value, j) {
                        return Some(i * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + index as u64);
                    };
                } else {
                    return Some(i * DYNAMIC_BITFIELD_PAGE_SIZE as u64 + j as u64);
                }
                i -= 1;
                j = FIXED_BITFIELD_BITS_LENGTH as u32 - 1; // We start at end of each page
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_value_range(bitfield: &DynamicBitfield, start: u64, length: u64, value: bool) {
        for i in start..start + length {
            assert_eq!(bitfield.get(i), value);
        }
    }

    fn get_dynamic_bitfield() -> DynamicBitfield {
        match DynamicBitfield::open(Some(StoreInfo::new_content(Store::Bitfield, 0, &[]))) {
            Either::Left(_) => panic!("Could not open bitfield"),
            Either::Right(bitfield) => bitfield,
        }
    }

    #[test]
    fn bitfield_dynamic_get_and_set() {
        let mut bitfield = get_dynamic_bitfield();
        assert_value_range(&bitfield, 0, 9, false);
        assert_eq!(bitfield.index_of(true, 0), None);
        assert_eq!(bitfield.index_of(false, 0), Some(0));
        assert_eq!(bitfield.last_index_of(true, 9), None);
        assert_eq!(bitfield.last_index_of(false, 9), Some(9));
        assert_eq!(bitfield.index_of(true, 10000000), None);
        assert_eq!(bitfield.index_of(false, 10000000), Some(10000000));
        assert_eq!(bitfield.last_index_of(true, 10000000), None);
        assert_eq!(bitfield.last_index_of(false, 10000000), Some(10000000));

        bitfield.set(0, true);
        assert!(bitfield.get(0));
        assert_eq!(bitfield.index_of(true, 0), Some(0));
        assert_eq!(bitfield.index_of(false, 0), Some(1));
        assert_eq!(bitfield.last_index_of(true, 9), Some(0));
        assert_eq!(bitfield.last_index_of(false, 9), Some(9));
        assert_eq!(bitfield.last_index_of(true, 10000000), Some(0));
        assert_eq!(bitfield.last_index_of(false, 10000000), Some(10000000));

        assert_value_range(&bitfield, 1, 63, false);
        bitfield.set(31, true);
        assert!(bitfield.get(31));

        assert_value_range(&bitfield, 32, 32, false);
        assert!(!bitfield.get(32));
        bitfield.set(32, true);
        assert!(bitfield.get(32));
        assert_value_range(&bitfield, 33, 31, false);

        assert_value_range(&bitfield, 32760, 8, false);
        assert!(!bitfield.get(32767));
        bitfield.set(32767, true);
        assert!(bitfield.get(32767));
        assert_value_range(&bitfield, 32760, 7, false);

        // Now for over one fixed bitfield values
        bitfield.set(32768, true);
        assert_value_range(&bitfield, 32767, 2, true);
        assert_value_range(&bitfield, 32769, 9, false);

        bitfield.set(10000000, true);
        assert!(bitfield.get(10000000));
        assert_value_range(&bitfield, 9999990, 10, false);
        assert_value_range(&bitfield, 10000001, 9, false);
        assert_eq!(bitfield.index_of(false, 32767), Some(32769));
        assert_eq!(bitfield.index_of(true, 32769), Some(10000000));
        assert_eq!(bitfield.last_index_of(true, 9999999), Some(32768));
    }

    #[test]
    fn bitfield_dynamic_set_range() {
        let mut bitfield = get_dynamic_bitfield();
        bitfield.set_range(0, 2, true);
        assert_value_range(&bitfield, 0, 2, true);
        assert_value_range(&bitfield, 3, 61, false);

        bitfield.set_range(2, 3, true);
        assert_value_range(&bitfield, 0, 5, true);
        assert_value_range(&bitfield, 5, 59, false);

        bitfield.set_range(1, 3, false);
        assert!(bitfield.get(0));
        assert_value_range(&bitfield, 1, 3, false);
        assert_value_range(&bitfield, 4, 1, true);
        assert_value_range(&bitfield, 5, 59, false);

        bitfield.set_range(30, 30070, true);
        assert_value_range(&bitfield, 5, 25, false);
        assert_value_range(&bitfield, 30, 100, true);
        assert_value_range(&bitfield, 30050, 50, true);
        assert_value_range(&bitfield, 31000, 50, false);

        bitfield.set_range(32750, 18, true);
        assert_value_range(&bitfield, 32750, 18, true);

        bitfield.set_range(32765, 3, false);
        assert_value_range(&bitfield, 32750, 15, true);
        assert_value_range(&bitfield, 32765, 3, false);

        // Now for over one fixed bitfield values
        bitfield.set_range(32765, 15, true);
        assert_value_range(&bitfield, 32765, 15, true);
        assert_value_range(&bitfield, 32780, 9, false);
        bitfield.set_range(32766, 3, false);
        assert_value_range(&bitfield, 32766, 3, false);

        bitfield.set_range(10000000, 50, true);
        assert_value_range(&bitfield, 9999990, 9, false);
        assert_value_range(&bitfield, 10000050, 9, false);
        assert_eq!(bitfield.index_of(true, 32780), Some(10000000));
        bitfield.set_range(0, 32780, false);
        // Manufacture empty pages to test sorting
        bitfield.set(900000, true);
        bitfield.set(900000, false);
        bitfield.set(300000, true);
        bitfield.set(300000, false);
        bitfield.set(200000, true);
        bitfield.set(200000, false);
        bitfield.set(500000, true);
        bitfield.set(500000, false);
        bitfield.set(100000, true);
        bitfield.set(100000, false);
        bitfield.set(700000, true);
        bitfield.set(700000, false);
        assert_eq!(bitfield.index_of(true, 0), Some(10000000));
        assert_eq!(bitfield.last_index_of(true, 9999999), None);

        bitfield.set_range(10000010, 10, false);
        assert_value_range(&bitfield, 10000000, 10, true);
        assert_value_range(&bitfield, 10000010, 10, false);
        assert_value_range(&bitfield, 10000020, 30, true);
        assert_value_range(&bitfield, 10000050, 9, false);
    }
}

use compact_encoding::{
    CompactEncoding, FixedWidthEncoding, FixedWidthU32, as_array_mut, get_slices_checked,
    get_slices_mut_checked, map_decode, take_array_mut,
};
use futures::future::Either;
use std::convert::{TryFrom, TryInto};

use crate::common::{BitfieldUpdate, Store, StoreInfo, StoreInfoInstruction};
use crate::tree::MerkleTreeChangeset;
use crate::{HypercoreError, PartialKeypair};

use hypercore_schema::Node;

pub(crate) mod entry;
mod header;

pub(crate) use entry::{Entry, EntryTreeUpgrade};
pub(crate) use header::{Header, HeaderTree};

pub(crate) const MAX_OPLOG_ENTRIES_BYTE_SIZE: u64 = 65536;
const HEADER_SIZE: usize = 4096;

// NB: we use the word "leader" to describe the 8 byte part put before a chunk of data that
// contains 4 byte checksum, then a 30 bit unsigned integer, then a "header bit" and a "partial
// bit"
const CRC_SIZE: usize = 4;
const LEN_PARTIAL_AND_HEADER_INFO_SIZE: usize = 4;
const LEADER_SIZE: usize = CRC_SIZE + LEN_PARTIAL_AND_HEADER_INFO_SIZE;

/// Oplog.
///
/// There are two memory areas for an `Header` in `RandomAccessStorage`: one is the current
/// and one is the older. Which one is used depends on the value stored in the eigth byte's
/// eight bit of the stored headers.
#[derive(Debug)]
pub(crate) struct Oplog {
    header_bits: [bool; 2],
    pub(crate) entries_length: u64,
    pub(crate) entries_byte_length: u64,
}

/// Oplog create header outcome
#[derive(Debug)]
pub(crate) struct OplogCreateHeaderOutcome {
    pub(crate) header: Header,
    pub(crate) infos_to_flush: Box<[StoreInfo]>,
}

/// Oplog open outcome
#[derive(Debug)]
pub(crate) struct OplogOpenOutcome {
    pub(crate) oplog: Oplog,
    pub(crate) header: Header,
    pub(crate) infos_to_flush: Box<[StoreInfo]>,
    pub(crate) entries: Option<Box<[Entry]>>,
}

impl OplogOpenOutcome {
    pub(crate) fn new(oplog: Oplog, header: Header, infos_to_flush: Box<[StoreInfo]>) -> Self {
        Self {
            oplog,
            header,
            infos_to_flush,
            entries: None,
        }
    }
    pub(crate) fn from_create_header_outcome(
        oplog: Oplog,
        create_header_outcome: OplogCreateHeaderOutcome,
    ) -> Self {
        Self {
            oplog,
            header: create_header_outcome.header,
            infos_to_flush: create_header_outcome.infos_to_flush,
            entries: None,
        }
    }
}

#[repr(usize)]
enum OplogSlot {
    FirstHeader = 0,
    SecondHeader = HEADER_SIZE,
    Entries = HEADER_SIZE * 2,
}

#[derive(Debug)]
struct ValidateLeaderOutcome<'a> {
    state: &'a [u8],
    header_bit: bool,
    partial_bit: bool,
}

// The first set of bits is [1, 0], see `get_next_header_oplog_slot_and_bit_value` for how
// they change.
const INITIAL_HEADER_BITS: [bool; 2] = [true, false];

impl Oplog {
    /// Opens an existing Oplog from existing byte buffer or creates a new one.
    pub(crate) fn open(
        key_pair: &Option<PartialKeypair>,
        info: Option<StoreInfo>,
    ) -> Result<Either<StoreInfoInstruction, OplogOpenOutcome>, HypercoreError> {
        match info {
            None => Ok(Either::Left(StoreInfoInstruction::new_all_content(
                Store::Oplog,
            ))),
            Some(info) => {
                let existing = info.data.expect("Could not get data of existing oplog");
                // First read and validate both headers stored in the existing oplog
                let h1_outcome = if let Some(h1) =
                    existing.get(OplogSlot::FirstHeader as usize..OplogSlot::SecondHeader as usize)
                {
                    Self::validate_leader(h1)?
                } else {
                    None
                };
                let h2_outcome = if let Some(h2) =
                    existing.get(OplogSlot::SecondHeader as usize..OplogSlot::Entries as usize)
                {
                    Self::validate_leader(h2)?
                } else {
                    None
                };
                // Depending on what is stored, the state needs to be set accordingly.
                // See `get_next_header_oplog_slot_and_bit_value` for details on header_bits.
                let mut outcome: OplogOpenOutcome = if let Some(h1_outcome) = h1_outcome {
                    let (header, header_bits): (Header, [bool; 2]) =
                        if let Some(h2_outcome) = h2_outcome {
                            let header_bits = [h1_outcome.header_bit, h2_outcome.header_bit];
                            let header: Header = if header_bits[0] == header_bits[1] {
                                Header::decode(h1_outcome.state)?.0
                            } else {
                                Header::decode(h2_outcome.state)?.0
                            };
                            (header, header_bits)
                        } else {
                            (
                                Header::decode(h1_outcome.state)?.0,
                                [h1_outcome.header_bit, h1_outcome.header_bit],
                            )
                        };
                    let oplog = Oplog {
                        header_bits,
                        entries_length: 0,
                        entries_byte_length: 0,
                    };
                    OplogOpenOutcome::new(oplog, header, Box::new([]))
                } else if let Some(h2_outcome) = h2_outcome {
                    // This shouldn't happen because the first header is saved to the first slot
                    // but Javascript supports this so we should too.
                    let header_bits: [bool; 2] = [!h2_outcome.header_bit, h2_outcome.header_bit];
                    let oplog = Oplog {
                        header_bits,
                        entries_length: 0,
                        entries_byte_length: 0,
                    };
                    OplogOpenOutcome::new(oplog, Header::decode(h2_outcome.state)?.0, Box::new([]))
                } else if let Some(key_pair) = key_pair {
                    // There is nothing in the oplog, start from fresh given key pair.
                    Self::fresh(key_pair.clone())?
                } else {
                    // The storage is empty and no key pair given, erroring
                    return Err(HypercoreError::EmptyStorage {
                        store: Store::Oplog,
                    });
                };

                // Read headers that might be stored in the existing content
                if existing.len() > OplogSlot::Entries as usize {
                    let mut entries_buff =
                        get_slices_checked(&existing, OplogSlot::Entries as usize)?.1;
                    let mut entries: Vec<Entry> = Vec::new();
                    let mut partials: Vec<bool> = Vec::new();
                    let mut entry_sizes: Vec<u64> = Vec::new();
                    while let Some(entry_outcome) = Self::validate_leader(entries_buff)? {
                        let res = Entry::decode(entry_outcome.state)?;
                        entry_sizes.push((entries_buff.len() - res.1.len()) as u64);
                        entries.push(res.0);
                        entries_buff = res.1;
                        partials.push(entry_outcome.partial_bit);
                    }

                    // Remove uncommitted trailing partials from every parallel
                    // record list. Failing to pop `partials` loops forever.
                    while partials.last() == Some(&true) {
                        partials.pop();
                        entry_sizes.pop();
                        entries.pop();
                    }
                    // A subsequent append must begin after the recovered
                    // committed entries, not overwrite the start of the log.
                    outcome.oplog.entries_length = entries.len() as u64;
                    outcome.oplog.entries_byte_length = entry_sizes.iter().sum();
                    let committed_end = OplogSlot::Entries as u64 + outcome.oplog.entries_byte_length;
                    if committed_end < existing.len() as u64 {
                        // Discard incomplete trailing bytes as well: a shorter
                        // next append must not leave an orphaned old suffix.
                        outcome.infos_to_flush = vec![StoreInfo::new_truncate(Store::Oplog, committed_end)].into_boxed_slice();
                    }
                    outcome.entries = Some(entries.into_boxed_slice());
                }
                Ok(Either::Right(outcome))
            }
        }
    }

    /// Appends an upgraded changeset to the Oplog.
    pub(crate) fn append_changeset(
        &mut self,
        changeset: &MerkleTreeChangeset,
        bitfield_update: Option<BitfieldUpdate>,
        atomic: bool,
        header: &Header,
    ) -> Result<OplogCreateHeaderOutcome, HypercoreError> {
        let mut header: Header = header.clone();
        let entry = self.update_header_with_changeset(changeset, bitfield_update, &mut header)?;

        Ok(OplogCreateHeaderOutcome {
            header,
            infos_to_flush: self.append_entries(&[entry], atomic)?,
        })
    }

    pub(crate) fn update_header_with_changeset(
        &self,
        changeset: &MerkleTreeChangeset,
        bitfield_update: Option<BitfieldUpdate>,
        header: &mut Header,
    ) -> Result<Entry, HypercoreError> {
        let tree_nodes: Vec<Node> = changeset.nodes.clone();
        let entry: Entry = if changeset.upgraded {
            let hash = changeset
                .hash
                .as_ref()
                .expect("Upgraded changeset must have a hash before appended");
            let signature = changeset
                .signature
                .expect("Upgraded changeset must be signed before appended");
            let signature: Box<[u8]> = signature.to_bytes().into();
            header.tree.root_hash = hash.clone();
            header.tree.signature = signature.clone();
            header.tree.length = changeset.length;

            Entry {
                user_data: vec![],
                tree_nodes,
                tree_upgrade: Some(EntryTreeUpgrade {
                    fork: changeset.fork,
                    ancestors: changeset.ancestors,
                    length: changeset.length,
                    signature,
                }),
                bitfield: bitfield_update,
            }
        } else {
            Entry {
                user_data: vec![],
                tree_nodes,
                tree_upgrade: None,
                bitfield: bitfield_update,
            }
        };
        Ok(entry)
    }

    /// Clears a segment, returns infos to write to storage.
    pub(crate) fn clear(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Box<[StoreInfo]>, HypercoreError> {
        let entry: Entry = Entry {
            user_data: vec![],
            tree_nodes: vec![],
            tree_upgrade: None,
            bitfield: Some(BitfieldUpdate {
                drop: true,
                start,
                length: end - start,
            }),
        };
        self.append_entries(&[entry], false)
    }

    /// Flushes pending changes, returns infos to write to storage.
    pub(crate) fn flush(
        &mut self,
        header: &Header,
        clear_traces: bool,
    ) -> Result<Box<[StoreInfo]>, HypercoreError> {
        let (new_header_bits, infos_to_flush) = if clear_traces {
            // When clearing traces, both slots need to be cleared, hence
            // do this twice, but for the first time, ignore the truncate
            // store info, to end up with three StoreInfos.
            let (new_header_bits, infos_to_flush) =
                Self::insert_header(header, 0, self.header_bits, clear_traces)?;
            let mut combined_infos_to_flush: Vec<StoreInfo> =
                infos_to_flush.into_vec().drain(0..1).collect();
            let (new_header_bits, infos_to_flush) =
                Self::insert_header(header, 0, new_header_bits, clear_traces)?;
            combined_infos_to_flush.extend(infos_to_flush.into_vec());
            (new_header_bits, combined_infos_to_flush.into_boxed_slice())
        } else {
            Self::insert_header(header, 0, self.header_bits, clear_traces)?
        };
        self.entries_byte_length = 0;
        self.entries_length = 0;
        self.header_bits = new_header_bits;
        Ok(infos_to_flush)
    }

    /// Appends a batch of entries to the Oplog.
    fn append_entries(
        &mut self,
        batch: &[Entry],
        atomic: bool,
    ) -> Result<Box<[StoreInfo]>, HypercoreError> {
        let len = batch.len();
        let header_bit = self.get_current_header_bit();

        let mut size = len * LEADER_SIZE;

        for e in batch.iter() {
            size += e.encoded_size()?;
        }

        let mut buffer = vec![0; size];
        let mut rest = buffer.as_mut_slice();
        for (i, entry) in batch.iter().enumerate() {
            let partial_bit: bool = atomic && i < len - 1;
            rest = encode_with_leader(entry, partial_bit, header_bit, rest)?;
        }
        let index = OplogSlot::Entries as u64 + self.entries_byte_length;
        self.entries_length += len as u64;
        self.entries_byte_length += size as u64;

        Ok(vec![StoreInfo::new_content(Store::Oplog, index, &buffer)].into_boxed_slice())
    }

    fn fresh(key_pair: PartialKeypair) -> Result<OplogOpenOutcome, HypercoreError> {
        let entries_length: u64 = 0;
        let entries_byte_length: u64 = 0;
        let header = Header::new(key_pair);
        let (header_bits, infos_to_flush) =
            Self::insert_header(&header, entries_byte_length, INITIAL_HEADER_BITS, false)?;
        let oplog = Oplog {
            header_bits,
            entries_length,
            entries_byte_length,
        };
        Ok(OplogOpenOutcome::from_create_header_outcome(
            oplog,
            OplogCreateHeaderOutcome {
                header,
                infos_to_flush,
            },
        ))
    }

    fn insert_header(
        header: &Header,
        entries_byte_length: u64,
        current_header_bits: [bool; 2],
        clear_traces: bool,
    ) -> Result<([bool; 2], Box<[StoreInfo]>), HypercoreError> {
        // The first 8 bytes will be filled with `prepend_leader`.

        // Get the right slot and header bit
        let (oplog_slot, header_bit) =
            Oplog::get_next_header_oplog_slot_and_bit_value(&current_header_bits);
        let mut new_header_bits = current_header_bits;
        match oplog_slot {
            OplogSlot::FirstHeader => new_header_bits[0] = header_bit,
            OplogSlot::SecondHeader => new_header_bits[1] = header_bit,
            _ => {
                panic!("Invalid oplog slot");
            }
        }

        let mut size = LEADER_SIZE + header.encoded_size()?;
        size += header.encoded_size()?;

        // If clearing, lets add zeros to the end
        if clear_traces {
            size = HEADER_SIZE;
        }

        // Create a buffer for the needed data
        let mut buffer = vec![0; size];
        encode_with_leader(header, false, header_bit, &mut buffer)?;

        // The oplog is always truncated to the minimum byte size, which is right after
        // all of the entries in the oplog finish.
        let truncate_index = OplogSlot::Entries as u64 + entries_byte_length;
        Ok((
            new_header_bits,
            vec![
                StoreInfo::new_content(Store::Oplog, oplog_slot as u64, &buffer),
                StoreInfo::new_truncate(Store::Oplog, truncate_index),
            ]
            .into_boxed_slice(),
        ))
    }

    /// Validates that leader at given index is valid, and returns header and partial bits and
    /// `State` for the header/entry that the leader was for.
    fn validate_leader(buffer: &[u8]) -> Result<Option<ValidateLeaderOutcome<'_>>, HypercoreError> {
        if buffer.len() < 8 {
            return Ok(None);
        }
        let ((stored_checksum, combined), data_buff) =
            map_decode!(buffer, [FixedWidthU32<'_>, FixedWidthU32<'_>]);

        let len = usize::try_from(combined >> 2)
            .expect("Attempted converting to a 32 bit usize on below 32 bit system");

        // NB: In the Javascript version IIUC zero length is caught only with a mismatch
        // of checksums, which is silently interpreted to only mean "no value". That doesn't sound good:
        // better to throw an error on mismatch and let the caller at least log the problem.
        if len == 0 || data_buff.len() < len {
            return Ok(None);
        }

        let header_bit = combined & 1 == 1;
        let partial_bit = combined & 2 == 2;

        let to_hash = &buffer[CRC_SIZE..LEADER_SIZE + len];
        let calculated_checksum = crc32fast::hash(to_hash);
        if calculated_checksum != stored_checksum {
            return Err(HypercoreError::InvalidChecksum {
                context: format!(
                    "Calculated signature [{calculated_checksum}] does not match oplog signature [{stored_checksum}]"
                ),
            });
        };
        Ok(Some(ValidateLeaderOutcome {
            header_bit,
            partial_bit,
            state: data_buff,
        }))
    }

    /// Gets the current header bit
    fn get_current_header_bit(&self) -> bool {
        self.header_bits[0] != self.header_bits[1]
    }

    /// Based on given header_bits, determines if saving the header should be done to the first
    /// header slot or the second header slot and the bit that it should get.
    fn get_next_header_oplog_slot_and_bit_value(header_bits: &[bool; 2]) -> (OplogSlot, bool) {
        // Writing a header to the disk is most efficient when only one area is saved.
        // This makes it a bit less obvious to find out which of the headers is older
        // and which newer. The bits indicate the header slot index in this way:
        //
        // [true, false] => [false, false] => [false, true] => [true, true] => [true, false] ...
        //      First    =>     Second     =>     First     =>    Second    =>    First
        if header_bits[0] != header_bits[1] {
            // First slot
            (OplogSlot::FirstHeader, !header_bits[0])
        } else {
            // Second slot
            (OplogSlot::SecondHeader, !header_bits[1])
        }
    }
}

/// Create a header. 30 bits are the length plus two bits for "partial" and "header" info
fn build_len_and_info_header(data_length: usize, header_bit: bool, partial_bit: bool) -> u32 {
    let data_length: u32 = data_length
        .try_into()
        .expect("Must be able to convert usize to u32");
    const MASK: u32 = (3u32).rotate_right(2);

    if (MASK & data_length) != 0 {
        panic!("Data length would overflow. It does not fit in 30 bits");
    }
    let partial_bit: u32 = if partial_bit { 2 } else { 0 };
    let header_bit: u32 = if header_bit { 1 } else { 0 };
    (data_length << 2) | header_bit | partial_bit
}

fn write_leader_parts(
    header_bit: bool,
    partial_bit: bool,
    crc_zone: &mut [u8; CRC_SIZE],
    len_and_meta_zone: &mut [u8; LEN_PARTIAL_AND_HEADER_INFO_SIZE],
    data: &[u8],
) -> Result<(), HypercoreError> {
    // first we write the length and partial data
    let len_and_info = build_len_and_info_header(data.len(), header_bit, partial_bit);
    (len_and_info.as_fixed_width()).encode(len_and_meta_zone)?;
    // next we hash the new header info along with the data
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(len_and_meta_zone);
    hasher.update(data);
    hasher.finalize().as_fixed_width().encode(crc_zone)?;
    Ok(())
}

fn encode_with_leader<'a>(
    thing: &impl CompactEncoding,
    partial_bit: bool,
    header_bit: bool,
    buffer: &'a mut [u8],
) -> Result<&'a mut [u8], HypercoreError> {
    let (leader_bytes, data_and_rest) = take_array_mut::<LEADER_SIZE>(buffer)?;
    let enc_size = thing.encoded_size()?;
    let (data_buff, rest) = get_slices_mut_checked(data_and_rest, enc_size)?;
    let (crc_zone, len_and_meta_zone) = get_slices_mut_checked(leader_bytes, CRC_SIZE)?;
    thing.encode(data_buff)?;
    write_leader_parts(
        header_bit,
        partial_bit,
        as_array_mut::<CRC_SIZE>(crc_zone)?,
        as_array_mut::<LEN_PARTIAL_AND_HEADER_INFO_SIZE>(len_and_meta_zone)?,
        data_buff,
    )?;
    Ok(rest)
}

#[cfg(test)]
mod shadw_regression_tests {
    use super::*;
    use crate::common::StoreInfoType;

    fn apply_infos(bytes: &mut Vec<u8>, infos: &[StoreInfo]) {
        for info in infos {
            let offset = info.index as usize;
            if info.info_type == StoreInfoType::Size {
                bytes.resize(offset, 0);
            } else if let Some(data) = &info.data {
                if bytes.len() < offset + data.len() { bytes.resize(offset + data.len(), 0); }
                bytes[offset..offset + data.len()].copy_from_slice(data);
            }
        }
    }

    #[test]
    fn shadw_recovered_entries_keep_append_offset() {
        let signing = crate::generate_signing_key();
        let mut fresh = Oplog::fresh(PartialKeypair { public: signing.verifying_key(), secret: None }).unwrap();
        let mut bytes = Vec::new();
        apply_infos(&mut bytes, &fresh.infos_to_flush);
        apply_infos(&mut bytes, &fresh.oplog.clear(4, 5).unwrap());
        apply_infos(&mut bytes, &fresh.oplog.clear(6, 7).unwrap());
        let expected_bytes = fresh.oplog.entries_byte_length;
        let Either::Right(mut reopened) = Oplog::open(&None, Some(StoreInfo::new_content(Store::Oplog, 0, &bytes))).unwrap() else { panic!("expected recovered log") };
        assert_eq!(reopened.oplog.entries_length, 2);
        assert_eq!(reopened.oplog.entries_byte_length, expected_bytes);
        let changes = reopened.oplog.clear(8, 9).unwrap();
        assert_eq!(changes[0].index, OplogSlot::Entries as u64 + expected_bytes);
    }

    #[test]
    fn shadw_trailing_partial_is_discarded_without_looping() {
        let signing = crate::generate_signing_key();
        let mut fresh = Oplog::fresh(PartialKeypair { public: signing.verifying_key(), secret: None }).unwrap();
        let mut bytes = Vec::new();
        apply_infos(&mut bytes, &fresh.infos_to_flush);
        apply_infos(&mut bytes, &fresh.oplog.clear(4, 5).unwrap());
        let committed_bytes = fresh.oplog.entries_byte_length;
        let partial = Entry { user_data: vec![], tree_nodes: vec![], tree_upgrade: None, bitfield: Some(BitfieldUpdate { drop: false, start: 9, length: 1 }) };
        let mut record = vec![0; LEADER_SIZE + partial.encoded_size().unwrap()];
        encode_with_leader(&partial, true, fresh.oplog.get_current_header_bit(), &mut record).unwrap();
        bytes.extend(record);
        let Either::Right(reopened) = Oplog::open(&None, Some(StoreInfo::new_content(Store::Oplog, 0, &bytes))).unwrap() else { panic!("expected recovered log") };
        assert_eq!(reopened.entries.unwrap().len(), 1);
        assert_eq!(reopened.oplog.entries_length, 1);
        assert_eq!(reopened.oplog.entries_byte_length, committed_bytes);
        assert_eq!(reopened.infos_to_flush.len(), 1);
        assert_eq!(reopened.infos_to_flush[0].index, OplogSlot::Entries as u64 + committed_bytes);
    }
}

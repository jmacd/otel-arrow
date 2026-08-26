use super::*;

pub(super) struct HeaderFields;

impl HeaderFields {
    pub(super) const VERSION: U16Field = U16Field::new(0);
    pub(super) const ENTRY_COUNT: U16Field = U16Field::new(Self::VERSION.end());
    pub(super) const ITEM_COUNT: U16Field = U16Field::new(Self::ENTRY_COUNT.end());
    pub(super) const MEMBER_COUNT: U16Field = U16Field::new(Self::ITEM_COUNT.end());
    pub(super) const LEN: usize = Self::MEMBER_COUNT.end();
}

pub(super) struct EntryFields;

impl EntryFields {
    pub(super) const FIRST_MEMBER: U16Field = U16Field::new(0);
    pub(super) const MEMBER_COUNT: U16Field = U16Field::new(Self::FIRST_MEMBER.end());
    pub(super) const HASH: U64Field = U64Field::new(Self::MEMBER_COUNT.end());
    pub(super) const LEN: usize = Self::HASH.end();
}

pub(super) struct ItemFields;

impl ItemFields {
    pub(super) const SCHEMA_INDEX: U16Field = U16Field::new(0);
    pub(super) const KIND: U8Field = U8Field::new(Self::SCHEMA_INDEX.end());
    pub(super) const _PAD: U8Field = U8Field::new(Self::KIND.end());
    pub(super) const WIRE_NAME: BlobRangeField = BlobRangeField::new(Self::_PAD.end());
    pub(super) const VALUE: BlobRangeField = BlobRangeField::new(Self::WIRE_NAME.end());
    pub(super) const LEN: usize = Self::VALUE.end();
}

pub(super) struct MemberFields;

impl MemberFields {
    pub(super) const ITEM: U16Field = U16Field::new(0);
    pub(super) const FIELD: U16Field = U16Field::new(Self::ITEM.end());
    pub(super) const LEN: usize = Self::FIELD.end();
}

const _: () = {
    assert!(HeaderFields::LEN == 8);
    assert!(EntryFields::LEN == 12);
    assert!(ItemFields::LEN == 12);
    assert!(MemberFields::LEN == 4);
};

#[derive(Clone, Copy, Debug)]
pub(super) struct TableOffsets {
    pub(super) entry_at: usize,
    pub(super) item_at: usize,
    pub(super) member_at: usize,
    pub(super) blob_at: usize,
}

impl TableOffsets {
    pub(super) fn calculate(
        entry_count: usize,
        item_count: usize,
        member_count: usize,
    ) -> Option<Self> {
        let presence_words = PresenceBitmap::word_count(entry_count);
        let entry_at = table_end(HeaderFields::LEN, presence_words, size_of::<u64>()).ok()?;
        let item_at = table_end(entry_at, entry_count, EntryFields::LEN).ok()?;
        let member_at = table_end(item_at, item_count, ItemFields::LEN).ok()?;
        let blob_at = table_end(member_at, member_count, MemberFields::LEN).ok()?;
        Some(Self {
            entry_at,
            item_at,
            member_at,
            blob_at,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Layout {
    pub(super) entry_count: usize,
    pub(super) item_count: usize,
    pub(super) member_count: usize,
    pub(super) offsets: TableOffsets,
    pub(super) blob_len: usize,
    pub(super) total_len: usize,
}

impl Layout {
    pub(super) fn new(
        entry_count: usize,
        item_count: usize,
        member_count: usize,
        blob_len: usize,
    ) -> Result<Self, ContextBytesError> {
        if entry_count > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "entries" });
        }
        if item_count > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "items" });
        }
        if u16::try_from(member_count).is_err() {
            return Err(ContextBytesError::TooMany { what: "members" });
        }

        let layout = Self::calculate(entry_count, item_count, member_count, blob_len)
            .ok_or(ContextBytesError::TooLarge)?;
        if layout.total_len > MAX_CONTEXT_LEN {
            return Err(ContextBytesError::TooLarge);
        }
        Ok(layout)
    }

    pub(super) fn read(bytes: &[u8]) -> Option<Self> {
        if HeaderFields::VERSION.read(bytes, 0)? != VERSION {
            return None;
        }
        let entry_count = usize::from(HeaderFields::ENTRY_COUNT.read(bytes, 0)?);
        let item_count = usize::from(HeaderFields::ITEM_COUNT.read(bytes, 0)?);
        let member_count = usize::from(HeaderFields::MEMBER_COUNT.read(bytes, 0)?);
        let offsets = TableOffsets::calculate(entry_count, item_count, member_count)?;
        Self::from_offsets(
            entry_count,
            item_count,
            member_count,
            bytes.len().checked_sub(offsets.blob_at)?,
            offsets,
        )
    }

    pub(super) fn calculate(
        entry_count: usize,
        item_count: usize,
        member_count: usize,
        blob_len: usize,
    ) -> Option<Self> {
        Self::from_offsets(
            entry_count,
            item_count,
            member_count,
            blob_len,
            TableOffsets::calculate(entry_count, item_count, member_count)?,
        )
    }

    pub(super) fn from_offsets(
        entry_count: usize,
        item_count: usize,
        member_count: usize,
        blob_len: usize,
        offsets: TableOffsets,
    ) -> Option<Self> {
        Some(Self {
            entry_count,
            item_count,
            member_count,
            offsets,
            blob_len,
            total_len: offsets.blob_at.checked_add(blob_len)?,
        })
    }

    pub(super) fn write_header(self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
        HeaderFields::VERSION.write(bytes, 0, VERSION)?;
        for (field, value) in [
            (HeaderFields::ENTRY_COUNT, self.entry_count),
            (HeaderFields::ITEM_COUNT, self.item_count),
            (HeaderFields::MEMBER_COUNT, self.member_count),
        ] {
            field.write_usize(bytes, 0, value)?;
        }
        Ok(())
    }

    pub(super) fn entry_offset(self, slot: usize) -> Result<usize, ContextBytesError> {
        table_offset(
            self.offsets.entry_at,
            slot,
            self.entry_count,
            EntryFields::LEN,
        )
    }

    pub(super) fn item_offset(self, index: usize) -> Result<usize, ContextBytesError> {
        table_offset(
            self.offsets.item_at,
            index,
            self.item_count,
            ItemFields::LEN,
        )
    }

    pub(super) fn member_offset(self, index: usize) -> Option<usize> {
        (index < self.member_count).then(|| self.offsets.member_at + index * MemberFields::LEN)
    }

    #[cfg(test)]
    pub(super) fn item_descriptor(self, bytes: &[u8], index: usize) -> Option<ItemDescriptor> {
        ItemDescriptor::read(bytes, self.item_offset(index).ok()?)
    }

    pub(super) fn entry_descriptor(self, bytes: &[u8], slot: usize) -> Option<EntryDescriptor> {
        EntryDescriptor::read(bytes, self.entry_offset(slot).ok()?)
    }

    pub(super) fn entry_present(self, bytes: &[u8], slot: usize) -> Option<bool> {
        if slot >= self.entry_count {
            return None;
        }
        PresenceBitmap::is_set_encoded(bytes.get(self.presence_section())?, slot)
    }

    pub(super) fn blob(self, bytes: &[u8]) -> Option<&[u8]> {
        bytes.get(self.blob_section())
    }

    pub(super) fn presence_section(self) -> Range<usize> {
        HeaderFields::LEN..self.offsets.entry_at
    }

    pub(super) fn entries_section(self) -> Range<usize> {
        self.offsets.entry_at..self.offsets.item_at
    }

    pub(super) fn items_section(self) -> Range<usize> {
        self.offsets.item_at..self.offsets.member_at
    }

    pub(super) fn members_section(self) -> Range<usize> {
        self.offsets.member_at..self.offsets.blob_at
    }

    pub(super) fn blob_section(self) -> Range<usize> {
        self.offsets.blob_at..self.total_len
    }
}

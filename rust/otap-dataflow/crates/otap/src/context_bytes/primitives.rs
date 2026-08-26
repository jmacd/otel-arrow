use super::*;

pub(super) const VERSION: u16 = 7;
pub(super) const MAX_CONTEXT_LEN: usize = u16::MAX as usize;
pub(super) const NO_FIELD: u16 = u16::MAX;
pub(super) const NO_SCHEMA_ITEM: u16 = u16::MAX;

pub(super) struct PresenceBitmap;

impl PresenceBitmap {
    pub(super) const WORD_BITS: usize = u64::BITS as usize;

    pub(super) const fn word_count(register_count: usize) -> usize {
        register_count.div_ceil(Self::WORD_BITS)
    }

    pub(super) const fn word_index(register: usize) -> usize {
        register / Self::WORD_BITS
    }

    pub(super) const fn mask(register: usize) -> u64 {
        1u64 << (register % Self::WORD_BITS)
    }

    pub(super) fn set_words(words: &mut [u64], register: usize) -> Result<(), ContextBytesError> {
        let word = words
            .get_mut(Self::word_index(register))
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        *word |= Self::mask(register);
        Ok(())
    }

    pub(super) fn is_set_encoded(bytes: &[u8], register: usize) -> Option<bool> {
        let word = read_u64(bytes, Self::word_index(register) * size_of::<u64>())?;
        Some(word & Self::mask(register) != 0)
    }

    pub(super) fn set_encoded(bytes: &mut [u8], register: usize) -> Result<(), ContextBytesError> {
        let at = Self::word_index(register) * size_of::<u64>();
        let word = read_u64(bytes, at).ok_or(ContextBytesError::InvalidEnvelope)?;
        write_u64(bytes, at, word | Self::mask(register))
    }
}

pub(super) trait Scalar: Copy {
    const WIDTH: usize;

    fn read(bytes: &[u8], at: usize) -> Option<Self>;
    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError>;
}

impl Scalar for u8 {
    const WIDTH: usize = size_of::<Self>();

    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        bytes.get(at).copied()
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_slice(bytes, at, &[self])
    }
}

impl Scalar for u16 {
    const WIDTH: usize = size_of::<Self>();

    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        read_u16(bytes, at)
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_u16(bytes, at, self)
    }
}

impl Scalar for u64 {
    const WIDTH: usize = size_of::<Self>();

    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        read_u64(bytes, at)
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_u64(bytes, at, self)
    }
}

#[derive(Clone, Copy)]
pub(super) struct Field<T> {
    offset: usize,
    value: PhantomData<fn() -> T>,
}

impl<T: Scalar> Field<T> {
    pub(super) const fn new(offset: usize) -> Self {
        Self {
            offset,
            value: PhantomData,
        }
    }

    pub(super) const fn end(self) -> usize {
        self.offset + T::WIDTH
    }

    pub(super) const fn at(self, base: usize) -> usize {
        base + self.offset
    }

    pub(super) fn read(self, bytes: &[u8], base: usize) -> Option<T> {
        T::read(bytes, self.at(base))
    }

    pub(super) fn write(
        self,
        bytes: &mut [u8],
        base: usize,
        value: T,
    ) -> Result<(), ContextBytesError> {
        value.write(bytes, self.at(base))
    }
}

impl Field<u16> {
    pub(super) fn write_usize(
        self,
        bytes: &mut [u8],
        base: usize,
        value: usize,
    ) -> Result<(), ContextBytesError> {
        self.write(
            bytes,
            base,
            u16::try_from(value).map_err(|_| ContextBytesError::TooLarge)?,
        )
    }
}

pub(super) type U8Field = Field<u8>;
pub(super) type U16Field = Field<u16>;
pub(super) type U64Field = Field<u64>;

#[derive(Clone, Copy)]
pub(super) struct BlobRangeField {
    offset: U16Field,
    len: U16Field,
}

impl BlobRangeField {
    pub(super) const fn new(offset: usize) -> Self {
        let offset = U16Field::new(offset);
        Self {
            len: U16Field::new(offset.end()),
            offset,
        }
    }

    pub(super) const fn end(self) -> usize {
        self.len.end()
    }

    pub(super) fn read(self, bytes: &[u8], base: usize) -> Option<BlobRange> {
        Some(BlobRange {
            offset: usize::from(self.offset.read(bytes, base)?),
            len: usize::from(self.len.read(bytes, base)?),
        })
    }

    pub(super) fn write(
        self,
        bytes: &mut [u8],
        base: usize,
        range: BlobRange,
    ) -> Result<(), ContextBytesError> {
        self.offset.write_usize(bytes, base, range.offset)?;
        self.len.write_usize(bytes, base, range.len)
    }
}

pub(super) fn table_end(
    start: usize,
    count: usize,
    width: usize,
) -> Result<usize, ContextBytesError> {
    count
        .checked_mul(width)
        .and_then(|len| start.checked_add(len))
        .ok_or(ContextBytesError::TooLarge)
}

pub(super) fn table_offset(
    start: usize,
    index: usize,
    count: usize,
    width: usize,
) -> Result<usize, ContextBytesError> {
    if index >= count {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    table_end(start, index, width)
}

pub(super) fn read_u16(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(read_array(bytes, at)?))
}

pub(super) fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(read_array(bytes, at)?))
}

pub(super) fn read_array<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..at.checked_add(N)?)?.try_into().ok()
}

pub(super) fn write_u16(bytes: &mut [u8], at: usize, value: u16) -> Result<(), ContextBytesError> {
    write_slice(bytes, at, &value.to_le_bytes())
}

pub(super) fn write_u64(bytes: &mut [u8], at: usize, value: u64) -> Result<(), ContextBytesError> {
    write_slice(bytes, at, &value.to_le_bytes())
}

pub(super) fn write_slice(
    bytes: &mut [u8],
    at: usize,
    value: &[u8],
) -> Result<(), ContextBytesError> {
    bytes
        .get_mut(
            at..at
                .checked_add(value.len())
                .ok_or(ContextBytesError::TooLarge)?,
        )
        .ok_or(ContextBytesError::InvalidEnvelope)?
        .copy_from_slice(value);
    Ok(())
}

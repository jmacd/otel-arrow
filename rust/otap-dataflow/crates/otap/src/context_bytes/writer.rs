use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum EnvelopeSection {
    Presence,
    Entries,
    Items,
    Members,
    Blob,
    Complete,
}

pub(super) struct EnvelopeWriter {
    pub(super) layout: Layout,
    pub(super) bytes: Vec<u8>,
    pub(super) next: EnvelopeSection,
}

impl EnvelopeWriter {
    pub(super) fn new(layout: Layout) -> Result<Self, ContextBytesError> {
        let mut bytes = vec![0; layout.total_len];
        layout.write_header(&mut bytes)?;
        Ok(Self {
            layout,
            bytes,
            next: EnvelopeSection::Presence,
        })
    }

    pub(super) fn presence(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Presence,
            EnvelopeSection::Entries,
            self.layout.presence_section(),
            write,
        )
    }

    pub(super) fn entries(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Entries,
            EnvelopeSection::Items,
            self.layout.entries_section(),
            write,
        )
    }

    pub(super) fn items(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Items,
            EnvelopeSection::Members,
            self.layout.items_section(),
            write,
        )
    }

    pub(super) fn members(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Members,
            EnvelopeSection::Blob,
            self.layout.members_section(),
            write,
        )
    }

    pub(super) fn blob(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Blob,
            EnvelopeSection::Complete,
            self.layout.blob_section(),
            write,
        )
    }

    pub(super) fn finish(self) -> Result<Vec<u8>, ContextBytesError> {
        if self.next != EnvelopeSection::Complete || self.bytes.len() != self.layout.total_len {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        Ok(self.bytes)
    }

    pub(super) fn write_section(
        &mut self,
        expected: EnvelopeSection,
        next: EnvelopeSection,
        range: Range<usize>,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        if self.next != expected {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        write(
            self.bytes
                .get_mut(range)
                .ok_or(ContextBytesError::InvalidEnvelope)?,
        )?;
        self.next = next;
        Ok(())
    }
}

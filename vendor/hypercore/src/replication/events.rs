//! events related to replication
use crate::{HypercoreError, common::BitfieldUpdate};
use async_broadcast::{InactiveReceiver, Receiver, Sender, broadcast};

static MAX_EVENT_QUEUE_CAPACITY: usize = 32;

/// Event emitted by [`crate::Hypercore::event_subscribe`]
#[derive(Debug, Clone)]
/// Emitted when [`crate::Hypercore::get`] is called when the block is missing.
pub struct Get {
    /// Index of the requested block
    pub index: u64,
    /// When the block is gotten this emits an event
    pub get_result: Sender<()>,
}

/// Emitted when
#[derive(Debug, Clone)]
pub struct DataUpgrade {}

/// Emitted when core gets new blocks
#[derive(Debug, Clone)]
pub struct Have {
    /// Starting index of the blocks we have
    pub start: u64,
    /// The number of blocks
    pub length: u64,
    /// TODO
    pub drop: bool,
}

impl From<&BitfieldUpdate> for Have {
    fn from(
        BitfieldUpdate {
            start,
            length,
            drop,
        }: &BitfieldUpdate,
    ) -> Self {
        Have {
            start: *start,
            length: *length,
            drop: *drop,
        }
    }
}

#[derive(Debug, Clone)]
/// Core events relevant to replication
pub enum Event {
    /// Emmited when core.get(i) happens for a missing block
    Get(Get),
    /// Emmitted when data.upgrade applied
    DataUpgrade(DataUpgrade),
    /// Emmitted when core gets new blocks
    Have(Have),
}

/// Derive From<msg> for Enum where enum variant and msg have the same name
macro_rules! impl_from_for_enum_variant {
    ($enum_name:ident, $variant_and_msg_name:ident) => {
        impl From<$variant_and_msg_name> for $enum_name {
            fn from(value: $variant_and_msg_name) -> Self {
                $enum_name::$variant_and_msg_name(value)
            }
        }
    };
}

impl_from_for_enum_variant!(Event, Get);
impl_from_for_enum_variant!(Event, DataUpgrade);
impl_from_for_enum_variant!(Event, Have);

#[derive(Debug)]
pub(crate) struct Events {
    /// Channel for core events
    pub(crate) channel: Sender<Event>,
    /// Kept around so `Events::channel` stays open.
    _receiver: InactiveReceiver<Event>,
}

impl Events {
    pub(crate) fn new() -> Self {
        let (mut channel, receiver) = broadcast(MAX_EVENT_QUEUE_CAPACITY);
        channel.set_await_active(false);
        let mut _receiver = receiver.deactivate();
        // Message sending is best effort. Is msg queue fills up, remove old messages to make place
        // for new ones.
        _receiver.set_overflow(true);
        Self { channel, _receiver }
    }

    /// The internal channel errors on send when no replicators are subscribed,
    /// For now we don't consider that an error, but just in case, we return a Result in case
    /// we want to change this or add another fail path later.
    pub(crate) fn send<T: Into<Event>>(&self, evt: T) -> Result<(), HypercoreError> {
        let _errs_when_no_replicators_subscribed = self.channel.try_broadcast(evt.into());
        Ok(())
    }

    /// Send a [`Get`] messages and return [`Receiver`] that will receive a message when block is
    /// gotten.
    pub(crate) fn send_on_get(&self, index: u64) -> Receiver<()> {
        let (mut tx, rx) = broadcast(1);
        tx.set_await_active(false);
        let _ = self.send(Get {
            index,
            get_result: tx,
        });
        rx
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::replication::CoreMethodsError;

    #[async_std::test]
    async fn test_events() -> Result<(), CoreMethodsError> {
        let mut core = crate::core::tests::create_hypercore_with_data(0).await?;

        // Check that appending data emits a DataUpgrade and Have event

        let mut rx = core.event_subscribe();
        let handle = async_std::task::spawn(async move {
            let mut out = vec![];
            loop {
                if out.len() == 2 {
                    return (out, rx);
                }
                if let Ok(evt) = rx.recv().await {
                    out.push(evt);
                }
            }
        });
        core.append(b"foo").await?;
        let (res, mut rx) = handle.await;
        assert!(matches!(res[0], Event::DataUpgrade(_)));
        assert!(matches!(
            res[1],
            Event::Have(Have {
                start: 0,
                length: 1,
                drop: false
            })
        ));
        // no messages in queue
        assert!(rx.is_empty());

        // Check that Hypercore::get for missing data emits a Get event

        let handle = async_std::task::spawn(async move {
            let mut out = vec![];
            loop {
                if out.len() == 1 {
                    return (out, rx);
                }
                if let Ok(evt) = rx.recv().await {
                    out.push(evt);
                }
            }
        });
        assert_eq!(core.get(1).await?, None);
        let (res, rx) = handle.await;
        assert!(matches!(
            res[0],
            Event::Get(Get {
                index: 1,
                get_result: _
            })
        ));
        // no messages in queue
        assert!(rx.is_empty());
        Ok(())
    }
}

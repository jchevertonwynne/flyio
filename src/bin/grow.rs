use anyhow::{Context, bail};
use enum_dispatch::enum_dispatch;
use flyio::{Body, Init, KvClient, Message, MsgIDProvider, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{select, sync::mpsc::Sender, time::MissedTickBehavior};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Clone)]
struct GrowNode {
    inner: Arc<GrowNodeInner>,
}

struct GrowNodeInner {
    id_provider: MsgIDProvider,
    propogate: AtomicU64,
    flushing: AtomicU64,
    key_state: AtomicU8,
    kv: KvClient,
    tasks: TaskTracker,
    cancel: CancellationToken,
}

impl GrowNodeInner {
    async fn add_to_shared_counter(&self, mut delta: u64) -> anyhow::Result<()> {
        let mut sleep_dur = Duration::from_millis(1);
        if self.key_state.load(Ordering::SeqCst) == 0 {
            self.key_state.store(1, Ordering::SeqCst);
        }

        let mut extra_added = 0;

        loop {
            let p = self.propogate.swap(0, Ordering::SeqCst);
            if p > 0 {
                self.flushing.fetch_add(p, Ordering::SeqCst);
                delta += p;
                extra_added += p;
            }
            let current = match self.kv.read("shared_counter").await {
                Ok(val) => {
                    self.key_state.store(2, Ordering::SeqCst);
                    val
                }
                Err(err) => {
                    if self.key_state.load(Ordering::SeqCst) == 2 {
                        eprintln!("failed to read shared counter value (state 2): {err}");
                        tokio::time::sleep(sleep_dur).await;
                        sleep_dur = sleep_dur.saturating_mul(2);
                        continue;
                    }

                    match self
                        .kv
                        .compare_and_swap("shared_counter", 0, delta, true)
                        .await
                    {
                        Ok(true) => {
                            self.key_state.store(2, Ordering::SeqCst);
                            self.flushing.fetch_sub(delta, Ordering::SeqCst);
                            return Ok(());
                        }
                        Ok(false) => {
                            self.key_state.store(2, Ordering::SeqCst);
                            continue;
                        }
                        Err(err) => {
                            eprintln!("failed to attempt cas for shared counter: {err}");
                            tokio::time::sleep(sleep_dur).await;
                            sleep_dur = sleep_dur.saturating_mul(2);
                            continue;
                        }
                    }
                }
            };

            let new = match current.checked_add(delta) {
                Some(n) => n,
                None => {
                    self.flushing.fetch_sub(extra_added, Ordering::SeqCst);
                    bail!("shared counter would overflow u64");
                }
            };

            match self
                .kv
                .compare_and_swap("shared_counter", current, new, false)
                .await
            {
                Ok(true) => {
                    self.flushing.fetch_sub(delta, Ordering::SeqCst);
                    break;
                }
                Ok(false) => {}
                Err(err) => {
                    eprintln!("failed to attempt cas for shared counter: {err}");
                    tokio::time::sleep(sleep_dur).await;
                    sleep_dur = sleep_dur.saturating_mul(2);
                    continue;
                }
            }

            eprintln!("cas for shared counter failed, retrying in {:?}", sleep_dur);
            tokio::time::sleep(sleep_dur).await;
            sleep_dur = sleep_dur.saturating_mul(2);
        }

        Ok(())
    }
}

impl Deref for GrowNode {
    type Target = GrowNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[enum_dispatch(HandleMessage)]
enum GrowNodePayload {
    Add(Add),
    AddOk(AddOk),
    Read(Read),
    ReadOk(ReadOk),
    Error(Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct Add {
    delta: u64,
}

impl HandleMessage for Add {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { delta: message } = self;
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: _,
                },
        } = msg;

        node.propogate.fetch_add(message, Ordering::SeqCst);

        let resp_msg_id = node.id_provider.id();

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: GrowNodePayload::AddOk(AddOk),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct AddOk;

impl HandleMessage for AddOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Read;

impl HandleMessage for Read {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: _,
                },
        } = msg;

        let resp_msg_id = node.id_provider.id();
        let propogate = node.propogate.load(Ordering::SeqCst);
        let flushing = node.flushing.load(Ordering::SeqCst);

        let value = if node.key_state.load(Ordering::SeqCst) < 2 {
            0
        } else {
            node.kv
                .read("shared_counter")
                .await
                .context("failed to send read request")?
        };

        let value = value + propogate + flushing;

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: GrowNodePayload::ReadOk(ReadOk { value }),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ReadOk {
    value: u64,
}

impl HandleMessage for ReadOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Error {
    code: u64,
    text: String,
}

impl HandleMessage for Error {
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &GrowNode,
        _tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { code, text } = self;
        eprintln!("error payload recieved: code = {code} text = {text}");

        Ok(())
    }
}

#[derive(Debug)]
enum SuppliedPayload {
    Tick,
}

#[enum_dispatch]
trait HandleMessage: Sized {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        _ = msg;
        _ = node;
        _ = tx;
        bail!("unexpected msg type")
    }
}

impl Node for GrowNode {
    type Payload = GrowNodePayload;
    type PayloadSupplied = SuppliedPayload;

    async fn from_init(
        _init: Init,
        kv: KvClient,
        id_provider: MsgIDProvider,
        tx: Sender<Self::PayloadSupplied>,
    ) -> anyhow::Result<Self> {
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();

        tracker.spawn({
            let cancel = cancel.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(100));
                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    select! {
                        _ = ticker.tick() => {
                            tx.send(SuppliedPayload::Tick)
                                .await
                                .context("failed to send msg")?;

                        }
                        _ = cancel.cancelled() => {
                            return Ok::<_, anyhow::Error>(())
                        }
                    }
                }
            }
        });

        Ok(GrowNode {
            inner: Arc::new(GrowNodeInner {
                id_provider,
                propogate: AtomicU64::new(0),
                flushing: AtomicU64::new(0),
                kv,
                tasks: tracker,
                cancel,
                key_state: AtomicU8::new(0),
            }),
        })
    }

    async fn handle(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let (payload, message) = msg.replace_payload(());

        payload.handle(message, self, tx).await?;

        Ok(())
    }

    async fn handle_supplied(
        &self,
        msg: Self::PayloadSupplied,
        _tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let SuppliedPayload::Tick = msg;

        let propogate = self.propogate.swap(0, Ordering::SeqCst);
        if propogate > 0 {
            self.flushing.fetch_add(propogate, Ordering::SeqCst);

            let res = self.add_to_shared_counter(propogate).await;

            if res.is_err() {
                self.flushing.fetch_sub(propogate, Ordering::SeqCst);
                res?;
            }
        }

        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        self.cancel.cancel();
        self.tasks.close();
        self.tasks.wait().await;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<GrowNode>()
        .await
        .inspect_err(|err| eprintln!("failed to run main: {err}"))
}

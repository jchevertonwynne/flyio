use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Body> {
    #[serde(rename = "src")]
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    #[serde(rename = "body")]
    pub body: Body,
}

impl<T> Message<T> {
    pub fn extract_body(self) -> (T, Message<()>) {
        self.replace_body(())
    }

    pub fn replace_body<U>(self, new_body: U) -> (T, Message<U>) {
        let Message { src, dst, body } = self;

        let msg = Message {
            src,
            dst,
            body: new_body,
        };

        (body, msg)
    }
}

impl<T> Message<Body<T>> {
    pub fn replace_payload<U>(self, new_payload: U) -> (T, Message<Body<U>>) {
        let (a, b) = self.extract_body();
        let (c, d) = a.replace_payload(new_payload);
        let (_, e) = b.replace_body(d);
        (c, e)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinBody {
    #[serde(rename = "msg_id")]
    pub incoming_msg_id: Option<u64>,
    #[serde(rename = "in_reply_to")]
    pub in_reply_to: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub incoming_msg_id: Option<u64>,
    #[serde(rename = "in_reply_to")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: Payload,
}

impl<T> Body<T> {
    pub fn extract_payload(self) -> (T, Body<()>) {
        self.replace_payload(())
    }

    pub fn replace_payload<U>(self, new_payload: U) -> (T, Body<U>) {
        let Body {
            incoming_msg_id,
            in_reply_to,
            payload,
        } = self;

        let body = Body {
            incoming_msg_id,
            in_reply_to,
            payload: new_payload,
        };

        (payload, body)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PayloadInit {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    #[serde(rename = "node_id")]
    pub node_id: String,
    #[serde(rename = "node_ids")]
    pub node_ids: HashSet<String>,
}

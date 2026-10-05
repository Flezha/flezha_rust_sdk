//! A graph's call to another relay: where it goes ([`CallTarget`]) and the
//! outcome that comes back on the Response channel ([`Response`]).

use std::any::Any;
use std::sync::Arc;

use flz_transport::errors::FlezhaError;
use serde::de::DeserializeOwned;

/// The relay a `call(...)` reaches: a named instance (or a partition group,
/// whose member the request's key chooses), or a factory's instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallTarget {
    pub(crate) target: Option<String>,
    pub(crate) factory: Option<String>,
    pub(crate) create: bool,
    pub(crate) channel: Option<String>,
    pub(crate) answer_channel: Option<String>,
}

impl CallTarget {
    /// The instance, or partition group, named `id`.
    pub fn id(id: impl Into<String>) -> Self {
        Self {
            target: Some(id.into()),
            factory: None,
            create: true,
            channel: None,
            answer_channel: None,
        }
    }

    /// An instance of `factory`: after a `key_by`, the one whose id is the
    /// request's key, started with the request when no instance holds it;
    /// without one, a new steps instance per call.
    pub fn factory(factory: impl Into<String>) -> Self {
        Self {
            target: None,
            factory: Some(factory.into()),
            create: true,
            channel: None,
            answer_channel: None,
        }
    }

    /// Whether a factory call starts the instance its key names when none
    /// holds it (the default); `false` answers `relay.not_found` instead.
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// The callee's entry channel the request enters on; its first when unset.
    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }

    /// The callee's channel whose output answers the call; its first output
    /// for the request when unset.
    pub fn answer_channel(mut self, channel: impl Into<String>) -> Self {
        self.answer_channel = Some(channel.into());
        self
    }
}

/// The outcome of one call, arriving as an input on the caller's Response
/// channel: the request as the call end wrote it, the callee's answer or the
/// error document that ended the call (`call.timeout` among them), the callee's
/// instance id and whether this call started it.
#[derive(Debug, Clone)]
pub struct Response<Req, Ans> {
    pub request: Req,
    pub result: Result<Ans, FlezhaError>,
    pub target: String,
    pub created: bool,
}

impl<Req, Ans> Response<Req, Ans>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Ans: DeserializeOwned + Send + Sync + 'static,
{
    /// Read one Response channel payload, as the Response channel does.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        decode_response(bytes)
    }
}

/// Decode one Response channel payload: the envelope, then its request and
/// answer with the rule an entry decodes and an output encodes by.
fn decode_response<Req, Ans>(bytes: &[u8]) -> Result<Response<Req, Ans>, String>
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Ans: DeserializeOwned + Send + Sync + 'static,
{
    let envelope = flz_core::call::Envelope::decode(bytes)?;
    Ok(Response {
        request: decode_value::<Req>(envelope.request)?,
        result: match envelope.result {
            Ok(answer) => Ok(decode_value::<Ans>(answer)?),
            Err(error) => Err(error),
        },
        target: envelope.target,
        created: envelope.created,
    })
}

/// A request or an answer decoded as `input::<T>` decodes its bytes.
fn decode_value<T: DeserializeOwned + Send + Sync + 'static>(bytes: Vec<u8>) -> Result<T, String> {
    let (decode, _parsed) = crate::channel::builder::default_entry_wrap::<T>();
    let value = decode(bytes)?
        .downcast::<T>()
        .unwrap_or_else(|_| unreachable!("the entry decode answers its own type"));
    Ok(Arc::try_unwrap(value)
        .unwrap_or_else(|_| unreachable!("a value just decoded is shared with nobody")))
}

/// The Response channel's entry decode.
pub(crate) fn response_wrap<Req, Ans>() -> crate::channel::EntryWrapFn
where
    Req: DeserializeOwned + Send + Sync + 'static,
    Ans: DeserializeOwned + Send + Sync + 'static,
{
    Arc::new(|bytes: Vec<u8>| {
        decode_response::<Req, Ans>(&bytes).map(|r| Arc::new(r) as Arc<dyn Any + Send + Sync>)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_request_and_a_json_answer_decode_to_their_types() {
        let fixture = r#"{"call":"c","target":"anna-42","created":true,
            "request":"YW5uYS00MjpoZWxsbw==","value":"eyJ0b2tlbnMiOjgxMn0="}"#;
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Usage {
            tokens: u32,
        }
        let response: Response<String, Usage> =
            decode_response(fixture.as_bytes()).expect("decodes");
        assert_eq!(response.request, "anna-42:hello");
        assert_eq!(response.result.expect("ok"), Usage { tokens: 812 });
    }
}

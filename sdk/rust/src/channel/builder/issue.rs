//! Registration-time refusals for one operative's channel declarations.

/// Reasons `StreamOperative::channels(c)` declarations fail at registration time.
#[derive(Debug, thiserror::Error)]
pub enum TopologyIssue {
    #[error("channel `{0}` declared more than once on this operative")]
    DuplicateChannel(&'static str),

    #[error(
        "node identity `{name}` is declared by two distinct nodes; a node name must be unique \
         per operative and distinct from every procedure name()"
    )]
    NodeIdentityCollision { name: &'static str },

    #[error("branch(...) requires at least two arms; {arms} given")]
    BranchTooFewArms { arms: usize },

    #[error(
        "procedure `{procedure}` is on a cycle; an operative topology is a DAG — a node's \
         output may never reach its own input"
    )]
    Cycle { procedure: &'static str },

    #[error("procedure `{procedure}` is not reachable from any entry channel")]
    UnreachableProcedure { procedure: &'static str },

    #[error(
        "procedure `{name}` declares non-empty entity_keys() but does not override entities_for \
         (set ENTITIES_FOR_OVERRIDDEN = true)"
    )]
    EntitiesForMissing { name: &'static str },

    #[error(
        "channel `{channel}` declares .key_by(...) into procedure `{procedure}`, which declares a \
         non-empty entity_keys() (static mode); static keying routes by entities_for and may name \
         several Entities, edge keying names exactly one — declare one OR the other, not both"
    )]
    KeyByOnUnkeyableTarget {
        channel: &'static str,
        procedure: &'static str,
    },

    #[error("channel `{channel}` declares .key_by(...) twice before one `.into(...)`")]
    DuplicateKeyBy { channel: &'static str },

    #[error(
        "channel `{channel}` declares a transform or .key_by(...) after a plugin sink; the sink \
         serializes the channel's own value, so nothing downstream of it can be reshaped"
    )]
    TransformAfterTerminal { channel: &'static str },

    #[error(
        "channel `{channel}` declares a transform after .key_by(...); the key is evaluated on the \
         final post-transform value, so it must be the last link before `.into(...)`"
    )]
    TransformAfterKeyBy { channel: &'static str },

    #[error(
        "channel `{channel}` declares a transform or .key_by(...) that is never closed by \
         `.into(...)`; a transform chain is stored on the consumer edge it terminates in, so a \
         dangling one would silently do nothing"
    )]
    DanglingTransformChain { channel: &'static str },

    #[error(
        "channel `{channel}` feeds procedure `{procedure}` without a .key_by(...), but another \
         edge keys it into dynamic mode; every edge into a dynamic-mode procedure must name the \
         Entity its inputs reach"
    )]
    UnkeyedEdgeIntoDynamicProcedure {
        channel: &'static str,
        procedure: &'static str,
    },

    #[error(
        "procedure `{procedure}` declared mailbox_capacity = 0; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityZero { procedure: &'static str },

    #[error(
        "procedure `{procedure}` declared mailbox_capacity = {value}; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityTooLarge {
        procedure: &'static str,
        value: usize,
    },

    #[error(
        "channel `{channel}` was declared with `sink(...)` more than once; \
         only one plugin sink per channel is allowed"
    )]
    DuplicatePluginSink { channel: &'static str },

    #[error(
        "channel `{channel}` is declared .keyed() but has no plugin wire; output identity \
         stamps only exist on plugin-bound frames (add .sink(...))"
    )]
    KeyedChannelWithoutPluginWire { channel: &'static str },

    #[error(
        "procedure `{procedure}` declares clock `{clock_type}` with `type Change = NoChange`; \
         the runtime cannot serve it (logical time is the durable change seq, which never \
         advances without persistent changes)"
    )]
    UnservableClock {
        procedure: &'static str,
        clock_type: &'static str,
    },

    #[error(
        "channel `{channel}` has a plugin wire but is not declared .keyed(); the \
         effectively-once tier deduplicates by output identity, so every plugin-bound \
         channel of an effectively_once() operative must opt in with .keyed()"
    )]
    UnkeyedChannelUnderEffectivelyOnce { channel: &'static str },

    #[error(
        "channel `{channel}` declared retry_max = 0; at least one attempt is required \
         (use ProcedureError::non_retryable to park without retries)"
    )]
    RetryMaxZero { channel: &'static str },

    #[error(
        "channel `{channel}` declares .on_full({policy}) on an operative that declares \
         effectively_once(); a shed input's offset is still advanced past by the tier's \
         watermark, so the loss is permanent and reported nowhere — declare one or the other, \
         not both (a correction window implies the tier, so it is refused here too)"
    )]
    DroppingPolicyUnderEffectivelyOnce {
        channel: &'static str,
        policy: &'static str,
    },

    #[error(
        "operative declares correction_window() without effectively_once(); correction \
         replays the input log against a rewound baseline, and only the tier journals \
         inputs before forwarding them (an at-least-once operative has no durable input \
         to replay)"
    )]
    CorrectionWithoutTier,

    #[error(
        "operative declares correction_window() but procedure `{procedure}` declares clock \
         `{clock_type}`; correction orders a replayed slice by event time, which only \
         `EventTimeFrom` yields"
    )]
    CorrectionWithoutEventTime {
        procedure: &'static str,
        clock_type: &'static str,
    },

    #[error("operative declares correction_window(0); a zero window can correct nothing")]
    CorrectionWindowZero,

    #[error(
        "operative declares checkpoint_every(0); an interval that closes every zero inputs is \
         not an interval — omit it to close only on an explicit ctx.checkpoint()"
    )]
    CheckpointEveryZero,

    #[error(
        "operative declares checkpoint_ack() without effectively_once(); the checkpoint is \
         where the tier's consumed-offset advances, and an at-least-once operative has no \
         durable offset to hold back (nor a durable input log to replay the interval from)"
    )]
    CheckpointAckWithoutTier,

    #[error(
        "merge into procedure `{procedure}` declares {sources} source channel(s); a merge \
         consumes two or more — wire a single source with `.into::<Proc>()` instead"
    )]
    MergeTooFewSources {
        procedure: &'static str,
        sources: usize,
    },

    #[error(
        "merge subscribes channel `{channel}` twice; each merge source must be a distinct \
         channel (one mapping per channel into the merged input)"
    )]
    DuplicateMergeSource { channel: &'static str },

    #[error(
        "channel `{channel}` already feeds procedure `{procedure}`; an edge carries exactly one \
         transform and one key or neither, so a re-declaration where either side carries one is \
         refused rather than silently keeping the first"
    )]
    EdgeAlreadyWired {
        channel: &'static str,
        procedure: &'static str,
    },
}

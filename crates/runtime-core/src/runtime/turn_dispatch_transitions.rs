use crate::TurnDispatchState;

pub(super) fn is_valid_dispatch_transition(from: TurnDispatchState, to: TurnDispatchState) -> bool {
    use TurnDispatchState::{Dispatched, Dispatching, NotDispatched, Pending, Unknown};

    matches!(
        (from, to),
        (Pending, Pending | Dispatching | NotDispatched)
            | (
                Dispatching,
                Dispatching | Dispatched | NotDispatched | Unknown
            )
            | (Dispatched, Dispatched)
            | (NotDispatched, NotDispatched)
            | (Unknown, Unknown | Dispatched | NotDispatched)
    )
}

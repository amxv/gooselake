//! Feature-gated P02 observer listener. This module is not present in the
//! default Goosetower build and never adds routes to the product listener.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};

use crate::gateway::{GatewayState, MaterializerObserverSnapshot, ServedFrameDebug};

const CONTROL_HEADER: &str = "x-gooseweb-verification-control";

#[derive(Clone)]
struct ObserverState {
    gateway: Arc<GatewayState>,
    secret: Arc<str>,
}

pub fn build_tower_observer_router(
    gateway: Arc<GatewayState>,
    secret: impl Into<Arc<str>>,
) -> Router {
    Router::new()
        .route("/__verification/v1/tower/materializer", get(materializer))
        .route("/__verification/v1/tower/frames", get(frames))
        .with_state(ObserverState {
            gateway,
            secret: secret.into(),
        })
}

async fn materializer(
    State(state): State<ObserverState>,
    headers: HeaderMap,
) -> Result<Json<Vec<MaterializerObserverSnapshot>>, StatusCode> {
    authorize(&state, &headers)?;
    Ok(Json(
        state.gateway.verification_materializer_observer().await,
    ))
}

async fn frames(
    State(state): State<ObserverState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ServedFrameDebug>>, StatusCode> {
    authorize(&state, &headers)?;
    Ok(Json(state.gateway.debug_served_frames().await))
}

fn authorize(state: &ObserverState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let supplied = headers
        .get(CONTROL_HEADER)
        .and_then(|value| value.to_str().ok());
    (supplied == Some(state.secret.as_ref()))
        .then_some(())
        .ok_or(StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn observer_is_secret_gated_on_separate_verification_router() {
        let config = Arc::new(crate::config::GoosetowerConfig::default());
        let gateway = Arc::new(GatewayState::new(config).unwrap());
        let router = build_tower_observer_router(gateway, Arc::<str>::from("p02-secret"));
        let hidden = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/__verification/v1/tower/materializer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);
        let visible = router
            .oneshot(
                Request::builder()
                    .uri("/__verification/v1/tower/materializer")
                    .header(CONTROL_HEADER, "p02-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(visible.status(), StatusCode::OK);
    }
}

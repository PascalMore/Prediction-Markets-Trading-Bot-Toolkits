//! The adapter's request methods.
//!
//! These four methods are the whole of the adapter's outbound traffic, and
//! they carry the two rules that matter:
//!
//! - a read may be sent once more with a fresh token, and
//! - a write is never sent again, not even with the same hash.
//!
//! They live here, apart from the rest of the adapter, so that the guarded
//! controlled path can reach the last `await` of a write without the rest of
//! the adapter's size growing. The bodies are the adapter's own; only their
//! home changed.

use super::{
    ApiError, Envelope, PredictFunClient, PredictFunTrader, ReplayPolicy,
};
use crate::service::venue::VenueError;
use anyhow::{Context, Result};
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::debug;

impl PredictFunClient {
    /// Send one request and return its body as text.
    ///
    /// A synthetic transport replaces the network in tests: it never reaches
    /// a venue, and it can be made to hang exactly where a real request would.
    pub(super) async fn request_text(
        &self,
        method: Method,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
    ) -> Result<String> {
        #[cfg(test)]
        if let Some(transport) = &self.synthetic {
            let result = transport.rest(&method, path, bearer, body);
            if method == Method::POST && path == "/orders" && transport.pending_post() {
                std::future::pending::<()>().await;
            }
            return result;
        }
        let url = format!("{}{}", self.deployment.api_base, path);
        let mut req = self.http.request(method.clone(), &url);
        if let Some(key) = &self.api_key {
            req = req.header("x-api-key", key);
        }
        if let Some(token) = bearer {
            req = req.bearer_auth(token);
        }
        if let Some(body) = body {
            req = req.json(body);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("{method} {path}"))?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(ApiError { status, body: text })
                .with_context(|| format!("{method} {path}"));
        }
        Ok(text)
    }

    /// Send one request and parse its envelope.
    pub(super) async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        bearer: Option<&str>,
        body: Option<&Value>,
    ) -> Result<Envelope<T>> {
        let text = self
            .request_text(method.clone(), path, bearer, body)
            .await?;
        serde_json::from_str(&text).with_context(|| format!("parsing {method} {path} response"))
    }

    /// Send one unauthenticated request and parse its envelope.
    pub(super) async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<Envelope<T>> {
        self.request(Method::GET, path, None, None).await
    }
}

impl PredictFunTrader {
    /// Send one authenticated request and return its body as text.
    ///
    /// `replay` decides what happens after a 401. A read may be sent once
    /// more with a fresh token: a token isn't part of the order. A write is
    /// never sent again: the venue may already have taken it.
    pub(crate) async fn authed_text(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        replay: ReplayPolicy,
    ) -> Result<String> {
        let token = self.token().await?;
        match self
            .client
            .request_text(method.clone(), path, Some(&token), body)
            .await
        {
            Err(e)
                if e.downcast_ref::<ApiError>()
                    .is_some_and(|a| a.status == StatusCode::UNAUTHORIZED) =>
            {
                *self.jwt.write().await = None;
                if replay == ReplayPolicy::NoReplay {
                    // The request may have reached the venue. The caller keeps
                    // the order's identity and looks it up instead.
                    return Err(VenueError::new(
                        crate::service::venue::VenueErrorCode::SubmissionUnknown,
                        "authed_text",
                        "POST_NOT_REPLAYED",
                    )
                    .into());
                }
                debug!("JWT rejected; signing in again");
                *self.jwt.write().await = None;
                let token = self.token().await?;
                self.client
                    .request_text(method, path, Some(&token), body)
                    .await
            }
            other => other,
        }
    }

    /// Send one authenticated read and parse its envelope.
    pub(crate) async fn authed<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Envelope<T>> {
        let text = self
            .authed_text(method.clone(), path, body, ReplayPolicy::ReadOnceRefresh)
            .await?;
        serde_json::from_str(&text).with_context(|| format!("parsing {method} {path} response"))
    }

    /// Send an already authenticated controlled POST exactly once.
    /// All preparation awaits precede this method. Only the first poll enters
    /// State -> handle -> gate; Pending is already a possible submission.
    pub(super) async fn controlled_write<T: DeserializeOwned>(
        &self,
        token: &str,
        body: &Value,
        handle: &crate::service::venue::AttemptHandle,
        guard: &dyn crate::service::venue::ControlledSendGuard,
    ) -> Result<Envelope<T>> {
        use std::future::{poll_fn, Future};
        use std::task::Poll;
        let mut request = std::pin::pin!(self.client.request_text(
            Method::POST, "/orders", Some(token), Some(body)));
        let mut first = true;
        let text = poll_fn(|cx| {
            if !first { return request.as_mut().poll(cx); }
            // Never construct a second request after handoff, even when its
            // first poll returns Pending or an immediate error.
            let mut response = None;
            let permitted = guard.with_send_permit(handle, &mut |record, check_clock| {
                self.gate.with_checked_prepared_attempt(record, &mut || check_clock().map_err(|_| {
                    VenueError::new(crate::service::venue::VenueErrorCode::InvalidInput,
                        "controlled_send", "INPUT_CHANGED")
                }), &mut || {
                    first = false;
                    response = Some(request.as_mut().poll(cx));
                })?;
                Ok(())
            });
            match permitted {
                Err(error) => Poll::Ready(Err(error)),
                Ok(()) => response.unwrap_or_else(|| Poll::Ready(Err(
                    anyhow::anyhow!("EXECUTION_NOT_ENABLED:FIRST_POLL_MISSING")))),
            }
        }).await;
        match text {
            Err(error) if error.downcast_ref::<ApiError>()
                .is_some_and(|error| error.status == StatusCode::UNAUTHORIZED) => {
                *self.jwt.write().await = None;
                Err(VenueError::new(crate::service::venue::VenueErrorCode::SubmissionUnknown,
                    "controlled_write", "POST_NOT_REPLAYED").into())
            }
            other => serde_json::from_str(&other?).context("parsing POST /orders response"),
        }
    }

    /// An authenticated write. It is sent once, and never again.
    pub(crate) async fn authed_write<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Envelope<T>> {
        let text = self
            .authed_text(method.clone(), path, body, ReplayPolicy::NoReplay)
            .await?;
        serde_json::from_str(&text).with_context(|| format!("parsing {method} {path} response"))
    }
}

/// The deployment of the client, for the controlled path's own checks.
pub(super) fn deployment_of(client: &PredictFunClient) -> &super::Deployment {
    &client.deployment
}

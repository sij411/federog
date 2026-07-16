use axum::{
    Form, Router,
    extract::State,
    http::{StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use chrono::{SecondsFormat, Utc};
use feder_runtime_server::{
    AppState, Error, InboxAuthPolicy, RuntimeConfig, StorageConfig,
    actor::actor,
    inbox::inbox,
    storage::RuntimeStore,
    webfinger::webfinger,
};
use feder_vocab::{Iri, Reference};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

const USERNAME: &str = "alice";
const BIND: &str = "127.0.0.1:3000";
const BASE_URL: &str = "http://127.0.0.1:3000";

#[derive(Clone)]
struct FederogState {
    runtime: AppState,
    posts: Arc<Mutex<Vec<LocalPost>>>,
}

#[derive(Clone)]
struct LocalPost {
    note: feder_vocab::Note,
    activity: feder_vocab::Create<feder_vocab::Note>,
}

#[derive(Deserialize)]
struct PostForm {
    content: String,
}

fn runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        bind: BIND.parse().expect("valid bind address"),
        actor_id: iri(&format!("{BASE_URL}/users/{USERNAME}")),
        inbox: iri(&format!("{BASE_URL}/users/{USERNAME}/inbox")),
        outbox: iri(&format!("{BASE_URL}/users/{USERNAME}/outbox")),
        username: USERNAME.to_string(),
        handle_host: BIND.to_string(),
        inbox_auth_policy: InboxAuthPolicy::AllowUnsignedInsecureDev,
        storage: StorageConfig::InMemory,
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = runtime_config();
    let bind = config.bind;
    let actor_id = config.actor_id.clone();
    let state = FederogState {
        runtime: AppState::from_config(config)?,
        posts: Arc::new(Mutex::new(Vec::new())),
    };
    let app = build_router(state);

    tracing::info!(bind = %bind, actor = %actor_id, "starting federog");

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(Error::Bind)?;
    axum::serve(listener, app).await.map_err(Error::Serve)?;

    Ok(())
}

fn build_router(state: FederogState) -> Router {
    Router::new()
        .route("/", get(home).post(create_post))
        .route("/healthz", get(healthz))
        .route("/.well-known/webfinger", get(runtime_webfinger))
        .route("/users/{username}", get(runtime_actor))
        .route("/users/{username}/inbox", post(runtime_inbox))
        .route("/users/{username}/followers", get(followers))
        .route("/users/{username}/outbox", get(outbox))
        .with_state(state)
}

async fn healthz() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn home(State(state): State<FederogState>) -> Result<Html<String>, StatusCode> {
    let posts = state
        .posts
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let post_items = posts
        .iter()
        .rev()
        .map(|post| {
            format!(
                r#"<article class="post"><p>{}</p><small>{}</small></article>"#,
                escape(post.note.content.as_deref().unwrap_or("")),
                escape(post.note.published.as_deref().unwrap_or(""))
            )
        })
        .collect::<Vec<_>>();

    Ok(Html(page(
        "Federog",
        &format!(
            r#"
            <section class="composer">
                <h1>{}</h1>
                <p class="handle">@{}@{}</p>
                <form method="post" action="/">
                    <textarea name="content" rows="5" maxlength="500" required></textarea>
                    <button type="submit">Post</button>
                </form>
            </section>
            <section>
                <h2>Posts</h2>
                {}
            </section>
            "#,
            escape(&state.runtime.username),
            escape(&state.runtime.username),
            escape(&state.runtime.handle_host),
            if post_items.is_empty() {
                r#"<p class="empty">No posts yet.</p>"#.to_string()
            } else {
                post_items.join("\n")
            }
        ),
    )))
}

async fn create_post(
    State(state): State<FederogState>,
    Form(form): Form<PostForm>,
) -> Result<Redirect, StatusCode> {
    let content = form.content.trim();
    if content.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let now = Utc::now();
    let slug = now.timestamp_millis();
    let note_id = iri(&format!("{BASE_URL}/users/{USERNAME}/posts/{slug}"));
    let create_id = iri(&format!("{BASE_URL}/users/{USERNAME}/activities/create/{slug}"));

    let mut note = feder_vocab::Note::new(note_id);
    note.attributed_to = Some(Reference::id(state.runtime.local_actor.id.clone()));
    note.content = Some(content.to_string());
    note.published = Some(now.to_rfc3339_opts(SecondsFormat::Secs, true));
    let activity = feder_vocab::Create::new(
        create_id,
        Reference::id(state.runtime.local_actor.id.clone()),
        Reference::object(note.clone()),
    );

    let decision = feder_core::Decision {
        state_changes: vec![
            feder_core::StateChange::StoreObject {
                object: feder_core::Object::Note(note.clone()),
            },
            feder_core::StateChange::StoreActivity {
                activity: feder_core::Activity::CreateNote(activity.clone()),
            },
        ],
        effects: Vec::new(),
    };
    state
        .runtime
        .store
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .apply_decision(&decision)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .posts
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .push(LocalPost { note, activity });

    Ok(Redirect::to("/"))
}

async fn followers(State(state): State<FederogState>) -> Result<Html<String>, StatusCode> {
    let followers = state
        .runtime
        .store
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .list_followers(&state.runtime.local_actor.id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let body = if followers.is_empty() {
        r#"<p class="empty">No followers yet.</p>"#.to_string()
    } else {
        format!(
            "<ul>{}</ul>",
            followers
                .iter()
                .map(|follower| format!("<li>{}</li>", escape(follower.follower.as_str())))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };

    Ok(Html(page("Followers", &body)))
}

async fn outbox(State(state): State<FederogState>) -> Result<Response, StatusCode> {
    let posts = state
        .posts
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let activities = posts
        .iter()
        .rev()
        .map(|post| serde_json::to_value(&post.activity))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok((
        [(header::CONTENT_TYPE, "application/activity+json")],
        serde_json::json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "OrderedCollection",
            "id": state.runtime.local_actor.outbox,
            "totalItems": activities.len(),
            "orderedItems": activities,
        })
        .to_string(),
    )
        .into_response())
}

async fn runtime_webfinger(
    State(state): State<FederogState>,
    query: axum::extract::Query<feder_runtime_server::webfinger::WebFingerQuery>,
) -> Result<Response, StatusCode> {
    webfinger(State(state.runtime), query).await
}

async fn runtime_actor(
    State(state): State<FederogState>,
    path: axum::extract::Path<String>,
) -> Result<Response, StatusCode> {
    actor(State(state.runtime), path).await
}

async fn runtime_inbox(
    State(state): State<FederogState>,
    path: axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    method: axum::http::Method,
    uri: axum::http::Uri,
    body: axum::body::Bytes,
) -> Result<Response, StatusCode> {
    inbox(State(state.runtime), path, headers, method, uri, body).await
}

fn page(title: &str, body: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{}</title>
    <style>
        body {{ margin: 0; font: 16px/1.5 system-ui, sans-serif; background: #f7f7f4; color: #202124; }}
        main {{ width: min(760px, calc(100vw - 32px)); margin: 40px auto; }}
        nav {{ display: flex; gap: 16px; margin-bottom: 24px; }}
        a {{ color: #315f72; }}
        h1, h2 {{ line-height: 1.1; }}
        textarea {{ box-sizing: border-box; width: 100%; resize: vertical; padding: 12px; border: 1px solid #b9b9b2; border-radius: 6px; font: inherit; background: #fff; }}
        button {{ margin-top: 10px; padding: 8px 14px; border: 1px solid #214657; border-radius: 6px; background: #315f72; color: white; font: inherit; cursor: pointer; }}
        .handle, .empty, small {{ color: #666861; }}
        .composer, .post {{ border-bottom: 1px solid #d9d9d2; padding-bottom: 24px; margin-bottom: 24px; }}
        .post p {{ white-space: pre-wrap; }}
    </style>
</head>
<body>
    <main>
        <nav><a href="/">Home</a><a href="/users/alice">Actor</a><a href="/users/alice/followers">Followers</a><a href="/users/alice/outbox">Outbox</a></nav>
        {}
    </main>
</body>
</html>"#,
        escape(title),
        body
    )
}

fn iri(value: &str) -> Iri {
    value.parse().expect("valid IRI")
}

fn escape(value: &str) -> String {
    html_escape::encode_text(value).to_string()
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use tower::ServiceExt;

    use super::*;

    fn test_state() -> FederogState {
        FederogState {
            runtime: AppState::from_config(runtime_config()).expect("build app state"),
            posts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[tokio::test]
    async fn actor_route_uses_runtime_state() {
        let app = build_router(test_state());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/users/alice")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/activity+json"
        );
    }

    #[tokio::test]
    async fn post_is_added_to_home_and_outbox() {
        let app = build_router(test_state());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("content=hello%20feder"))
                    .expect("valid request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SEE_OTHER);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/users/alice/outbox")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json outbox");

        assert_eq!(json["type"], "OrderedCollection");
        assert_eq!(json["totalItems"], 1);
        assert_eq!(json["orderedItems"][0]["type"], "Create");
        assert_eq!(json["orderedItems"][0]["object"]["content"], "hello feder");
    }
}

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};

use axum::{
    Form, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, Method, StatusCode, Uri, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use feder_core::{Activity, FederConfig, FederCore, Recipients, SendActivity, UserCreateNote};
use feder_runtime_server::{
    AppState as FederState, InboxAuthPolicy, OutboundAddressPolicy, RuntimeConfig, StorageConfig,
    followers::followers as feder_followers,
    inbox::inbox as feder_inbox,
    object::get_object as feder_object,
    send::ActivitySender,
    storage::{RuntimeStore, StoredFollower},
    webfinger::{WebFingerQuery, webfinger as feder_webfinger},
};
use feder_vocab::{Actor, Endpoints, Follow, Iri, Reference, References};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use url::Url;

const PUBLIC_BIND: &str = "0.0.0.0:3000";
const ADMIN_BIND: &str = "127.0.0.1:3001";
const DB_PATH: &str = "microblog.sqlite3";
const DEFAULT_PUBLIC_ORIGIN: &str = "https://fedora.tuatara-lenok.ts.net";

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    feder: Arc<RwLock<Option<FederState>>>,
}

#[derive(Clone, Debug)]
struct Account {
    username: String,
    name: Option<String>,
    handle: String,
}

struct FollowerProfile {
    uri: String,
    name: Option<String>,
    handle: String,
}

struct PostRecord {
    uri: String,
    content: String,
    url: Option<String>,
    created: String,
}

#[derive(Deserialize)]
struct SetupForm {
    username: String,
    name: String,
}

#[derive(Deserialize)]
struct PostForm {
    content: String,
}

#[derive(Deserialize)]
struct FollowForm {
    actor: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(log_filter).init();

    let db = open_db(DB_PATH)?;
    let feder = load_account_from_db(&db)?
        .map(|account| build_feder_state(&account))
        .transpose()?;
    let state = AppState {
        db: Arc::new(Mutex::new(db)),
        feder: Arc::new(RwLock::new(feder)),
    };
    let public_app = Router::new()
        .route("/", get(public_home))
        .route("/.well-known/webfinger", get(webfinger))
        .route("/inbox", post(shared_inbox))
        .route("/users/{username}", get(profile))
        .route("/users/{username}/followers", get(followers))
        .route("/users/{username}/inbox", post(personal_inbox))
        .route("/users/{username}/posts/{id}", get(post_page))
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(state.clone());
    let admin_app = Router::new()
        .route("/", get(home))
        .route("/setup", get(setup_form).post(create_account))
        .route("/users/{username}", get(profile))
        .route("/users/{username}/followers", get(followers))
        .route("/users/{username}/following", post(follow_actor))
        .route("/users/{username}/posts", post(create_post))
        .route("/users/{username}/posts/{id}", get(post_page))
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(state);

    let public_bind: SocketAddr = PUBLIC_BIND.parse()?;
    let admin_bind: SocketAddr = ADMIN_BIND.parse()?;
    tracing::info!(bind = %public_bind, "starting public federog listener");
    tracing::info!(bind = %admin_bind, "starting private federog admin listener");

    let public_listener = tokio::net::TcpListener::bind(public_bind).await?;
    let admin_listener = tokio::net::TcpListener::bind(admin_bind).await?;
    tokio::try_join!(
        axum::serve(public_listener, public_app),
        axum::serve(admin_listener, admin_app),
    )?;

    Ok(())
}

async fn public_home(State(state): State<AppState>) -> Result<Redirect, StatusCode> {
    let account = load_account(&state)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Redirect::to(&format!("/users/{}", account.username)))
}

fn open_db(path: &str) -> rusqlite::Result<Connection> {
    let db = Connection::open(path)?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "foreign_keys", "ON")?;
    db.execute_batch(include_str!("schema.sql"))?;
    Ok(db)
}

async fn home(State(state): State<AppState>) -> Result<Html<String>, Redirect> {
    let Some(account) = load_account(&state).map_err(|_| Redirect::to("/setup"))? else {
        return Err(Redirect::to("/setup"));
    };

    Ok(Html(layout(&home_html(&account))))
}

async fn setup_form(State(state): State<AppState>) -> Result<Html<String>, Redirect> {
    if load_account(&state)
        .map_err(|_| Redirect::to("/setup"))?
        .is_some()
    {
        return Err(Redirect::to("/"));
    }

    Ok(Html(layout(&setup_form_html())))
}

async fn create_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<SetupForm>,
) -> Redirect {
    let Ok(existing_account) = load_account(&state) else {
        return Redirect::to("/setup");
    };
    if existing_account.is_some() {
        return Redirect::to("/");
    }

    let username = form.username.trim();
    if !is_valid_username(username) {
        return Redirect::to("/setup");
    }
    let name = form.name.trim();
    if name.is_empty() {
        return Redirect::to("/setup");
    }

    let origin = request_origin(&headers);
    let actor_uri = format!("{origin}/users/{username}");
    let handle = format!("@{username}@{}", request_host(&headers));
    let inbox_url = format!("{actor_uri}/inbox");
    let shared_inbox_url = format!("{origin}/inbox");

    let result = state
        .db
        .lock()
        .map_or(Err(rusqlite::Error::InvalidQuery), |mut db| {
            db.transaction().and_then(|tx| {
                tx.execute(
                    "INSERT OR REPLACE INTO users (id, username) VALUES (1, ?1)",
                    params![username],
                )?;
                tx.execute(
                    r#"
                INSERT OR REPLACE INTO actors
                  (user_id, uri, handle, name, inbox_url, shared_inbox_url, url)
                VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                    params![
                        actor_uri,
                        handle,
                        name,
                        inbox_url,
                        shared_inbox_url,
                        actor_uri
                    ],
                )?;
                tx.commit()
            })
        });
    if result.is_err() {
        return Redirect::to("/setup");
    }

    let account = Account {
        username: username.to_string(),
        name: Some(name.to_string()),
        handle,
    };
    let Ok(feder) = build_feder_state(&account) else {
        return Redirect::to("/");
    };
    let Ok(mut current_feder) = state.feder.write() else {
        return Redirect::to("/");
    };
    *current_feder = Some(feder);

    Redirect::to("/")
}

async fn profile(
    State(state): State<AppState>,
    Path(username): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let account = load_account_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    if wants_activity_json(&headers) {
        let actor = actor_json(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        return Ok((
            [(header::CONTENT_TYPE, "application/activity+json")],
            actor.to_string(),
        )
            .into_response());
    }

    let follower_count = load_followers(&state, &account)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .len();
    let posts =
        load_posts(&state, &account.username).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let profile = profile_html(
        &display_name(&account),
        &account.username,
        &format!("@{}@{}", account.username, request_host(&headers)),
        follower_count,
    );
    let posts = post_list_html(&posts, &account);

    Ok(Html(layout(&format!("{profile}{posts}"))).into_response())
}

async fn followers(
    State(state): State<AppState>,
    Path(username): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let account = load_account_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let feder = current_feder(&state).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if wants_activity_json(&headers) {
        let feder =
            feder_for_account(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let response = feder_followers(State(feder), Path(username), headers.clone()).await?;
        if response.status() != StatusCode::NOT_ACCEPTABLE {
            return Ok(response);
        }
    }

    let stored_followers = load_followers_from_feder(&feder, &account)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut profiles = Vec::with_capacity(stored_followers.len());

    for stored in stored_followers {
        let uri = stored.follower.to_string();
        let profile = match feder.actor_resolver.resolve(&stored.follower).await {
            Ok(actor) => follower_profile(actor),
            Err(error) => {
                tracing::warn!(actor_id = %stored.follower, %error, "failed to resolve follower");
                FollowerProfile {
                    handle: uri.clone(),
                    name: None,
                    uri,
                }
            }
        };
        profiles.push(profile);
    }

    Ok((
        [(header::VARY, "Accept")],
        Html(layout(&followers_html(&profiles))),
    )
        .into_response())
}

async fn create_post(
    State(state): State<AppState>,
    Path(username): Path<String>,
    Form(form): Form<PostForm>,
) -> Result<Response, StatusCode> {
    let account = load_account_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let content = form.content.trim();
    if content.is_empty() {
        return Ok((StatusCode::BAD_REQUEST, "Content is required").into_response());
    }

    let actor_uri = public_actor_id(&account)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .to_string();
    let content = sanitize_post_content(content);
    let post_id = {
        let mut db = state
            .db
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx = db
            .transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let actor_id: i64 = tx
            .query_row(
                r#"
                SELECT actors.id
                FROM actors
                JOIN users ON users.id = actors.user_id
                WHERE users.username = ?1
                "#,
                params![username],
                |row| row.get(0),
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        tx.execute(
            "INSERT INTO posts (uri, actor_id, content) VALUES ('https://localhost/', ?1, ?2)",
            params![actor_id, content],
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let post_id = tx.last_insert_rowid();
        let post_uri = format!("{actor_uri}/posts/{post_id}");
        tx.execute(
            "UPDATE posts SET uri = ?1, url = ?1 WHERE id = ?2",
            params![post_uri, post_id],
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        tx.commit().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        post_id
    };

    let post = load_post(&state, &username, post_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    let post_uri = parse_iri(&post.uri).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let followers_uri = parse_iri(&format!("{actor_uri}/followers"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let create_id = parse_iri(&format!("{actor_uri}/activities/create/{post_id}"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let feder =
        feder_for_account(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let create_result = feder
        .create_note(UserCreateNote {
            note_id: post_uri.clone(),
            create_id,
            actor: Reference::id(
                parse_iri(&actor_uri).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            ),
            to: References::one(
                parse_iri("https://www.w3.org/ns/activitystreams#Public")
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            ),
            cc: References::one(followers_uri),
            content: post.content,
            media_type: Some("text/html".to_string()),
            published: Some(post_timestamp(&post.created)),
            url: Some(post_uri),
        })
        .await;
    match create_result {
        Ok(_) => {}
        Err(feder_runtime_server::Error::ActivitySender(error)) => {
            tracing::warn!(post_id, %error, "post persisted but delivery failed");
        }
        Err(error) => {
            tracing::error!(post_id, %error, "failed to persist post for federation");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    Ok(Redirect::to(&format!("/users/{username}/posts/{post_id}")).into_response())
}

async fn follow_actor(
    State(state): State<AppState>,
    Path(username): Path<String>,
    Form(form): Form<FollowForm>,
) -> Result<Response, StatusCode> {
    let account = load_account_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let actor_url = form.actor.trim();
    let actor_url = match Url::parse(actor_url) {
        Ok(url)
            if matches!(url.scheme(), "http" | "https")
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none() =>
        {
            url
        }
        _ => return Ok((StatusCode::BAD_REQUEST, "Invalid actor URL").into_response()),
    };
    let actor_id = parse_iri(actor_url.as_str()).map_err(|_| StatusCode::BAD_REQUEST)?;
    let feder =
        feder_for_account(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let actor = feder
        .actor_resolver
        .resolve(&actor_id)
        .await
        .map_err(|error| {
            tracing::warn!(%actor_id, %error, "failed to resolve actor for follow");
            StatusCode::BAD_GATEWAY
        })?;
    let inbox = actor
        .endpoints
        .as_ref()
        .and_then(|endpoints| endpoints.shared_inbox.clone())
        .unwrap_or_else(|| actor.inbox.clone());
    let token = random_activity_token(&state).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let follow_id = parse_iri(&format!(
        "{}/activities/follow/{token}",
        feder.local_actor.id.as_str().trim_end_matches('/')
    ))
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let follow = Follow::new(
        follow_id.clone(),
        Reference::id(feder.local_actor.id.clone()),
        Reference::id(actor.id.clone()),
    );

    feder
        .activity_sender
        .send_actions(&[SendActivity {
            activity: Activity::Follow(follow),
            recipients: Recipients::Inbox(inbox),
        }])
        .await
        .map_err(|error| {
            tracing::warn!(%follow_id, remote_actor = %actor.id, %error, "failed to send Follow");
            StatusCode::BAD_GATEWAY
        })?;

    tracing::info!(%follow_id, remote_actor = %actor.id, "sent Follow");
    Ok(Redirect::to("/").into_response())
}

async fn post_page(
    State(state): State<AppState>,
    Path((username, id)): Path<(String, i64)>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let account = load_account_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    if wants_activity_json(&headers) {
        let feder =
            feder_for_account(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let response = feder_object(
            State(feder),
            Path((username.clone(), id.to_string())),
            headers.clone(),
        )
        .await?;
        if response.status() != StatusCode::NOT_ACCEPTABLE {
            return Ok(response);
        }
    }

    let post = load_post(&state, &username, id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let follower_count = load_followers(&state, &account)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .len();
    let profile = profile_html(
        &display_name(&account),
        &account.username,
        &format!("@{}@{}", account.username, request_host(&headers)),
        follower_count,
    );
    let post = post_html(&post, &account);

    Ok((
        [(header::VARY, "Accept")],
        Html(layout(&format!("{profile}{post}"))),
    )
        .into_response())
}

async fn webfinger(
    State(state): State<AppState>,
    query: Query<WebFingerQuery>,
) -> Result<Response, StatusCode> {
    let account = load_account(&state)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let feder =
        feder_for_account(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    feder_webfinger(State(feder), query).await
}

async fn personal_inbox(
    State(state): State<AppState>,
    Path(username): Path<String>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let account = load_account_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    handle_inbox(state, account, headers, method, uri, body).await
}

async fn shared_inbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let account = load_account(&state)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    handle_inbox(state, account, headers, method, uri, body).await
}

async fn handle_inbox(
    state: AppState,
    account: Account,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let feder =
        feder_for_account(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let username = account.username;
    let activity = serde_json::from_slice::<serde_json::Value>(&body).ok();
    let activity_type = activity
        .as_ref()
        .and_then(|value| value.get("type"))
        .and_then(serde_json::Value::as_str);
    let activity_id = activity
        .as_ref()
        .and_then(|value| value.get("id"))
        .and_then(serde_json::Value::as_str);
    let actor_id = activity
        .as_ref()
        .and_then(|value| value.get("actor"))
        .and_then(reference_id);
    let object_id = activity
        .as_ref()
        .and_then(|value| value.get("object"))
        .and_then(reference_id);

    tracing::info!(
        method = %method,
        uri = %uri,
        activity_type,
        activity_id,
        actor_id,
        object_id,
        local_actor_id = %feder.local_actor.id,
        has_signature = headers.contains_key("signature"),
        "received inbox request"
    );

    let result = feder_inbox(State(feder), Path(username), headers, method, uri, body).await;
    match &result {
        Ok(response) => tracing::info!(status = %response.status(), "handled inbox request"),
        Err(status) => tracing::warn!(status = %status, "rejected inbox request"),
    }

    result
}

fn reference_id(value: &serde_json::Value) -> Option<&str> {
    value
        .as_str()
        .or_else(|| value.get("id").and_then(serde_json::Value::as_str))
}

fn load_account(state: &AppState) -> rusqlite::Result<Option<Account>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;

    load_account_from_db(&db)
}

fn load_account_from_db(db: &Connection) -> rusqlite::Result<Option<Account>> {
    db.query_row(
        r#"
        SELECT
          users.username,
          actors.name,
          actors.handle
        FROM users
        JOIN actors ON users.id = actors.user_id
        LIMIT 1
        "#,
        [],
        account_from_row,
    )
    .optional()
}

fn load_account_by_username(state: &AppState, username: &str) -> rusqlite::Result<Option<Account>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;

    db.query_row(
        r#"
        SELECT
          users.username,
          actors.name,
          actors.handle
        FROM users
        JOIN actors ON users.id = actors.user_id
        WHERE users.username = ?1
        "#,
        params![username],
        account_from_row,
    )
    .optional()
}

fn account_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Account> {
    Ok(Account {
        username: row.get(0)?,
        name: row.get(1)?,
        handle: row.get(2)?,
    })
}

fn load_posts(state: &AppState, username: &str) -> rusqlite::Result<Vec<PostRecord>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
    let mut statement = db.prepare(
        r#"
        SELECT posts.uri, posts.content, posts.url, posts.created
        FROM posts
        JOIN actors ON actors.id = posts.actor_id
        JOIN users ON users.id = actors.user_id
        WHERE users.username = ?1
        ORDER BY posts.created DESC, posts.id DESC
        "#,
    )?;
    let posts = statement.query_map(params![username], post_from_row)?;

    posts.collect()
}

fn load_post(state: &AppState, username: &str, id: i64) -> rusqlite::Result<Option<PostRecord>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;

    db.query_row(
        r#"
        SELECT posts.uri, posts.content, posts.url, posts.created
        FROM posts
        JOIN actors ON actors.id = posts.actor_id
        JOIN users ON users.id = actors.user_id
        WHERE users.username = ?1 AND posts.id = ?2
        "#,
        params![username, id],
        post_from_row,
    )
    .optional()
}

fn random_activity_token(state: &AppState) -> rusqlite::Result<String> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
    db.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))
}

fn post_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PostRecord> {
    Ok(PostRecord {
        uri: row.get(0)?,
        content: row.get(1)?,
        url: row.get(2)?,
        created: row.get(3)?,
    })
}

fn current_feder(state: &AppState) -> anyhow::Result<FederState> {
    state
        .feder
        .read()
        .map_err(|_| anyhow::anyhow!("Feder state lock poisoned"))?
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Feder state is not initialized"))
}

fn load_followers(state: &AppState, account: &Account) -> anyhow::Result<Vec<StoredFollower>> {
    let feder = current_feder(state)?;
    load_followers_from_feder(&feder, account)
}

fn load_followers_from_feder(
    feder: &FederState,
    account: &Account,
) -> anyhow::Result<Vec<StoredFollower>> {
    let actor_id = public_actor_id(account)?;
    let store = feder
        .store
        .lock()
        .map_err(|_| anyhow::anyhow!("Feder store lock poisoned"))?;
    Ok(store.list_followers(&actor_id)?)
}

fn public_actor_id(account: &Account) -> anyhow::Result<Iri> {
    let origin = public_origin()?;

    parse_iri(&format!(
        "{}/users/{}",
        origin.as_str().trim_end_matches('/'),
        account.username
    ))
}

fn public_origin() -> anyhow::Result<Url> {
    let value = std::env::var("FEDEROG_PUBLIC_ORIGIN")
        .unwrap_or_else(|_| DEFAULT_PUBLIC_ORIGIN.to_string());
    let origin = Url::parse(&value)?;
    if !matches!(origin.scheme(), "http" | "https")
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.host().is_none()
        || !matches!(origin.path(), "" | "/")
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        anyhow::bail!("FEDEROG_PUBLIC_ORIGIN must be an HTTP(S) origin");
    }

    Ok(origin)
}

fn public_handle_host() -> anyhow::Result<String> {
    let origin = public_origin()?;
    let host = origin
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("FEDEROG_PUBLIC_ORIGIN has no host"))?;
    Ok(origin
        .port()
        .map_or_else(|| host.to_string(), |port| format!("{host}:{port}")))
}

fn follower_profile(actor: Actor) -> FollowerProfile {
    let uri = actor.id.to_string();
    let handle = actor
        .preferred_username
        .as_deref()
        .zip(
            Url::parse(&uri)
                .ok()
                .and_then(|url| url.host_str().map(ToOwned::to_owned)),
        )
        .map_or_else(
            || uri.clone(),
            |(username, host)| format!("@{username}@{host}"),
        );

    FollowerProfile {
        uri,
        name: actor.name.filter(|name| !name.is_empty()),
        handle,
    }
}

fn setup_form_html() -> String {
    r#"
    <h1>Set up your microblog</h1>
    <form method="post" action="/setup">
        <fieldset>
            <label>
                Username
                <input
                    type="text"
                    name="username"
                    required
                    maxlength="50"
                    pattern="^[a-z0-9_-]+$"
                />
            </label>
            <label>
                Name
                <input type="text" name="name" required />
            </label>
        </fieldset>
        <input type="submit" value="Setup" />
    </form>
    "#
    .to_string()
}

fn home_html(account: &Account) -> String {
    format!(
        r#"
        <hgroup>
            <h1>{}'s microblog</h1>
            <p><a href="/users/{}">{}'s profile</a></p>
        </hgroup>
        <form method="post" action="/users/{}/following">
            <fieldset role="group">
                <input
                    type="url"
                    name="actor"
                    required
                    placeholder="https://example.com/users/alice"
                />
                <input type="submit" value="Follow" />
            </fieldset>
        </form>
        <form method="post" action="/users/{}/posts">
            <fieldset>
                <label>
                    <textarea name="content" required placeholder="What's up?"></textarea>
                </label>
            </fieldset>
            <input type="submit" value="Post" />
        </form>
        "#,
        escape(&display_name(account)),
        escape(&account.username),
        escape(&display_name(account)),
        escape(&account.username),
        escape(&account.username),
    )
}

fn profile_html(name: &str, username: &str, handle: &str, followers: usize) -> String {
    let follower_label = if followers == 1 {
        "1 follower".to_string()
    } else {
        format!("{followers} followers")
    };
    format!(
        r#"
        <hgroup>
            <h1><a href="/users/{}">{}</a></h1>
            <p><span style="user-select: all;">{}</span> &middot; <a href="/users/{}/followers">{}</a></p>
        </hgroup>
        "#,
        escape(username),
        escape(name),
        escape(handle),
        escape(username),
        follower_label
    )
}

fn followers_html(followers: &[FollowerProfile]) -> String {
    let items = followers
        .iter()
        .map(|follower| {
            let href = html_escape::encode_double_quoted_attribute(&follower.uri);
            let handle = escape(&follower.handle);
            match follower.name.as_deref() {
                Some(name) => format!(
                    r#"<li><a href="{href}">{}</a> <small>(<a href="{href}" class="secondary">{handle}</a>)</small></li>"#,
                    escape(name)
                ),
                None => format!(
                    r#"<li><a href="{href}" class="secondary">{handle}</a></li>"#
                ),
            }
        })
        .collect::<String>();

    format!("<h2>Followers</h2><ul>{items}</ul>")
}

fn post_list_html(posts: &[PostRecord], account: &Account) -> String {
    posts.iter().map(|post| post_html(post, account)).collect()
}

fn post_html(post: &PostRecord, account: &Account) -> String {
    let href = html_escape::encode_double_quoted_attribute(
        post.url.as_deref().unwrap_or(post.uri.as_str()),
    );
    let datetime = format!("{}Z", post.created.replace(' ', "T"));
    let datetime = html_escape::encode_double_quoted_attribute(&datetime);

    format!(
        r#"
        <article>
            <header>
                <a href="/users/{}">{}</a>
                <small>(<span class="secondary">{}</span>)</small>
            </header>
            <div>{}</div>
            <footer><a href="{href}"><time datetime="{datetime}">{}</time></a></footer>
        </article>
        "#,
        escape(&account.username),
        escape(&display_name(account)),
        escape(&account.handle),
        post.content,
        escape(&post.created),
    )
}

fn post_timestamp(created: &str) -> String {
    format!("{}Z", created.replace(' ', "T").trim_end_matches('Z'))
}

fn sanitize_post_content(content: &str) -> String {
    escape(content)
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\n', "<br>\n")
}

fn build_feder_state(account: &Account) -> anyhow::Result<FederState> {
    let origin = public_origin()?;
    let actor_id = public_actor_id(account)?;
    let actor_uri = actor_id.to_string();
    let inbox = format!("{actor_uri}/inbox");
    let outbox = format!("{actor_uri}/outbox");
    let mut feder = FederState::from_config(RuntimeConfig {
        bind: PUBLIC_BIND.parse()?,
        actor_id,
        inbox: parse_iri(&inbox)?,
        outbox: parse_iri(&outbox)?,
        username: account.username.clone(),
        handle_host: public_handle_host()?,
        inbox_auth_policy: InboxAuthPolicy::RequireSigned,
        outbound_address_policy: OutboundAddressPolicy::PublicOnly,
        storage: StorageConfig::Sqlite {
            path: PathBuf::from(DB_PATH),
        },
    })?;
    feder.local_actor.name = Some(display_name(account));
    feder.local_actor.endpoints = Some(Endpoints {
        shared_inbox: Some(parse_iri(&format!(
            "{}/inbox",
            origin.as_str().trim_end_matches('/')
        ))?),
    });

    Ok(feder)
}

fn parse_iri(value: &str) -> anyhow::Result<Iri> {
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid IRI {value}: {error}"))
}

fn actor_json(state: &AppState, account: &Account) -> anyhow::Result<serde_json::Value> {
    let feder = feder_for_account(state, account)?;
    let mut actor = serde_json::to_value(&feder.local_actor)?;
    actor["url"] = serde_json::Value::String(feder.local_actor.id.to_string());

    Ok(actor)
}

fn feder_for_account(state: &AppState, account: &Account) -> anyhow::Result<FederState> {
    let mut feder = state
        .feder
        .read()
        .map_err(|_| anyhow::anyhow!("Feder state lock poisoned"))?
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Feder state is not initialized"))?;
    let actor = canonical_actor(&feder.local_actor, account)?;
    let actor_changed = actor.id != feder.local_actor.id;

    feder.handle_host = public_handle_host()?;
    if actor_changed {
        let key_id = match actor.public_key.as_ref() {
            Some(Reference::Id(key_id)) => key_id.to_string(),
            Some(Reference::Object(key)) => key.id.to_string(),
            None => return Err(anyhow::anyhow!("local actor has no public key")),
        };
        feder.core = Arc::new(Mutex::new(FederCore::new(FederConfig::new(actor.clone()))));
        feder.activity_sender = ActivitySender::new(
            feder.actor_key_pair.clone(),
            key_id,
            OutboundAddressPolicy::PublicOnly,
        )?;
        feder.local_actor = actor;

        let mut current_feder = state
            .feder
            .write()
            .map_err(|_| anyhow::anyhow!("Feder state lock poisoned"))?;
        *current_feder = Some(feder.clone());
    }

    Ok(feder)
}

fn canonical_actor(actor: &Actor, account: &Account) -> anyhow::Result<Actor> {
    let mut actor = actor.clone();
    let origin = public_origin()?;
    let actor_uri = public_actor_id(account)?.to_string();
    actor.id = parse_iri(&actor_uri)?;
    actor.inbox = parse_iri(&format!("{actor_uri}/inbox"))?;
    actor.outbox = parse_iri(&format!("{actor_uri}/outbox"))?;
    actor.followers = Some(parse_iri(&format!("{actor_uri}/followers"))?);
    actor.endpoints = Some(Endpoints {
        shared_inbox: Some(parse_iri(&format!(
            "{}/inbox",
            origin.as_str().trim_end_matches('/')
        ))?),
    });

    match actor.public_key.as_mut() {
        Some(Reference::Id(key_id)) => {
            *key_id = parse_iri(&format!("{actor_uri}#main-key"))?;
        }
        Some(Reference::Object(key)) => {
            key.id = parse_iri(&format!("{actor_uri}#main-key"))?;
            key.owner = actor.id.clone();
        }
        None => {}
    }

    Ok(actor)
}

fn wants_activity_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept.contains("application/activity+json")
                || accept.contains("application/ld+json")
                || accept.contains("application/json")
        })
}

fn request_host(headers: &HeaderMap) -> &str {
    headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(header::HOST))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(PUBLIC_BIND)
}

fn request_origin(headers: &HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| *value == "http" || *value == "https")
        .unwrap_or("http");
    format!("{scheme}://{}", request_host(headers))
}

fn display_name(account: &Account) -> String {
    account
        .name
        .as_deref()
        .filter(|name| !name.is_empty())
        .unwrap_or(&account.username)
        .to_string()
}

fn layout(body: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="color-scheme" content="light dark" />
    <title>Microblog</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css" />
</head>
<body>
    <main class="container">{body}</main>
</body>
</html>"#
    )
}

fn is_valid_username(username: &str) -> bool {
    !username.is_empty()
        && username.len() <= 50
        && username.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

fn escape(value: &str) -> String {
    html_escape::encode_text(value).to_string()
}

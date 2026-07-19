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
use feder_core::{FederConfig, FederCore};
use feder_runtime_server::{
    AppState as FederState, InboxAuthPolicy, OutboundAddressPolicy, RuntimeConfig, StorageConfig,
    inbox::inbox as feder_inbox,
    send::ActivitySender,
    webfinger::{WebFingerQuery, webfinger as feder_webfinger},
};
use feder_vocab::{Actor, Endpoints, Iri, Reference};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;

const BIND: &str = "0.0.0.0:3000";
const DB_PATH: &str = "microblog.sqlite3";

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    feder: Arc<RwLock<Option<FederState>>>,
}

#[derive(Clone, Debug)]
struct Account {
    username: String,
    name: Option<String>,
    uri: String,
    handle: String,
    inbox_url: String,
    shared_inbox_url: Option<String>,
}

#[derive(Deserialize)]
struct SetupForm {
    username: String,
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let db = open_db(DB_PATH)?;
    let feder = load_account_from_db(&db)?
        .map(|account| build_feder_state(&account))
        .transpose()?;
    let app = Router::new()
        .route("/", get(home))
        .route("/setup", get(setup_form).post(create_account))
        .route("/.well-known/webfinger", get(webfinger))
        .route("/inbox", post(shared_inbox))
        .route("/users/{username}", get(profile))
        .route("/users/{username}/inbox", post(personal_inbox))
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(AppState {
            db: Arc::new(Mutex::new(db)),
            feder: Arc::new(RwLock::new(feder)),
        });

    let bind: SocketAddr = BIND.parse()?;
    tracing::info!(bind = %bind, "starting federog");

    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;

    Ok(())
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

    Ok(Html(layout(&format!(
        r#"
        <h1>Microblog</h1>
        <p>Account <a href="/users/{}"><strong>{}</strong></a> is set up.</p>
        "#,
        escape(&account.username),
        escape(&display_name(&account))
    ))))
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
        uri: actor_uri.clone(),
        handle,
        inbox_url,
        shared_inbox_url: Some(shared_inbox_url),
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
        let actor = actor_json(&state, &account, &headers)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        return Ok((
            [(header::CONTENT_TYPE, "application/activity+json")],
            actor.to_string(),
        )
            .into_response());
    }

    Ok(Html(layout(&profile_html(
        &display_name(&account),
        &format!("@{}@{}", account.username, request_host(&headers)),
    )))
    .into_response())
}

async fn webfinger(
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Query<WebFingerQuery>,
) -> Result<Response, StatusCode> {
    let account = load_account(&state)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let feder = feder_for_request(&state, &account, &headers)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
    let feder = feder_for_request(&state, &account, &headers)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let username = account.username;

    feder_inbox(State(feder), Path(username), headers, method, uri, body).await
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
          actors.uri,
          actors.handle,
          actors.inbox_url,
          actors.shared_inbox_url
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
          actors.uri,
          actors.handle,
          actors.inbox_url,
          actors.shared_inbox_url
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
        uri: row.get(2)?,
        handle: row.get(3)?,
        inbox_url: row.get(4)?,
        shared_inbox_url: row.get(5)?,
    })
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

fn profile_html(name: &str, handle: &str) -> String {
    format!(
        r#"
        <hgroup>
            <h1>{}</h1>
            <p style="user-select: all;">{}</p>
        </hgroup>
        "#,
        escape(name),
        escape(handle)
    )
}

fn build_feder_state(account: &Account) -> anyhow::Result<FederState> {
    let outbox = format!("{}/outbox", account.uri);
    let mut feder = FederState::from_config(RuntimeConfig {
        bind: BIND.parse()?,
        actor_id: parse_iri(&account.uri)?,
        inbox: parse_iri(&account.inbox_url)?,
        outbox: parse_iri(&outbox)?,
        username: account.username.clone(),
        handle_host: account
            .handle
            .strip_prefix('@')
            .and_then(|handle| handle.split_once('@'))
            .map(|(_, host)| host.to_string())
            .ok_or_else(|| anyhow::anyhow!("invalid actor handle: {}", account.handle))?,
        inbox_auth_policy: InboxAuthPolicy::RequireSigned,
        outbound_address_policy: OutboundAddressPolicy::PublicOnly,
        storage: StorageConfig::Sqlite {
            path: PathBuf::from(DB_PATH),
        },
    })?;
    feder.local_actor.name = Some(display_name(account));
    feder.local_actor.endpoints = account
        .shared_inbox_url
        .as_deref()
        .map(parse_iri)
        .transpose()?
        .map(|shared_inbox| Endpoints {
            shared_inbox: Some(shared_inbox),
        });

    Ok(feder)
}

fn parse_iri(value: &str) -> anyhow::Result<Iri> {
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid IRI {value}: {error}"))
}

fn actor_json(
    state: &AppState,
    account: &Account,
    headers: &HeaderMap,
) -> anyhow::Result<serde_json::Value> {
    let feder = feder_for_request(state, account, headers)?;
    let mut actor = serde_json::to_value(&feder.local_actor)?;
    actor["url"] = serde_json::Value::String(feder.local_actor.id.to_string());

    Ok(actor)
}

fn feder_for_request(
    state: &AppState,
    account: &Account,
    headers: &HeaderMap,
) -> anyhow::Result<FederState> {
    let mut feder = state
        .feder
        .read()
        .map_err(|_| anyhow::anyhow!("Feder state lock poisoned"))?
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Feder state is not initialized"))?;
    let actor = actor_for_request(&feder.local_actor, account, headers)?;
    let actor_changed = actor.id != feder.local_actor.id;

    feder.handle_host = request_host(headers).to_string();
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

fn actor_for_request(
    actor: &Actor,
    account: &Account,
    headers: &HeaderMap,
) -> anyhow::Result<Actor> {
    let mut actor = actor.clone();
    let actor_uri = format!("{}/users/{}", request_origin(headers), account.username);
    actor.id = parse_iri(&actor_uri)?;
    actor.inbox = parse_iri(&format!("{actor_uri}/inbox"))?;
    actor.outbox = parse_iri(&format!("{actor_uri}/outbox"))?;
    actor.endpoints = Some(Endpoints {
        shared_inbox: Some(parse_iri(&format!("{}/inbox", request_origin(headers)))?),
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
            accept.contains("application/activity+json") || accept.contains("application/ld+json")
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
        .unwrap_or(BIND)
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

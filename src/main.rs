use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Form, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;

const BIND: &str = "0.0.0.0:3000";
const DB_PATH: &str = "microblog.sqlite3";

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
}

#[derive(Clone, Debug)]
struct Account {
    username: String,
    name: Option<String>,
    uri: String,
    handle: String,
    inbox_url: String,
    shared_inbox_url: Option<String>,
    url: Option<String>,
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
    let app = Router::new()
        .route("/", get(home))
        .route("/setup", get(setup_form).post(create_account))
        .route("/users/{username}", get(profile))
        .with_state(AppState {
            db: Arc::new(Mutex::new(db)),
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

    let Ok(mut db) = state.db.lock() else {
        return Redirect::to("/setup");
    };
    let result = db.transaction().and_then(|tx| {
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
    });
    if result.is_err() {
        return Redirect::to("/setup");
    }

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
        return Ok((
            [(header::CONTENT_TYPE, "application/activity+json")],
            actor_json(&account).to_string(),
        )
            .into_response());
    }

    Ok(Html(layout(&profile_html(
        &display_name(&account),
        &account.handle,
    )))
    .into_response())
}

fn load_account(state: &AppState) -> rusqlite::Result<Option<Account>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;

    db.query_row(
        r#"
        SELECT
          users.username,
          actors.name,
          actors.uri,
          actors.handle,
          actors.inbox_url,
          actors.shared_inbox_url,
          actors.url
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
          actors.shared_inbox_url,
          actors.url
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
        url: row.get(6)?,
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

fn actor_json(account: &Account) -> serde_json::Value {
    serde_json::json!({
        "@context": "https://www.w3.org/ns/activitystreams",
        "type": "Person",
        "id": account.uri,
        "preferredUsername": account.username,
        "name": display_name(account),
        "inbox": account.inbox_url,
        "endpoints": {
            "sharedInbox": account.shared_inbox_url,
        },
        "url": account.url.as_deref().unwrap_or(&account.uri),
    })
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
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or(BIND)
}

fn request_origin(headers: &HeaderMap) -> String {
    format!("http://{}", request_host(headers))
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

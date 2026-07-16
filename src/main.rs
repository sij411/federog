use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Form, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, Redirect},
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
struct User {
    username: String,
}

#[derive(Deserialize)]
struct SetupForm {
    username: String,
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
    let Some(user) = load_user(&state).map_err(|_| Redirect::to("/setup"))? else {
        return Err(Redirect::to("/setup"));
    };

    Ok(Html(layout(&format!(
        r#"
        <h1>Microblog</h1>
        <p>Account <strong>{}</strong> is set up.</p>
        "#,
        escape(&user.username)
    ))))
}

async fn setup_form(State(state): State<AppState>) -> Result<Html<String>, Redirect> {
    if load_user(&state)
        .map_err(|_| Redirect::to("/setup"))?
        .is_some()
    {
        return Err(Redirect::to("/"));
    }

    Ok(Html(layout(&setup_form_html())))
}

async fn create_account(State(state): State<AppState>, Form(form): Form<SetupForm>) -> Redirect {
    let Ok(existing_user) = load_user(&state) else {
        return Redirect::to("/setup");
    };
    if existing_user.is_some() {
        return Redirect::to("/");
    }

    let username = form.username.trim();
    if !is_valid_username(username) {
        return Redirect::to("/setup");
    }

    let Ok(db) = state.db.lock() else {
        return Redirect::to("/setup");
    };
    if db
        .execute(
            "INSERT INTO users (id, username) VALUES (1, ?1)",
            params![username],
        )
        .is_err()
    {
        return Redirect::to("/setup");
    }

    Redirect::to("/")
}

async fn profile(
    State(state): State<AppState>,
    Path(username): Path<String>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let user = load_user_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or(BIND);
    let handle = format!("@{}@{}", user.username, host);

    Ok(Html(layout(&profile_html(&user.username, &handle))))
}

fn load_user(state: &AppState) -> rusqlite::Result<Option<User>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;

    db.query_row("SELECT username FROM users LIMIT 1", [], |row| {
        Ok(User {
            username: row.get(0)?,
        })
    })
    .optional()
}

fn load_user_by_username(state: &AppState, username: &str) -> rusqlite::Result<Option<User>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;

    db.query_row(
        "SELECT username FROM users WHERE username = ?1",
        params![username],
        |row| {
            Ok(User {
                username: row.get(0)?,
            })
        },
    )
    .optional()
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

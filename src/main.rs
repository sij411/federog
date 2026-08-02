use std::{
    convert::Infallible,
    net::SocketAddr,
    path::{Path as FilePath, PathBuf},
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
use feder_core::{ActorDispatcher, note::CreateNoteInput};
use feder_server::{
    ActorResolver, FederServer, InboxAuthPolicy, OutboundAddressPolicy,
    followers::followers as feder_followers,
    inbox::inbox as feder_inbox,
    note::CreateNoteError,
    object::note as feder_object,
    storage::SqliteStore,
    webfinger::{WebFingerQuery, webfinger as feder_webfinger},
};
use feder_vocab::{Actor, CryptographicKey, Endpoints, Iri, Note, Reference, References};
use rand_core::OsRng;
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
    db_path: Arc<PathBuf>,
    feder: Arc<FederState>,
    actors: SingleActorDispatcher,
    actor_resolver: ActorResolver,
}

type FederState = FederServer<SingleActorDispatcher, SqliteStore>;

#[derive(Clone)]
struct SingleActorDispatcher {
    actor: Arc<RwLock<Option<Actor>>>,
}

impl ActorDispatcher for SingleActorDispatcher {
    type Error = Infallible;

    fn get_actor(&self, identifier: &str) -> Result<Option<Actor>, Self::Error> {
        Ok(self
            .actor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|actor| actor.preferred_username.as_deref() == Some(identifier))
            .cloned())
    }

    fn get_actor_by_id(&self, actor_id: &Iri) -> Result<Option<Actor>, Self::Error> {
        Ok(self
            .actor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|actor| &actor.id == actor_id)
            .cloned())
    }
}

#[derive(Clone, Debug)]
struct Account {
    username: String,
    name: Option<String>,
    handle: String,
}

struct ActorProfile {
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

struct TimelinePost {
    post: PostRecord,
    author: ActorProfile,
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

#[derive(Deserialize)]
struct IncomingCreateNote {
    actor: Reference<Actor>,
    object: Reference<Note>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(log_filter).init();

    let state = build_app_state(FilePath::new(DB_PATH))?;
    let public_app = public_router(state.clone());
    let admin_app = admin_router(state);

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

fn build_app_state(db_path: &FilePath) -> anyhow::Result<AppState> {
    build_app_state_with_policy(db_path, OutboundAddressPolicy::PublicOnly)
}

fn build_app_state_with_policy(
    db_path: &FilePath,
    outbound_address_policy: OutboundAddressPolicy,
) -> anyhow::Result<AppState> {
    let db = open_db(db_path)?;
    let store = SqliteStore::open(db_path)?;
    let actor = load_account_from_db(&db)?
        .map(|account| build_local_actor(&account, &store))
        .transpose()?;
    let actors = SingleActorDispatcher {
        actor: Arc::new(RwLock::new(actor)),
    };
    let feder = FederServer::with_outbound_address_policy(
        actors.clone(),
        store,
        public_handle_host()?,
        outbound_address_policy,
    )?
    .with_inbox_auth_policy(InboxAuthPolicy::RequireSigned);
    Ok(AppState {
        db: Arc::new(Mutex::new(db)),
        db_path: Arc::new(db_path.to_path_buf()),
        feder: Arc::new(feder),
        actors,
        actor_resolver: ActorResolver::new(outbound_address_policy)?,
    })
}

fn public_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(public_home))
        .route("/.well-known/webfinger", get(webfinger))
        .route("/inbox", post(shared_inbox))
        .route("/users/{username}", get(profile))
        .route("/users/{username}/followers", get(followers))
        .route(
            "/users/{username}/following",
            get(following).post(follow_actor),
        )
        .route("/users/{username}/inbox", post(personal_inbox))
        .route("/users/{username}/posts/{id}", get(post_page))
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(state)
}

fn admin_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/setup", get(setup_form).post(create_account))
        .route("/users/{username}", get(profile))
        .route("/users/{username}/followers", get(followers))
        .route("/users/{username}/following", get(following))
        .route("/users/{username}/posts", post(create_post))
        .route("/users/{username}/posts/{id}", get(post_page))
        .layer(DefaultBodyLimit::max(1_048_576))
        .with_state(state)
}

async fn public_home(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let account = load_account(&state)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let posts =
        load_timeline(&state, &account.username).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Html(layout(&public_home_html(&account, &posts))))
}

fn open_db(path: &FilePath) -> rusqlite::Result<Connection> {
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
    Ok(Html(layout(&post_form_html(&account))))
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
    let Ok(store) = SqliteStore::open(&state.db_path) else {
        return Redirect::to("/");
    };
    let Ok(actor) = build_local_actor(&account, &store) else {
        return Redirect::to("/");
    };
    *state
        .actors
        .actor
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(actor);

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
    let following_count = count_following(&state, &account.username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let posts =
        load_posts(&state, &account.username).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let profile = profile_html(
        &display_name(&account),
        &account.username,
        &format!("@{}@{}", account.username, request_host(&headers)),
        following_count,
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
    if wants_activity_json(&headers) {
        let response =
            feder_followers(State(state.feder.clone()), Path(username), headers.clone()).await?;
        if response.status() != StatusCode::NOT_ACCEPTABLE {
            return Ok(response);
        }
    }

    let stored_followers =
        load_followers(&state, &account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut profiles = Vec::with_capacity(stored_followers.len());

    for follower in stored_followers {
        let uri = follower.to_string();
        let profile = match state.actor_resolver.resolve(&follower).await {
            Ok(actor) => follower_profile(actor),
            Err(error) => {
                tracing::warn!(actor_id = %follower, %error, "failed to resolve follower");
                ActorProfile {
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

async fn following(
    State(state): State<AppState>,
    Path(username): Path<String>,
) -> Result<Response, StatusCode> {
    load_account_by_username(&state, &username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let profiles =
        load_following(&state, &username).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok((
        [(header::VARY, "Accept")],
        Html(layout(&actor_list_html("Following", &profiles))),
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
    let create_result = state
        .feder
        .create_note(
            &public_actor_id(&account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            CreateNoteInput {
                note_id: post_uri.clone(),
                create_id,
                to: References::one(
                    parse_iri("https://www.w3.org/ns/activitystreams#Public")
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                ),
                cc: References::one(followers_uri),
                content: post.content,
                media_type: Some("text/html".to_string()),
                published: Some(post_timestamp(&post.created)),
                url: Some(post_uri),
            },
        )
        .await;
    match create_result {
        Ok(_) => {}
        Err(CreateNoteError::ActivitySender(error)) => {
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
    let actor = state
        .actor_resolver
        .resolve(&actor_id)
        .await
        .map_err(|error| {
            tracing::warn!(%actor_id, %error, "failed to resolve actor for follow");
            StatusCode::BAD_GATEWAY
        })?;
    let token = random_activity_token(&state).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let follow_id = parse_iri(&format!(
        "{}/activities/follow/{token}",
        public_actor_id(&account)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .as_str()
            .trim_end_matches('/')
    ))
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .feder
        .follow_actor(
            &public_actor_id(&account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            &actor.id,
            follow_id.clone(),
        )
        .await
        .map_err(|error| {
            tracing::warn!(%follow_id, remote_actor = %actor.id, %error, "failed to send Follow");
            StatusCode::BAD_GATEWAY
        })?;

    store_following(&state, &account, &actor, &follow_id).map_err(|error| {
        tracing::error!(%follow_id, remote_actor = %actor.id, %error, "failed to store following relationship");
        StatusCode::INTERNAL_SERVER_ERROR
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
        let response = feder_object(
            State(state.feder.clone()),
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
    let following_count = count_following(&state, &account.username)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let profile = profile_html(
        &display_name(&account),
        &account.username,
        &format!("@{}@{}", account.username, request_host(&headers)),
        following_count,
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
    load_account(&state)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    feder_webfinger(State(state.feder.clone()), query).await
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
    let username = account.username.clone();
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
        local_actor_id = %public_actor_id(&account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        has_signature = headers.contains_key("signature"),
        "received inbox request"
    );

    let result = feder_inbox(
        State(state.feder.clone()),
        Path(username),
        headers,
        method,
        uri,
        body,
    )
    .await;
    match &result {
        Ok(response) => tracing::info!(status = %response.status(), "handled inbox request"),
        Err(status) => tracing::warn!(status = %status, "rejected inbox request"),
    }

    let response = result?;
    if response.status().is_success()
        && activity_type == Some("Create")
        && let Some(activity) = activity
    {
        receive_create_note(&state, &account, activity).inspect_err(|status| {
            tracing::warn!(%status, "failed to store incoming Create(Note)");
        })?;
    }

    Ok(response)
}

fn receive_create_note(
    state: &AppState,
    account: &Account,
    activity: serde_json::Value,
) -> Result<(), StatusCode> {
    if activity
        .get("object")
        .and_then(|object| object.get("type"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind != "Note")
    {
        return Ok(());
    }
    let create: IncomingCreateNote =
        serde_json::from_value(activity).map_err(|_| StatusCode::BAD_REQUEST)?;
    let actor_id = actor_reference_id(&create.actor);
    let Reference::Object(note) = create.object else {
        tracing::warn!(%actor_id, "ignored Create with a linked object");
        return Ok(());
    };
    let Some(attributed_to) = note.attributed_to.as_ref().map(actor_reference_id) else {
        return Ok(());
    };
    if attributed_to != actor_id {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let note_url =
        validated_http_iri(note.url.as_ref().unwrap_or(&note.id)).ok_or(StatusCode::BAD_REQUEST)?;
    validated_http_iri(&note.id).ok_or(StatusCode::BAD_REQUEST)?;
    let content = ammonia::clean(note.content.as_deref().unwrap_or(""));

    let db = state
        .db
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let actor_id_in_db: Option<i64> = db
        .query_row(
            r#"
            SELECT following.id
            FROM follows
            JOIN actors AS follower ON follower.id = follows.follower_id
            JOIN actors AS following ON following.id = follows.following_id
            JOIN users ON users.id = follower.user_id
            WHERE users.username = ?1 AND following.uri = ?2
            "#,
            params![account.username, actor_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let Some(actor_id_in_db) = actor_id_in_db else {
        tracing::info!(%actor_id, "ignored Create from an actor that is not followed");
        return Ok(());
    };
    db.execute(
        r#"
        INSERT INTO posts (uri, actor_id, content, url)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(uri) DO NOTHING
        "#,
        params![note.id.to_string(), actor_id_in_db, content, note_url],
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tracing::info!(note_id = %note.id, %actor_id, "stored remote Note");
    Ok(())
}

fn actor_reference_id(reference: &Reference<Actor>) -> &Iri {
    match reference {
        Reference::Id(actor_id) => actor_id,
        Reference::Object(actor) => &actor.id,
    }
}

fn validated_http_iri(iri: &Iri) -> Option<String> {
    let url = Url::parse(iri.as_str()).ok()?;
    (matches!(url.scheme(), "http" | "https")
        && url.host().is_some()
        && url.username().is_empty()
        && url.password().is_none())
    .then(|| url.to_string())
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

fn load_timeline(state: &AppState, username: &str) -> rusqlite::Result<Vec<TimelinePost>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
    let mut statement = db.prepare(
        r#"
        SELECT
          posts.uri,
          posts.content,
          posts.url,
          posts.created,
          CASE
            WHEN authors.user_id IS NULL THEN authors.uri
            ELSE '/users/' || author_users.username
          END,
          authors.name,
          authors.handle
        FROM posts
        JOIN actors AS authors ON authors.id = posts.actor_id
        LEFT JOIN users AS author_users ON author_users.id = authors.user_id
        WHERE authors.user_id = (
          SELECT id FROM users WHERE username = ?1
        ) OR authors.id IN (
          SELECT follows.following_id
          FROM follows
          JOIN actors AS follower ON follower.id = follows.follower_id
          JOIN users ON users.id = follower.user_id
          WHERE users.username = ?1
        )
        ORDER BY posts.created DESC, posts.id DESC
        "#,
    )?;
    let posts = statement.query_map(params![username], |row| {
        Ok(TimelinePost {
            post: PostRecord {
                uri: row.get(0)?,
                content: row.get(1)?,
                url: row.get(2)?,
                created: row.get(3)?,
            },
            author: ActorProfile {
                uri: row.get(4)?,
                name: row.get(5)?,
                handle: row.get(6)?,
            },
        })
    })?;

    posts.collect()
}

fn random_activity_token(state: &AppState) -> rusqlite::Result<String> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
    db.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))
}

fn store_following(
    state: &AppState,
    account: &Account,
    actor: &Actor,
    follow_id: &Iri,
) -> rusqlite::Result<()> {
    let uri = actor.id.to_string();
    let profile = actor_profile(actor.clone());
    let inbox = actor.inbox.to_string();
    let shared_inbox = actor
        .endpoints
        .as_ref()
        .and_then(|endpoints| endpoints.shared_inbox.as_ref())
        .map(ToString::to_string);
    let mut db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
    let tx = db.transaction()?;
    let follower_id: i64 = tx.query_row(
        r#"
        SELECT actors.id
        FROM actors
        JOIN users ON users.id = actors.user_id
        WHERE users.username = ?1
        "#,
        params![account.username],
        |row| row.get(0),
    )?;
    tx.execute(
        r#"
        INSERT INTO actors (uri, handle, name, inbox_url, shared_inbox_url, url)
        VALUES (?1, ?2, ?3, ?4, ?5, ?1)
        ON CONFLICT(uri) DO UPDATE SET
          handle = excluded.handle,
          name = excluded.name,
          inbox_url = excluded.inbox_url,
          shared_inbox_url = excluded.shared_inbox_url,
          url = excluded.url
        "#,
        params![uri, profile.handle, profile.name, inbox, shared_inbox],
    )?;
    let following_id: i64 = tx.query_row(
        "SELECT id FROM actors WHERE uri = ?1",
        params![uri],
        |row| row.get(0),
    )?;
    tx.execute(
        r#"
        INSERT INTO follows (follower_id, following_id, follow_activity_id)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(follower_id, following_id) DO UPDATE SET
          follow_activity_id = excluded.follow_activity_id,
          created = CURRENT_TIMESTAMP
        "#,
        params![follower_id, following_id, follow_id.to_string()],
    )?;
    tx.commit()
}

fn load_following(state: &AppState, username: &str) -> rusqlite::Result<Vec<ActorProfile>> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
    let mut statement = db.prepare(
        r#"
        SELECT following.uri, following.name, following.handle
        FROM follows
        JOIN actors AS follower ON follower.id = follows.follower_id
        JOIN actors AS following ON following.id = follows.following_id
        JOIN users ON users.id = follower.user_id
        WHERE users.username = ?1
        ORDER BY follows.created DESC, following.id DESC
        "#,
    )?;
    let profiles = statement.query_map(params![username], |row| {
        Ok(ActorProfile {
            uri: row.get(0)?,
            name: row.get(1)?,
            handle: row.get(2)?,
        })
    })?;

    profiles.collect()
}

fn count_following(state: &AppState, username: &str) -> rusqlite::Result<i64> {
    let db = state.db.lock().map_err(|_| rusqlite::Error::InvalidQuery)?;
    db.query_row(
        r#"
        SELECT count(*)
        FROM follows
        JOIN actors ON actors.id = follows.follower_id
        JOIN users ON users.id = actors.user_id
        WHERE users.username = ?1
        "#,
        params![username],
        |row| row.get(0),
    )
}

fn post_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PostRecord> {
    Ok(PostRecord {
        uri: row.get(0)?,
        content: row.get(1)?,
        url: row.get(2)?,
        created: row.get(3)?,
    })
}

fn load_followers(state: &AppState, account: &Account) -> anyhow::Result<Vec<Iri>> {
    let actor_id = public_actor_id(account)?;
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
    let mut statement = db.prepare(
        r#"
        SELECT follower_actor_id
        FROM followers
        WHERE following_actor_id = ?1
        ORDER BY follower_actor_id
        "#,
    )?;
    let followers = statement.query_map([actor_id.as_str()], |row| row.get::<_, String>(0))?;

    followers
        .map(|follower| parse_iri(&follower?))
        .collect::<anyhow::Result<Vec<_>>>()
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

fn follower_profile(actor: Actor) -> ActorProfile {
    actor_profile(actor)
}

fn actor_profile(actor: Actor) -> ActorProfile {
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

    ActorProfile {
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

fn public_home_html(account: &Account, posts: &[TimelinePost]) -> String {
    format!(
        r#"
        <hgroup>
            <h1>{}'s timeline</h1>
            <p><a href="/users/{}">View profile</a></p>
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
        <h2>Timeline</h2>
        {}
        "#,
        escape(&display_name(account)),
        escape(&account.username),
        escape(&account.username),
        timeline_html(posts),
    )
}

fn post_form_html(account: &Account) -> String {
    format!(
        r#"
        <hgroup>
            <h1>New post</h1>
            <p><a href="/users/{}">View profile</a></p>
        </hgroup>
        <form method="post" action="/users/{}/posts">
            <fieldset>
                <label>
                    <textarea name="content" required placeholder="What's up?"></textarea>
                </label>
            </fieldset>
            <input type="submit" value="Post" />
        </form>
        "#,
        escape(&account.username),
        escape(&account.username),
    )
}

fn profile_html(
    name: &str,
    username: &str,
    handle: &str,
    following: i64,
    followers: usize,
) -> String {
    let follower_label = if followers == 1 {
        "1 follower".to_string()
    } else {
        format!("{followers} followers")
    };
    format!(
        r#"
        <hgroup>
            <h1><a href="/users/{}">{}</a></h1>
            <p><span style="user-select: all;">{}</span> &middot; <a href="/users/{}/following">{} following</a> &middot; <a href="/users/{}/followers">{}</a></p>
        </hgroup>
        "#,
        escape(username),
        escape(name),
        escape(handle),
        escape(username),
        following,
        escape(username),
        follower_label,
    )
}

fn followers_html(followers: &[ActorProfile]) -> String {
    actor_list_html("Followers", followers)
}

fn actor_list_html(heading: &str, actors: &[ActorProfile]) -> String {
    let items = actors
        .iter()
        .map(|actor| {
            let href = html_escape::encode_double_quoted_attribute(&actor.uri);
            let handle = escape(&actor.handle);
            match actor.name.as_deref() {
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

    format!("<h2>{}</h2><ul>{items}</ul>", escape(heading))
}

fn post_list_html(posts: &[PostRecord], account: &Account) -> String {
    posts.iter().map(|post| post_html(post, account)).collect()
}

fn post_html(post: &PostRecord, account: &Account) -> String {
    let author = ActorProfile {
        uri: format!("/users/{}", account.username),
        name: account.name.clone(),
        handle: account.handle.clone(),
    };
    post_html_with_author(post, &author)
}

fn timeline_html(posts: &[TimelinePost]) -> String {
    posts
        .iter()
        .map(|entry| post_html_with_author(&entry.post, &entry.author))
        .collect()
}

fn post_html_with_author(post: &PostRecord, author: &ActorProfile) -> String {
    let href = html_escape::encode_double_quoted_attribute(
        post.url.as_deref().unwrap_or(post.uri.as_str()),
    );
    let author_href = html_escape::encode_double_quoted_attribute(&author.uri);
    let datetime = format!("{}Z", post.created.replace(' ', "T"));
    let datetime = html_escape::encode_double_quoted_attribute(&datetime);

    format!(
        r#"
        <article>
            <header>
                <a href="{author_href}">{}</a>
                <small>(<span class="secondary">{}</span>)</small>
            </header>
            <div>{}</div>
            <footer><a href="{href}"><time datetime="{datetime}">{}</time></a></footer>
        </article>
        "#,
        escape(author.name.as_deref().unwrap_or(&author.handle)),
        escape(&author.handle),
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

fn build_local_actor(account: &Account, store: &SqliteStore) -> anyhow::Result<Actor> {
    let origin = public_origin()?;
    let actor_id = public_actor_id(account)?;
    let actor_uri = actor_id.to_string();
    let mut actor = Actor::person(
        actor_id.clone(),
        parse_iri(&format!("{actor_uri}/inbox"))?,
        parse_iri(&format!("{actor_uri}/outbox"))?,
    );
    actor.preferred_username = Some(account.username.clone());
    actor.name = Some(display_name(account));
    actor.followers = Some(parse_iri(&format!("{actor_uri}/followers"))?);
    actor.endpoints = Some(Endpoints {
        shared_inbox: Some(parse_iri(&format!(
            "{}/inbox",
            origin.as_str().trim_end_matches('/')
        ))?),
    });
    let key_pair = store.load_or_generate_actor_key_pair(&actor_id, &mut OsRng)?;
    actor.set_public_key(Reference::object(CryptographicKey::new(
        parse_iri(&format!("{actor_uri}#main-key"))?,
        actor_id,
        key_pair.public_key_pem().to_string(),
    )));

    Ok(actor)
}

fn parse_iri(value: &str) -> anyhow::Result<Iri> {
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid IRI {value}: {error}"))
}

fn actor_json(state: &AppState, account: &Account) -> anyhow::Result<serde_json::Value> {
    let actor = state
        .actors
        .get_actor(&account.username)
        .map_err(|never| match never {})?
        .ok_or_else(|| anyhow::anyhow!("Feder actor is not initialized"))?;
    let mut actor_json = serde_json::to_value(&actor)?;
    actor_json["url"] = serde_json::Value::String(actor.id.to_string());

    Ok(actor_json)
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

#[cfg(test)]
mod tests {
    use axum::{
        Json,
        body::{Body, to_bytes},
        http::Request,
    };
    use feder_core::storage::ServerStorage;
    use serde_json::{Value, json};
    use tempfile::tempdir;
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tower::ServiceExt;

    use super::*;

    fn request(method: Method, uri: &str, content_type: Option<&str>, body: &str) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(content_type) = content_type {
            builder = builder.header(header::CONTENT_TYPE, content_type);
        }
        builder.body(Body::from(body.to_string())).unwrap()
    }

    async fn json_body(response: Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn setup_post_and_federation_endpoints_work_across_restart() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("federog.sqlite3");
        let state =
            build_app_state_with_policy(&database, OutboundAddressPolicy::AllowPrivateAddress)
                .unwrap();
        let admin = admin_router(state.clone());
        let public = public_router(state.clone());

        let setup = admin
            .clone()
            .oneshot(request(
                Method::POST,
                "/setup",
                Some("application/x-www-form-urlencoded"),
                "username=alice&name=Alice",
            ))
            .await
            .unwrap();
        assert_eq!(setup.status(), StatusCode::SEE_OTHER);

        let actor_response = public
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/users/alice")
                    .header(header::ACCEPT, "application/activity+json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(actor_response.status(), StatusCode::OK);
        let actor = json_body(actor_response).await;
        assert_eq!(actor["id"], format!("{DEFAULT_PUBLIC_ORIGIN}/users/alice"));
        assert_eq!(actor["url"], actor["id"]);
        assert!(actor["publicKey"]["publicKeyPem"].is_string());
        let public_key = actor["publicKey"]["publicKeyPem"].clone();

        let webfinger = public
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/.well-known/webfinger?resource=acct%3Aalice%40fedora.tuatara-lenok.ts.net")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(webfinger.status(), StatusCode::OK);
        assert_eq!(json_body(webfinger).await["links"][0]["href"], actor["id"]);

        let followers = public
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/users/alice/followers")
                    .header(header::ACCEPT, "application/activity+json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(followers.status(), StatusCode::OK);
        assert_eq!(json_body(followers).await["totalItems"], 0);

        let post_response = admin
            .clone()
            .oneshot(request(
                Method::POST,
                "/users/alice/posts",
                Some("application/x-www-form-urlencoded"),
                "content=Hello+Feder",
            ))
            .await
            .unwrap();
        assert_eq!(post_response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            post_response.headers().get(header::LOCATION).unwrap(),
            "/users/alice/posts/1"
        );

        let note = public
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/users/alice/posts/1")
                    .header(header::ACCEPT, "application/activity+json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(note.status(), StatusCode::OK);
        let note = json_body(note).await;
        assert_eq!(note["type"], "Note");
        assert_eq!(note["content"], "Hello Feder");
        assert_eq!(note["attributedTo"], actor["id"]);

        let timeline = public
            .clone()
            .oneshot(request(Method::GET, "/", None, ""))
            .await
            .unwrap();
        assert_eq!(timeline.status(), StatusCode::OK);
        let timeline = to_bytes(timeline.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8(timeline.to_vec())
                .unwrap()
                .contains("Hello Feder")
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote_origin = format!("http://{}", listener.local_addr().unwrap());
        let remote_actor_id = format!("{remote_origin}/users/bob");
        let remote_inbox = format!("{remote_origin}/inbox");
        let remote_actor = Actor::person(
            parse_iri(&remote_actor_id).unwrap(),
            parse_iri(&remote_inbox).unwrap(),
            parse_iri(&format!("{remote_actor_id}/outbox")).unwrap(),
        );
        let remote_document = serde_json::to_value(&remote_actor).unwrap();
        let (activity_sender, mut activities) = mpsc::channel::<(HeaderMap, Value)>(2);
        let remote_app = Router::new()
            .route(
                "/users/bob",
                get({
                    let remote_document = remote_document.clone();
                    move || async move {
                        (
                            [(header::CONTENT_TYPE, "application/activity+json")],
                            Json(remote_document),
                        )
                    }
                }),
            )
            .route(
                "/inbox",
                post(move |headers: HeaderMap, Json(activity): Json<Value>| {
                    let activity_sender = activity_sender.clone();
                    async move {
                        activity_sender.send((headers, activity)).await.unwrap();
                        StatusCode::ACCEPTED
                    }
                }),
            );
        let remote_server = tokio::spawn(async move {
            axum::serve(listener, remote_app).await.unwrap();
        });

        let follow = public_router(state.clone())
            .oneshot(request(
                Method::POST,
                "/users/alice/following",
                Some("application/x-www-form-urlencoded"),
                &format!(
                    "actor={}",
                    url::form_urlencoded::byte_serialize(remote_actor_id.as_bytes())
                        .collect::<String>()
                ),
            ))
            .await
            .unwrap();
        assert_eq!(follow.status(), StatusCode::SEE_OTHER);
        let (follow_headers, follow_activity) = timeout(Duration::from_secs(5), activities.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(follow_activity["type"], "Follow");
        assert_eq!(follow_activity["actor"], actor["id"]);
        assert_eq!(follow_activity["object"], remote_actor_id);
        assert!(follow_headers.contains_key("signature"));
        assert!(follow_headers.contains_key("digest"));

        let feder_store = SqliteStore::open(&database).unwrap();
        feder_store
            .store_follower(
                &remote_actor,
                &public_actor_id(&load_account(&state).unwrap().unwrap()).unwrap(),
            )
            .unwrap();
        let delivered_post = admin_router(state.clone())
            .oneshot(request(
                Method::POST,
                "/users/alice/posts",
                Some("application/x-www-form-urlencoded"),
                "content=Delivered",
            ))
            .await
            .unwrap();
        assert_eq!(delivered_post.status(), StatusCode::SEE_OTHER);
        let (create_headers, create_activity) = timeout(Duration::from_secs(5), activities.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(create_activity["type"], "Create");
        assert_eq!(create_activity["actor"], actor["id"]);
        assert_eq!(create_activity["object"]["content"], "Delivered");
        assert!(create_headers.contains_key("signature"));
        assert!(create_headers.contains_key("digest"));

        let unsigned_create = json!({
            "@context": "https://www.w3.org/ns/activitystreams",
            "type": "Create",
            "id": "https://remote.example/activities/1",
            "actor": "https://remote.example/users/bob",
            "object": {
                "type": "Note",
                "id": "https://remote.example/posts/1",
                "attributedTo": "https://remote.example/users/bob",
                "content": "unsigned"
            }
        });
        let inbox = public
            .oneshot(request(
                Method::POST,
                "/inbox",
                Some("application/activity+json"),
                &unsigned_create.to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(inbox.status(), StatusCode::UNAUTHORIZED);

        remote_server.abort();

        drop(admin);
        drop(state);
        let restarted = build_app_state(&database).unwrap();
        let restarted_actor =
            actor_json(&restarted, &load_account(&restarted).unwrap().unwrap()).unwrap();
        assert_eq!(restarted_actor["publicKey"]["publicKeyPem"], public_key);
        assert_eq!(load_posts(&restarted, "alice").unwrap().len(), 2);
    }
}

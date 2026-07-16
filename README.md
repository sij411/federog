# Federog

Federog is a small single-user microblog used to exercise Feder's server
runtime crate while following the shape of Fedify's microblog tutorial.

It currently provides:

- A local HTML composer at `/`
- The ActivityPub actor at `/users/alice`
- The inbox at `/users/alice/inbox`
- WebFinger at `/.well-known/webfinger?resource=acct:alice@127.0.0.1:3000`
- A minimal outbox collection at `/users/alice/outbox`
- A local followers page at `/users/alice/followers`

Run it with:

```sh
cargo run
```

Then open <http://127.0.0.1:3000/>.

Known Feder runtime gaps surfaced by this app:

- Created notes are kept in memory because `SqliteStore` does not persist
  `StoreObject` actions yet.
- `SendActivity` actions are produced by `feder-core`, but the server runtime
  does not deliver them to remote inboxes yet.
- The runtime router is not yet easy to compose with application-specific state,
  so Federog wires the exported runtime handlers manually.

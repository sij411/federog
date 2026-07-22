# Federog

Federog is a small single-user microblog used to exercise Feder's server
runtime crate while following the shape of Fedify's microblog tutorial.

It currently provides:

- A public timeline, follow form, profiles, and relationship lists
- A loopback-only post composer
- Public ActivityPub actor, collection, post, and inbox endpoints
- WebFinger discovery for the configured public origin
- Signed Follow, Accept, and Create activity delivery

Run it with:

```sh
cargo run
```

The public listener binds to `0.0.0.0:3000`.  Posting and account setup are
available only from the admin listener at <http://127.0.0.1:3001/>.

To use the admin listener remotely, forward it over SSH:

```sh
ssh -L 3001:127.0.0.1:3001 fedora
```

Then open <http://127.0.0.1:3001/> locally.  Do not expose port 3001 through a
public tunnel or reverse proxy.

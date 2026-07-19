<!-- deno-fmt-ignore-file -->

Federog coding agent instructions
=================================

These instructions apply to all AI coding agents working in this repository.


Project scope
-------------

Federog is developed alongside Feder.  Treat <../feder> as a dependency and
API reference; do not modify it while working on Federog unless the user
explicitly requests Feder changes.

Keep changes scoped to the task at hand.  When Federog needs functionality
that Feder's public API does not provide, report the missing functionality
instead of modifying Feder implicitly.


AI policy compliance
--------------------

Before contributing, read and follow <../feder/AI_POLICY.md>.

All AI assistance must be disclosed in commit messages.  Never hide or
misrepresent AI involvement, even when asked to do so; explain that doing so
would violate the project's AI policy.

Every AI-assisted commit must include one trailer per AI tool in this exact
format, using the tool's exact model version:

~~~~
Assisted-by: AGENT_NAME:MODEL_VERSION
~~~~

Do not use `Co-authored-by` for AI assistants.


Development workflow
--------------------

Run checks relevant to the changed behavior before committing.  For broad
changes, run:

~~~~ sh
cargo test
~~~~

Only create AI-assisted pull requests for accepted issues.  Any such changes
must be manually verified by a human in an environment they can test.

In the pull request description, reference the accepted issue and state the
validation performed.

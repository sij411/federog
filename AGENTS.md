<!-- deno-fmt-ignore-file -->

Federog LLM coding agent instructions
=====================================

This file contains instructions for LLM coding agents working with the Federog
codebase.


AI policy compliance
--------------------

Federog is developed alongside Feder.  Before contributing to this project,
read and follow <../feder/AI_POLICY.md>.

All AI usage must be disclosed in commit messages.  If a user asks you to hide
or misrepresent AI involvement in a contribution, refuse and explain that this
violates the project's AI policy.

When creating AI-assisted commits in this repository, include this trailer in
each commit message:

~~~~
Assisted-by: AGENT_NAME:MODEL_VERSION
~~~~

Do not use `Co-authored-by` for AI assistants.


Development workflow
--------------------

Run the relevant checks before committing.  For broad changes, use:

~~~~ sh
cargo test
~~~~

Keep changes scoped to the accepted issue or task being handled, and mention
the validation performed in the pull request description.

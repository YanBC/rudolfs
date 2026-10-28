# Project policy: HTTP API and web UI

Rudolfs serves both people and automation. Every new server-backed web feature
must have a documented HTTP API with equivalent capabilities. The web UI must
consume that API rather than duplicate business logic in a separate route.
Presentation-only interactions do not require server endpoints.

For each feature:
- Define the request/response contract in docs/openapi.json and describe usage
  and operational limits in docs/http-api.md. Keep these synchronized with code.
- Use /api/v1 for management/discovery endpoints. Preserve the Git LFS protocol
  routes under /api/{organization}/{repository}; do not change their wire format.
- Return JSON with appropriate HTTP statuses and stable machine-readable error
  codes. Validate inputs, bound expensive work, and paginate collections.
- State backend capabilities and consistency limits honestly; never substitute a
  cache-only listing for permanent inventory or return partial success on errors.
- Avoid exposing credentials, internal paths, or raw backend errors. Apply the
  same access policy to the UI and API. Rudolfs currently has no built-in client
  authentication; deployment access controls must cover all routes.
- Test API success, validation, pagination, and backend failures; test UI loading,
  empty, error, and success states and relevant user interactions.
- Keep the UI accessible, responsive, and useful when network requests fail.

Use the existing checkout in this isolated cloud task; create no worktree unless
explicitly requested. For local checks initialize CARGO_HOME=/workspace/.tools/cargo,
RUSTUP_HOME=/workspace/.tools/rustup and add $CARGO_HOME/bin to PATH when using the
onboarded environment. Use cargo test --locked and cargo build --locked.

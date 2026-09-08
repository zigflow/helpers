# Zigflow Helpers agent instructions

This repository contains **Zigflow Helpers**, a collection of reusable helpers
for applications built with the **Temporal Go SDK**.

The package is named `temporal` and is normally imported as:

```go
import temporal "github.com/zigflow/helpers"
```

Zigflow Helpers prioritises:

* correctness
* compatibility with the Temporal Go SDK
* small, focused abstractions
* explicit behaviour
* idiomatic Go
* useful defaults without hiding Temporal

These instructions apply to any agent working in this repository.

---

## Core intent

Zigflow Helpers exists to remove repetitive plumbing and make common Temporal
patterns easier to implement correctly.

It is a **thin convenience layer over the Temporal Go SDK**.

It is not:

* a replacement Temporal SDK
* a workflow engine
* a framework that hides Temporal concepts
* an alternative abstraction for Activities, Workflows, Futures, Workers, or
  Clients

Users should remain able to use normal Temporal SDK functionality directly
alongside these helpers.

When adding an abstraction, ask:

> Does this make an existing Temporal pattern easier, or does it invent a new
> way of using Temporal?

Prefer the former.

---

## Design philosophy

Preserve the Temporal knobs.

Helpers may:

* reduce repeated boilerplate
* provide sensible defaults
* encapsulate common lifecycle patterns
* compose Temporal SDK options
* make correct Temporal behaviour easier to achieve
* adapt existing libraries to Temporal SDK interfaces

Helpers should not:

* obscure important Temporal behaviour
* replace SDK types unnecessarily
* introduce parallel implementations of SDK functionality
* prevent callers from using underlying Temporal options
* invent abstractions solely because they may be useful later

Prefer:

* Temporal SDK types over equivalent local types
* composition over replacement
* small helpers over frameworks
* explicit behaviour over magic
* standard library solutions over custom machinery
* abstractions justified by real use cases

Avoid speculative generalisation.

If only one concrete use case exists, solve that use case first rather than
building an extensibility system around hypothetical future requirements.

---

## Temporal-specific constraints

Workflow code must remain deterministic and replay safe.

Do not introduce:

* wall-clock time inside Workflow code
* random behaviour that bypasses Temporal APIs
* non-deterministic I/O from Workflow code
* goroutines where Temporal workflow primitives are required
* hidden side effects
* SDK behaviour that differs from normal Temporal semantics without explicit
  documentation

Side effects and external I/O belong in Activities unless Temporal explicitly
provides a replay-safe Workflow API for the operation.

When wrapping Temporal functionality:

* preserve normal Temporal errors where practical
* preserve normal SDK types where practical
* preserve cancellation behaviour unless the helper explicitly exists to alter
  it
* preserve retry and timeout configuration
* preserve the caller's ability to configure SDK behaviour
* do not silently swallow Temporal control-flow errors

Special Temporal behaviour such as Continue-As-New should remain recognisable
as Temporal behaviour rather than being replaced with a custom workflow
mechanism.

---

## Continue-As-New and workflow lifecycle helpers

Helpers that automate Continue-As-New should automate the repetitive Temporal
lifecycle behaviour, not reimplement workflow execution.

Use Temporal's own APIs and semantics, including:

* `workflow.GetInfo`
* `GetContinueAsNewSuggested`
* `workflow.AllHandlersFinished`
* `workflow.NewContinueAsNewError`

Continue-As-New starts a new Workflow Run. It does not resume Go execution at
the previous instruction.

Any helper that provides automatic resumption must make its state and
checkpoint semantics explicit and deterministic.

Do not build an alternative Workflow engine inside the helper library.

Where state is persisted across Continue-As-New:

* state must be serialisable
* state semantics must be documented
* replay and versioning implications must be considered
* the point at which state advances must be unambiguous
* work that did not successfully execute must not accidentally be marked as
  completed

Prefer Temporal-native state passing over hidden persistence.

---

## Public API design

Treat every exported type, function, method, constant, and behaviour as a
potential long-term compatibility commitment.

Before exporting something, consider whether it genuinely needs to be public.

Public APIs should:

* have clear GoDoc comments
* use idiomatic Go naming
* expose Temporal SDK types directly where appropriate
* return useful errors
* have predictable zero-value behaviour where practical
* compose cleanly with the Temporal SDK

Avoid:

* unnecessary interfaces
* interfaces with only one speculative implementation
* generic abstractions without a concrete need
* duplicate local representations of Temporal SDK concepts
* Boolean options whose meaning is unclear at the call site

Prefer functional options when several independent pieces of configuration are
needed and option ordering has useful, predictable semantics.

Option application order should be deterministic. When later options override
earlier options, preserve that behaviour deliberately and test it.

---

## Error handling

Errors are part of the public contract.

Prefer:

```go
return fmt.Errorf("error doing something: %w", err)
```

when additional context is useful.

Use `errors.Is` and `errors.As` where callers need to inspect wrapped errors.

Do not:

* discard errors silently
* replace an important original error with a secondary cleanup error without a
  deliberate reason
* use panic for normal error handling
* terminate the host process from reusable library code unless the behaviour is
  explicitly documented and intentional

When several independent cleanup or post-processing operations should all be
attempted, `errors.Join` is appropriate where returning the combined failures
is useful.

Temporal control-flow errors should not be casually wrapped or converted if
doing so changes their meaning.

---

## Logging

Use Temporal's logger from Workflow code:

```go
logger := workflow.GetLogger(ctx)
```

Logging must remain replay safe.

Use structured key/value pairs.

Prefer useful contextual information over verbose tracing.

Do not assume that every Temporal `log.Logger` implements optional logger
interfaces such as `log.WithLogger`.

Library code must remain compatible with the base Temporal logger interface
unless explicitly documented otherwise.

Avoid logging the same error repeatedly at several layers without adding useful
context.

---

## Context handling

Respect Go and Temporal context semantics.

For normal Go code, use `context.Context`.

For Workflow code, use `workflow.Context`.

Do not treat the two as interchangeable.

When intentionally changing cancellation semantics, make that behaviour
explicit. For example, saga compensation uses a Temporal disconnected context
so compensation can continue after the original Workflow context is
cancelled.

Avoid storing Workflow contexts in long-lived configuration objects.

Pass the Workflow context through the execution path where it is needed.

---

## Saga compensation

`Compensator` implements a simple LIFO saga compensation pattern.

Its intended behaviour is:

* the zero value is usable
* compensations are registered after successful forward steps
* compensations execute in reverse order
* all registered compensations are attempted even when one fails
* compensation failures are logged
* compensation failures do not replace the original Workflow failure
* compensation runs using a disconnected Workflow context

Do not add additional saga semantics without a concrete requirement.

In particular, do not accidentally make implementation details such as repeated
calls to `Compensate` part of the public contract unless deliberately designed
and documented.

---

## Connection helpers

Connection helpers should produce ordinary Temporal SDK clients and options.

They must not hide or replace `client.Options`.

Functional options should compose predictably.

When options affect overlapping SDK configuration:

* define precedence clearly
* preserve unrelated settings
* test ordering behaviour
* avoid generic reflection-based or "merge every non-zero field" machinery

Prefer narrow, explicit merging behaviour over clever generic merging.

Environment-based configuration is a starting point. Explicitly supplied
options should take precedence over environment-derived values.

Experimental Temporal SDK features should remain described as experimental
where the underlying SDK marks them as such.

---

## Resource lifecycle

If a helper owns a resource, make ownership clear.

Resources such as:

* Temporal clients
* HTTP servers
* listeners
* metrics scopes
* reporters

must have deliberate lifecycle behaviour.

Where callers need lifecycle control, expose it.

Convenience APIs may intentionally create process-lifetime resources, but this
trade-off must be documented.

Do not asynchronously terminate the caller's process because a background
resource fails unless that behaviour is explicitly part of the API contract.

Prefer synchronous validation of immediately detectable startup failures where
possible.

---

## Test philosophy

Use tests to describe meaningful public behaviour rather than incidental
implementation details.

Prefer classic TDD for behavioural changes:

1. write or update a test that demonstrates the desired behaviour
2. confirm it fails for the expected reason
3. make the smallest production change that satisfies it
4. run the focused test
5. run the full validation suite

Use:

* `testify/assert`
* `testify/require`
* table-driven tests where they improve clarity

Tests should be:

* deterministic
* isolated
* repeatable
* safe under shuffled execution
* safe under the race detector where applicable

Do not:

* depend on sleeps where deterministic synchronisation is possible
* depend on test execution order
* accidentally modify process globals without restoring them
* share global Prometheus registries or HTTP muxes between tests without
  explicit isolation
* use an external Temporal Server for unit tests where the Temporal SDK test
  environment is sufficient

When testing Temporal Workflow behaviour, prefer proving observable behaviour
over inspecting SDK implementation details.

For example, to test a disconnected Workflow context, prove that useful
Workflow work can execute after parent cancellation rather than merely
comparing context values.

---

## Test isolation

Some dependencies use process-global state.

Tests touching global state such as:

* `http.DefaultServeMux`
* Prometheus default registries
* environment variables
* package-level configuration

must save the original value, install isolated state, and restore it using
`t.Cleanup`.

Do not use `t.Parallel` for tests that mutate shared process-global state unless
the isolation mechanism genuinely makes parallel execution safe.

Tests should remain stable under repeated and shuffled execution.

---

## Validation

After making Go changes, run the relevant focused tests first, then the full
suite.

At minimum run:

```bash
go test ./...
go test -race ./...
go vet ./...
golangci-lint run ./...
pre-commit run -a
```

For changes involving test isolation or shared state, also run repeated and
shuffled tests where appropriate:

```bash
go test ./... -count=10
go test ./... -shuffle=on -count=10
```

The repository CI tests the supported Go versions declared by the project.

The minimum supported Go version follows the minimum supported version of the
Temporal Go SDK.

Do not lower or raise the minimum Go version casually. Treat it as a
compatibility decision.

The pre-commit configuration includes checks for:

* licence headers
* Go formatting and imports
* `go vet`
* `gofumpt`
* error checking
* static analysis
* `golangci-lint`
* JSON and YAML validity
* Markdown TOC generation
* Markdown linting
* trailing whitespace
* end-of-file formatting
* TODO scanning

Do not disable linters or add `nolint` directives merely to make validation
pass.

If a lint rule appears inappropriate, understand the underlying issue before
changing or suppressing it.

---

## Formatting and Go style

Prefer idiomatic Go.

Use:

* `gofmt`
* `gofumpt`
* standard Go naming conventions

Prefer:

* small functions
* explicit control flow
* early returns
* standard library functionality
* clear names
* zero-value-safe types where appropriate

Avoid:

* cleverness
* unnecessary reflection
* unnecessary generics
* unnecessary interfaces
* deep nesting
* premature abstraction

Generics are appropriate when they materially improve type safety or remove
real duplication. Do not use them merely because different types happen to
look structurally similar.

---

## Dependencies

Keep dependencies deliberate.

Before adding a dependency, consider whether the standard library or an
existing dependency already solves the problem.

Do not remove `github.com/mrsimonemms/golang-helpers` merely because this
repository is also a helpers library. It contains general-purpose Go helpers,
while this repository contains Temporal-specific helpers.

Keep Temporal dependencies current deliberately rather than through unrelated
dependency churn.

When upgrading the Temporal Go SDK:

* check its minimum supported Go version
* review relevant release notes
* update compatibility documentation where required
* run the full test and lint suite

---

## Documentation expectations

Documentation is part of the public API.

The README should describe currently supported behaviour rather than planned
behaviour.

Every exported symbol should have appropriate GoDoc.

Examples should compile conceptually against the current API and should use
normal Temporal SDK patterns.

Documentation should reinforce the central project philosophy:

> These helpers build on the Temporal Go SDK rather than replacing it.

Do not present a helper as replacing or abstracting away Temporal when it only
automates a common pattern.

Avoid aspirational documentation for functionality that does not yet exist.

---

## Documentation drift and behavioural changes

Documentation represents the intended and supported behaviour of the library.

When behaviour changes, determine whether the change is:

1. **Intentional**

   * update documentation and tests to describe the new behaviour
   * consider compatibility and versioning implications

2. **Unintentional**

   * do not update documentation to legitimise the regression
   * fix the implementation instead

If code, documentation, and tests disagree, determine which reflects the
intended public contract.

Do not blindly change one to match another.

If a behaviour is merely an implementation detail, avoid documenting or
testing it as a guaranteed public contract.

---

## Examples

Examples should demonstrate realistic Temporal usage.

Keep ordinary SDK calls visible:

```go
workflow.ExecuteActivity(...)
workflow.ExecuteChildWorkflow(...)
workflow.Sleep(...)
```

Do not replace them with local equivalents merely to make an example use more
of this library.

A helper should surround or compose normal Temporal functionality where useful,
not force all Temporal operations through a Zigflow Helpers abstraction.

Examples may use local Temporal development infrastructure where useful, but
library unit tests should not depend on an external Temporal Server unless
there is a concrete integration-testing requirement.

---

## Scope and uncertainty

Start with the smallest correct change.

Do not:

* invent new product features
* create extension points without a concrete use case
* generalise a helper merely because another use case might exist later
* refactor unrelated code while implementing a focused change
* silently change public behaviour

Follow existing patterns unless there is a clear reason to improve them.

When an abstraction starts to resemble a replacement Temporal API, reconsider
the design.

Correctness and Temporal compatibility matter more than reducing every line of
boilerplate.

---

## Git workflow

Do not stage files.

Do not run:

```bash
git add
```

Do not commit or push unless explicitly instructed to do so.

The user owns staging, commits, and pushes.

It is fine to inspect:

```bash
git status
git diff
git diff --stat
```

Use Conventional Commits when proposing commit messages.

Do not modify unrelated files merely to produce a clean working tree.

---

## Licence headers

Go source files must retain the repository's Apache 2.0 licence header.

New Go source files should use the existing repository header:

```text
Copyright 2026 Zigflow authors <https://github.com/zigflow/helpers/graphs/contributors>
```

Do not remove or alter existing licence headers unless explicitly required.

---

## Writing style

Use British English.

Do not use em dashes. Use commas, full stops, colons, or sentence
restructuring instead.

Prefer:

* concise technical prose
* sentence-case headings
* short paragraphs
* clear bullet lists
* fenced code blocks with language identifiers

Avoid:

* marketing language
* unnecessary superlatives
* vague claims
* excessive commentary

Use **Temporal** with its normal capitalisation.

Use **Zigflow** with a capital `Z` and lowercase `f` when referring to the
project or organisation in prose.

---

## Definition of done

A change is complete when:

* the smallest appropriate implementation has been made
* relevant tests cover the intended behaviour
* existing behaviour remains intact unless intentionally changed
* documentation matches the supported behaviour
* formatting passes
* unit tests pass
* race-sensitive tests pass where relevant
* `go vet` passes
* `golangci-lint` passes
* pre-commit hooks pass
* no unrelated changes have been introduced
* nothing has been staged or committed unless explicitly requested

Do not optimise for making the diff large or comprehensive.

Optimise for making it correct, understandable, and appropriately small.

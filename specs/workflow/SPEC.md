# Event-Driven Workflow Engine — Design Spec

| | |
|---|---|
| **Status** | Draft v1.0 — reviewed, scenario walked through end to end |
| **Owner** | enxent |
| **Audience** | Backend/platform developers, solution architects |
| **Reference scenario** | Azure Databricks provisioning via GitHub Actions deployment |

## How to read this document

This spec describes an event-driven workflow orchestration engine — the internal equivalent of Logic Apps / n8n / Step Functions — built on React (UI), FastAPI (API), Postgres (state), Azure Service Bus (event queue), and an Azure Function (execution engine). Sections 1–12 define the engine itself: data model, workflow definition format, execution semantics, and API surface. Section 13 walks one real scenario through every layer of the system, because the abstract design only means something once it's been pressure-tested against something concrete. Section 14 specifies the UI screens the frontend needs to build. Section 15 lists what's still open. Appendix A is a decision log, kept so a reader can see not just what was decided but why, and can trace a decision back to the discussion that produced it.

---

## 1. Purpose & Scope

The engine lets a request go through an arbitrary graph of steps — approvals, transformations, external API calls, conditional branches, parallel work, long-running waits, and LLM-driven agentic loops — triggered from the UI today, with the trigger mechanism designed to be pluggable for webhook/schedule/event triggers later. A workflow is authored as JSON (with an optional visual builder on top), versioned, and executed by a horizontally-scalable, at-least-once-tolerant engine.

**In scope for this version:** UI-triggered workflows; parallel and gated approvals; conditional/fan-out/fan-in/loop/agentic node types; JSON import/export; immutable node inputs/outputs with global/run/iteration-scoped variables; per-node execution conditions; a stop signal; runtime variable mutation; externally-referenced connections; workflow and node-kind versioning; retry/timeout policies; React Flow visualization.

**Explicitly out of scope for this version:** sub-workflow composition (calling one workflow from another); triggers other than manual/UI.

---

## 2. Terminology

| Term | Meaning |
|---|---|
| Workflow | A named, versioned graph of nodes. |
| Workflow version | An immutable, published snapshot of a workflow's definition. A run always executes against the exact version it started with. |
| Run | One execution instance of a workflow version, created by a trigger. |
| Node | One step in the graph. Identified by `id`; its behavior is defined by its `kind` + `version`. |
| Node kind | A registered, versioned contract (input/output/config schema, connection roles, default policies, events) that a node instance references. |
| Node execution | The runtime record of one node's execution within one run (or one iteration of a fan-out branch). |
| Event | Something a node kind's executor reports happening — some events are terminal (resolve the node), most are not. |
| Connection | A reference to externally-configured credentials (e.g. a GitHub token), resolved per environment. Never stored in workflow JSON. |

---

## 3. System Architecture

```mermaid
flowchart LR
  UI["React UI<br/>Builder / Runs / Approvals"]
  API["FastAPI"]
  DB[("Postgres<br/>state + audit")]
  SB[["Service Bus<br/>event queue"]]
  FN["Azure Function<br/>Engine"]
  KV[("Key Vault<br/>connections")]
  BLOB[("Blob Storage<br/>large payloads")]
  EXT["External systems<br/>GitHub, SMTP, Databricks..."]

  UI <--> API
  API <--> DB
  API -- "RunStarted / Stop / ApprovalResolved" --> SB
  SB -- "deliver event" --> FN
  FN <--> DB
  FN -- "resolve connection alias" --> KV
  FN -- "call connector" --> EXT
  FN -- "large output" --> BLOB
  FN -- "schedule re-check" --> SB
  UI -. "poll run status" .-> API
```

The Engine never talks to the UI directly — every state change lands in Postgres first, so the UI's view of a run is always a read of committed state, never a race with in-flight processing.

The API tier owns writes originating from people: creating/publishing workflows, resolving connection aliases at trigger time, recording approval decisions, issuing stop requests. The Engine owns writes originating from execution: node state transitions, variable mutations, the event log. Neither tier bypasses Postgres to talk to the other directly — Service Bus is the only channel between them, which keeps the Engine horizontally scalable without the API needing to know how many Function instances are running.

---

## 4. Data Model

```mermaid
erDiagram
  WORKFLOW ||--o{ WORKFLOW_VERSION : has
  WORKFLOW_VERSION ||--o{ WORKFLOW_RUN : "pinned by"
  WORKFLOW_RUN ||--o{ NODE_EXECUTION : contains
  WORKFLOW_RUN ||--o{ VARIABLE : scopes
  WORKFLOW_RUN ||--o{ EVENT : logs
  NODE_KIND_DEFINITION ||--o{ NODE_EXECUTION : "instance of"
  NODE_EXECUTION ||--o{ APPROVAL_REQUEST : may_create
  APPROVAL_GROUP ||--o{ APPROVAL_REQUEST : "targeted by"
  APPROVAL_REQUEST ||--o{ APPROVAL_DECISION : receives
  APPROVAL_GROUP ||--o{ APPROVAL_GROUP_MEMBER : has
  WORKFLOW ||--o{ CONNECTION_BINDING : declares
  CONNECTION_BINDING }o--|| CONNECTION : "resolves to"
```

### workflow_version

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | Primary key. |
| `workflow_id` | uuid | FK to workflow. |
| `version_number` | int | Monotonic per workflow, assigned on publish. |
| `status` | enum | draft / published / archived. |
| `definition` | jsonb | The full workflow JSON — immutable once published. |
| `published_at` | timestamptz | Null while draft. |

### workflow_run

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | Primary key. |
| `workflow_version_id` | uuid | Pinned at trigger time — never changes for the life of the run. |
| `status` | enum | See §7.1. |
| `trigger_payload` | jsonb | Immutable input the run started with. |
| `connection_bindings` | jsonb | Alias → connection map, resolved once at trigger time. |
| `stop_requested_at` | timestamptz | Set by the API; the Engine checks this before starting any new node. |

### node_execution

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | Primary key. |
| `run_id` | uuid | FK to workflow_run. |
| `node_id` | text | Node id from the workflow definition. |
| `iteration_key` | text | Null for single-shot nodes; branch index for a fan-out instance. |
| `attempt` | int | Increments on retry only — not on a poll recheck. |
| `idempotency_key` | text | `run_id:node_id:iteration_key:attempt` — unique constraint. A redelivered message that finds this row already terminal is a no-op. |
| `status` | enum | See §7.2 — what the graph acts on. |
| `input_snapshot` | jsonb | Resolved inputs after JSONata evaluation (not the raw expressions). |
| `state` | jsonb | Opaque scratchpad only the node kind's own executor reads and writes — internal phase, counters, anything needed across repeated invocations while non-terminal. The Engine merges it in and passes it back; it never inspects the contents (§6.2). |
| `output` | jsonb / blob ref | Set once, on the terminal event. This — **not** `state` — is what `$node('id').output` exposes downstream. Inlined under a size threshold, otherwise a Blob Storage reference. |

### event (append-only audit log)

| Column | Type | Notes |
|---|---|---|
| `run_id` | uuid | FK to workflow_run. |
| `sequence` | bigint | Monotonic per run — the ordering the UI replays to render run history. |
| `event_type` | text | `NodeStarted`, `NodeSucceeded`, `ApprovalRequested`, `VariableSet`, `StopRequested`, or any kind-specific event (§6.2). |
| `payload` | jsonb | Event-specific detail. |

The event log is what makes the Engine debuggable and resumable without being a full event-sourced system: `node_execution` is always the current-state view an Engine instance queries to decide what to do next; `event` is the immutable trail a human reads to answer "what happened, and when."

**Audit trail completeness.** Every event is captured here — terminal or not, whether raised by a node kind's executor (§6.2: `Checked`, `DecisionRecorded`, `RunProgressed`, and every other non-terminal event, not only the ones that resolve a node) or by the Engine itself (`RunStarted`, `NodeReady`, `ApprovalRequested`, `VariableSet`, `StopRequested`). Rows are appended only — nothing is ever updated in place or deleted. The Audit Trail screen (§14.5) is a direct, unfiltered read of this table ordered by `sequence`; nothing that happens during a run is invisible to it.

---

## 5. Workflow Definition Format

### 5.1 Field schema convention

Every schema-defining block in the system — a trigger's `payloadSchema`, a variable declaration, a node kind's declared input/output/config schema — uses one field shape:

```json
{ "type": "string", "label": "Resource Name", "description": "...", "required": true, "default": "..." }
```

`label` and `description` are what the visual builder shows a person filling in a form; `default` is optional and only meaningful when the field isn't `required`.

### 5.2 Variables & scopes

`variables` is a dictionary keyed by **scope**, then by name — never an array, so both the scope and the name are unambiguous from the key path itself:

```json
"variables": {
  "global": {
    "defaultRegion": { "type": "string", "access": "ro", "label": "Default Region",
                        "description": "Fallback Azure region used when a request omits one", "default": "eastus" }
  },
  "run": {
    "deploymentBranch": { "type": "string", "access": "rw", "label": "Deployment Branch",
                           "description": "Git branch the deployment JSON is committed to for this run" }
  }
}
```

| Scope key | Lifetime | Declared where |
|---|---|---|
| `trigger` | Life of the run | Not in `variables` at all — it's `trigger.payloadSchema`, and always read-only. |
| `global` | Persists across runs, shared across workflows | Registered once in the platform's global registry; a workflow's `variables.global` block only lists the ones it actually reads or writes, for validation and builder autocomplete. |
| `run` | Life of this run | `variables.run`, in this workflow's own definition. |
| `iteration` | One `control.forEach` iteration, discarded after | Inside that `forEach` node's own `config`, not the top-level `variables` block — it doesn't exist outside that loop. |

A reference is always `$vars.<scope key>.<name>` — e.g. `$vars.global.defaultRegion`, `$vars.run.deploymentBranch` — so scope travels with every expression, not just the declaration.

A `control.setVariable` node execution takes a row lock (`SELECT … FOR UPDATE`) on the target variable before writing, so two parallel branches racing to update the same `rw` variable serialize instead of silently losing one write. Publish-time validation rejects any workflow that targets a variable declared `ro`.

### 5.3 Run context

Alongside `$trigger`, `$vars`, and `$node`, every expression can read `$context` — engine-assembled, always read-only, never something a workflow author sets:

| Field | Meaning |
|---|---|
| `$context.workflowId` | The workflow's stable id — constant across every version. |
| `$context.workflowVersion` | The pinned version number this run is executing. |
| `$context.runId` | This run's id, matching `workflow_run.id`. |
| `$context.environment` | The environment connection aliases were resolved against. |
| `$context.startedAt` / `.startedBy` | When the run was triggered, and by whom. |
| `$context.node.id` / `.attempt` / `.iterationKey` | Identifies the node execution currently evaluating the expression. |

**Expressions are JSONata.** `$trigger`, `$vars.<scope>.<name>`, `$node('id').output` / `$node('id').status`, and `$context` are the reference roots every node input, `when`, and edge `metadata` can draw from — never a raw variable name, so a schema linter can statically catch a typo'd reference at publish time rather than run time. Edge `metadata` is resolved exactly like a node's `inputs` — every value is a JSONata expression; a fixed label is just a string literal (`'approved'`) — nothing in the mechanism distinguishes "static" from "computed."

### 5.4 Control flow lives on the node

Each node carries its own `next` list of successors — target id, optional `when` guard, optional `metadata` map. There is no separate top-level edge array: the Engine derives every node's structural predecessors by scanning who names it in their `next`. Visual position is a separate `layout` block the Engine never reads.

- **Static fan-out** needs no special kind: a node with more than one `next` entry and no discriminating `when` has every target become eligible independently.
- **Dynamic fan-out** (an unknown-until-runtime array) is `control.forEach` — it creates one `node_execution` of its child node per array item, each with a distinct `iteration_key`, up to a concurrency cap.
- **Fan-in** has no dedicated kind: it's the `join` field on whichever downstream node has more than one structural predecessor — `all` (default), `any`, or an explicit N-of-M count, evaluated against exactly the predecessors the graph shape gives it.

### 5.5 Connections

A node doesn't reference a connection alias directly — it maps its own required roles to workflow-level aliases, since a node can need more than one connection at once:

```json
"connections": { "repo": "github-deploy", "source": "azure-blob-artifacts" }
```

The registry entry for the node's kind declares which roles are required and what connection type each expects (§6.1); the workflow author fills in which alias plays which role, and publish-time validation checks the type matches. See §10 for alias resolution.

### 5.6 Worked example (abbreviated)

```json
{
  "workflowId": "wf_databricks_provision",
  "version": 1,
  "metadata": { "name": "Provision Azure Databricks", "owner": "platform-eng" },

  "connections": [
    { "alias": "github-deploy", "type": "github" },
    { "alias": "smtp-notify",   "type": "smtp"   }
  ],

  "variables": {
    "global": {
      "defaultRegion": { "type": "string", "access": "ro", "label": "Default Region",
                          "description": "Fallback Azure region used when a request omits one", "default": "eastus" }
    },
    "run": {
      "deploymentBranch": { "type": "string", "access": "rw", "label": "Deployment Branch",
                             "description": "Git branch the deployment JSON is committed to for this run" }
    }
  },

  "trigger": {
    "kind": "manual",
    "payloadSchema": {
      "resourceName":   { "type": "string", "label": "Resource Name",   "description": "Name of the resource that will be created in Azure", "required": true },
      "region":         { "type": "string", "label": "Azure Region",    "description": "Target Azure region for the deployment", "required": true, "default": "eastus" },
      "requesterEmail": { "type": "string", "label": "Requester Email", "description": "Where the completion notification is sent", "required": true }
    },
    "next": [ { "to": "approve-platform" }, { "to": "approve-security" } ]
  },

  "onError": { "to": "notifyOpsFailure" },

  "nodes": [
    {
      "id": "approve-platform", "kind": "approval", "version": 1,
      "name": "Platform leads approval",
      "config": { "groupId": "platform-leads", "quorum": 1 },
      "next": [ { "to": "bothApproved" } ]
    },
    {
      "id": "approve-security", "kind": "approval", "version": 1,
      "name": "Security review approval",
      "config": { "groupId": "security-review", "quorum": 2 },
      "next": [ { "to": "bothApproved" } ]
    },
    {
      "id": "bothApproved", "kind": "control.switch", "version": 1,
      "name": "Both groups approved?",
      "join": "all",
      "next": [
        { "to": "buildJson",
          "when": "$node('approve-platform').status='Succeeded' and $node('approve-security').status='Succeeded'",
          "metadata": { "label": "'approved'" } },
        { "to": "notifyRejected",
          "when": "$node('approve-platform').status='Failed' or $node('approve-security').status='Failed'",
          "metadata": { "label": "'rejected by ' & ($node('approve-platform').status='Failed' ? 'platform-leads' : 'security-review')" } }
      ]
    },
    {
      "id": "buildJson", "kind": "action.transform.jsonBuild", "version": 1,
      "inputs": { "resourceName": "$trigger.resourceName", "region": "$vars.global.defaultRegion" },
      "next": [ { "to": "commit" } ]
    },
    {
      "id": "commit", "kind": "action.github.commit", "version": 1,
      "connections": { "repo": "github-deploy" },
      "inputs": {
        "branch": "$vars.run.deploymentBranch",
        "path": "'deployments/' & $trigger.resourceName & '-' & $context.runId & '.json'",
        "content": "$node('buildJson').output"
      },
      "policies": {
        "retry":   { "maxAttempts": 3, "backoff": "exponential", "initialDelaySeconds": 15 },
        "timeout": { "seconds": 60 }
      },
      "next": [ { "to": "awaitWorkflowRun" } ]
    },
    {
      "id": "awaitWorkflowRun", "kind": "action.github.awaitWorkflowRun", "version": 1,
      "connections": { "repo": "github-deploy" },
      "inputs": { "commitSha": "$node('commit').output.commitSha" },
      "config": {
        "locate": { "intervalSeconds": 10, "maxDurationSeconds": 300 },
        "await":  { "intervalSeconds": 30, "maxDurationSeconds": 1800 }
      },
      "next": [
        { "to": "notify",             "when": "$node('awaitWorkflowRun').output.conclusion='success'" },
        { "to": "notifyDeployFailed", "when": "$node('awaitWorkflowRun').output.conclusion!='success'" }
      ]
    },
    {
      "id": "notify", "kind": "action.email.send", "version": 1,
      "connections": { "smtp": "smtp-notify" },
      "inputs": { "to": "$trigger.requesterEmail", "subject": "'Databricks deployment complete (run ' & $context.runId & ')'" },
      "next": []
    },
    {
      "id": "notifyDeployFailed", "kind": "action.email.send", "version": 1,
      "connections": { "smtp": "smtp-notify" },
      "inputs": { "to": "$trigger.requesterEmail", "subject": "'Databricks deployment failed (run ' & $context.runId & ')'" },
      "next": []
    },
    {
      "id": "notifyRejected", "kind": "action.email.send", "version": 1,
      "connections": { "smtp": "smtp-notify" },
      "inputs": { "to": "$trigger.requesterEmail", "subject": "'Databricks request was rejected (run ' & $context.runId & ')'" },
      "next": []
    },
    {
      "id": "notifyOpsFailure", "kind": "action.email.send", "version": 1,
      "connections": { "smtp": "smtp-notify" },
      "inputs": { "to": "'platform-eng-oncall@company.example'",
                  "subject": "'Workflow error (run ' & $context.runId & ')'" },
      "next": []
    }
  ],

  "layout": { "approve-platform": { "x": 40, "y": 40 }, "approve-security": { "x": 40, "y": 160 } }
}
```

`buildJson`'s actual output depends on what the target repo's pipeline consumes — Terraform tfvars, a Bicep parameters file, or a custom manifest. The shape implied above (`resourceType`, `name`, `region`, `requestedBy`, `runId`) is a placeholder — **confirm against the real deployment repo before finalizing this node's schema.**

---

## 6. Node Kind Registry

### 6.1 Kind catalogue

A node in a workflow definition is a reference, not a description — the contract lives in a registry entry keyed by `kind` and `version`, so a published workflow can't silently change behavior when a connector is updated.

| Category | Example kind | Purpose |
|---|---|---|
| action | `action.github.commit` | Single unit of work against a connector. |
| approval | `approval` | Single-group sign-off gate. Multi-group approval composes several of these (§8.1). |
| control | `control.switch` | No-op branch point: a `join` policy plus guarded `next` entries. |
| fan-out | `control.forEach` | Dynamic parallel branches over a runtime array, with a concurrency cap. Static fan-out needs no dedicated kind (§5.4). |
| loop | `control.pollUntil` | Single-condition retry-until-condition (§8.4). |
| custom composite | `action.github.awaitWorkflowRun` | Purpose-built, multi-phase wait — see §6.2, §8.4, §13.2. |
| agentic | `agent.loop` | LLM-driven tool-calling loop with iteration/cost/timeout caps. |
| variable | `control.setVariable` | Writes a scoped or global `rw` variable. |
| terminal | `control.stop` | Node-initiated graceful stop. |

Every registry entry declares: an input schema, output schema, and optional config schema (all built from the §5.1 field shape); the named connection roles it requires (zero, one, or several — §5.5); and default `policies` (§11) a node instance can override. This is also the palette the visual builder reads to render the node picker — it never hardcodes a node list. There's no dedicated fan-in kind, per §5.4.

### 6.2 Node event model

A kind's manifest declares the events it can raise, and which of those are **terminal** — the ones that resolve the node to a final status and let the Engine move on to its `next` list. Not every non-terminal event is inert: some move the node through internal phases without the graph ever seeing it.

| Kind | Events it raises | Terminal events → status |
|---|---|---|
| `action.*` | Started, Completed, Errored | Completed→Succeeded, Errored→Failed |
| `approval` | Requested, DecisionRecorded, QuorumReached, Rejected | QuorumReached→Succeeded, Rejected→Failed |
| `control.pollUntil` | Started, Checked, ConditionMet, ConditionFailed, MaxDurationExceeded | ConditionMet→Succeeded, ConditionFailed / MaxDurationExceeded→Failed |
| `control.switch` | Evaluated | Evaluated→Succeeded |
| `agent.loop` | Started, IterationStarted, ToolInvoked, IterationCompleted, StoppedByModel, MaxIterationsExceeded, MaxCostExceeded, TimedOut | StoppedByModel→Succeeded, everything else terminal→Failed |
| `action.github.awaitWorkflowRun` | LocateStarted, LocateProgressed, RunLocated, LocateTimedOut, RunProgressed, RunSucceeded, RunFailed, AwaitTimedOut | RunSucceeded→Succeeded, every other terminal event→Failed |

`control.pollUntil` is for a single condition against a single value — "wait until this variable changes," "wait until this timestamp passes." It stops being the right tool the moment a wait has more than one distinct phase, or needs real API-specific knowledge to run (like the head_sha correlation in §13.2) — that's when it earns its own kind instead of being contorted into `checkExpression` strings nobody can read. Either way it's still **one node** in the graph: the phases live inside the kind's own state machine, never as a second node.

### 6.3 How the Engine handles an event

Every kind ships an executor — the code the Engine actually calls. Each invocation gets back whatever `state` the node saved last time, does one unit of work, and returns an event plus updated state. What the Engine does with that is mechanical, and identical for every kind:

```mermaid
flowchart TD
  A["Service Bus delivers a message for this node_execution<br/>(a scheduled recheck, or an external event -<br/>an approval decision, a webhook)"] --> B["Engine loads node_execution.state<br/>+ the kind's manifest"]
  B --> C["Engine calls the kind's executor:<br/>executor(state, incomingEvent) returns { event, newState }"]
  C --> D["Engine appends { event, payload } to the event log,<br/>merges newState into node_execution.state"]
  D --> E{"Is event in the kind's<br/>terminalEvents map?"}
  E -- no --> F{"Does the kind self-schedule<br/>its next check?"}
  F -- "yes, on an interval" --> G["Publish a scheduled Service Bus message:<br/>invoke this node again at now + interval"]
  F -- "no, waits on an external actor" --> H["Nothing further - node stays Running<br/>until an external call arrives<br/>(e.g. POST /approvals/id/decision)"]
  E -- yes --> I["node_execution.status = mapped status,<br/>output = whatever the executor returned as final result"]
  I --> J["Engine walks this node's next list,<br/>evaluates each 'when', checks join policy<br/>on every target's predecessors (Section 7)"]
  J --> K["Eligible targets: insert node_execution<br/>(idempotent on run_id:node_id:iteration_key:attempt),<br/>publish NodeReady"]
```

The distinction that matters: `state` is scratch space only the kind's own executor ever reads — the Engine merges it in and hands it back next time without looking inside. `output` is set exactly once, on the terminal event, and it's the only one of the two `$node('id')` exposes downstream. A node can rewrite its `state` a dozen times while Running; nothing downstream sees any of it until `output` exists.

**Worked trace — `action.github.awaitWorkflowRun`.** One node, two phases, tracked entirely in `state.phase`:

| Tick | Event | Terminal? | `state.phase` | Node status | Engine does |
|---|---|---|---|---|---|
| T+0s | LocateStarted | no | locating | Running | schedule recheck at T+10s |
| T+10s | LocateProgressed | no | locating | Running | schedule recheck at T+20s |
| T+20s | RunLocated | no | watching | Running | switch cadence, schedule at T+50s |
| T+50s | RunProgressed | no | watching | Running | schedule recheck at T+80s |
| T+80s | RunProgressed | no | watching | Running | schedule recheck at T+110s |
| T+110s | RunSucceeded | **yes** | watching | Succeeded | set output, evaluate next — `notify` becomes eligible |

`RunLocated` is the one people expect to be terminal and isn't — it changes something real (the phase, the polling cadence, which GitHub endpoint the next check even calls) without resolving the node. That's the point of giving a kind its own phases: the graph only ever sees one thing happen to this node, at T+110s.

---

## 7. Execution State Machine

### 7.1 WorkflowRun

```mermaid
stateDiagram-v2
  [*] --> Created
  Created --> Running
  Running --> WaitingApproval
  WaitingApproval --> Running
  Running --> WaitingPoll
  WaitingPoll --> Running
  Running --> Completed
  Running --> Failed
  Running --> StopRequested
  StopRequested --> Stopped
  Completed --> [*]
  Failed --> [*]
  Stopped --> [*]
```

### 7.2 NodeExecution

```mermaid
stateDiagram-v2
  [*] --> Pending
  Pending --> Skipped: runCondition false
  Pending --> Running
  Running --> Succeeded
  Running --> Failed
  Failed --> Pending: retry, attempt+1
  Running --> WaitingApproval
  WaitingApproval --> Succeeded: terminal event maps to Succeeded
  WaitingApproval --> Failed: terminal event maps to Failed
  Running --> WaitingPoll
  WaitingPoll --> WaitingPoll: non-terminal event, self-scheduled recheck
  WaitingPoll --> Succeeded: terminal event maps to Succeeded
  WaitingPoll --> Failed: terminal event maps to Failed
  Succeeded --> [*]
  Failed --> [*]
  Skipped --> [*]
```

### 7.3 Core engine loop

The Engine's core loop is a single rule applied on every Service Bus delivery: when a `node_execution` reaches a terminal status, walk its `next` list and collect every target whose `when` is absent or evaluates true. For each candidate, check its `join` policy against its structural predecessors — every node in the definition whose own `next` names it, computed once when the version is published:

- `all` (default) — waits for every predecessor to reach a terminal status.
- `any` — needs just one.
- N-of-M — needs that many.

Once satisfied, the Engine attempts to insert the target's `node_execution` row keyed by its idempotency key. If the insert is rejected by the unique constraint, another delivery already claimed it — the current delivery is a no-op. This is what makes horizontal scale-out of the Function safe without a distributed lock, and it's the same rule whether the terminal status came from a plain action completing or from a long-waiting node's terminal event landing (§6.3).

---

## 8. Execution Patterns

### 8.1 Approval — one node per group, a switch to combine them

Multi-group approval is graph shape, not node config: each group gets its own single-purpose `approval` node (one `groupId`, one `quorum`); the trigger (or any predecessor) fans out into all required groups by simply listing them in its own `next`; and a plain `control.switch` node — a no-op branch point with a `join` policy and guarded `next` entries — reads every group's terminal status and decides which path continues. The same mechanism handles any "did enough of these prior steps succeed" gate, not only approvals.

```mermaid
sequenceDiagram
  participant Engine
  participant DB as Postgres
  participant PlatformApprover as Platform-leads approver
  participant SecurityApprovers as Security-review approvers
  participant API as FastAPI
  participant SB as Service Bus

  Engine->>DB: create approve-platform, approve-security (Running)
  Engine->>DB: raise Requested on both
  PlatformApprover->>API: POST /approvals/approve-platform/decision
  API->>DB: DecisionRecorded, quorum(1) reached
  API->>SB: publish NodeEvent(approve-platform, QuorumReached)
  SB->>Engine: deliver
  Engine->>DB: approve-platform.status = Succeeded
  Note over Engine,DB: bothApproved still waiting - approve-security not terminal
  SecurityApprovers->>API: POST decision (1 of 2)
  API->>DB: DecisionRecorded, quorum not yet reached
  SecurityApprovers->>API: POST decision (2 of 2)
  API->>DB: DecisionRecorded, quorum(2) reached
  API->>SB: publish NodeEvent(approve-security, QuorumReached)
  SB->>Engine: deliver
  Engine->>DB: approve-security.status = Succeeded
  Engine->>DB: bothApproved.join="all" now satisfied - evaluate its next
  Engine->>SB: publish NodeReady(buildJson)
```

Each group's approvers only ever see and act on their own node — an "agent" approving is just a group member calling the same two endpoints with a service credential instead of a browser session. Nothing about combining groups lives inside the approval kind; the downstream switch decides, so the identical mechanism handles three groups, five groups, or a mix of approval and non-approval predecessors without the approval kind ever needing to change.

### 8.2 Fan-out / fan-in and the agentic loop

Static fan-out needs no special kind — a node with more than one `next` entry and no discriminating `when` simply has every target become eligible independently. Dynamic fan-out — an unknown-until-runtime array — is `control.forEach`: it evaluates a JSONata expression against prior output, then creates one `node_execution` per item, each with a distinct `iteration_key`, up to a concurrency cap. Fan-in, in both cases, is just the `join` field on whichever downstream node has more than one structural predecessor. The agentic loop node stays opaque to the surrounding graph either way — internally it runs its own bounded tool-calling loop and logs each iteration as a non-terminal event, but presents one input and one output to whatever node lists it as a predecessor.

### 8.3 Runtime variable mutation & stop

`control.setVariable` writes a scoped or global `rw` variable at runtime (§5.2). A stop request (`POST /runs/{runId}/stop`) is honored gracefully: the current node finishes, the next node never starts, and node kinds may optionally define a compensation handler for side effects already committed — nothing is silently rolled back.

### 8.4 Poll-until — a single-phase wait

```mermaid
sequenceDiagram
  participant Engine
  participant Target as External system
  participant DB as Postgres
  participant SB as Service Bus

  Engine->>Target: check condition (via the node's connection)
  Target-->>Engine: not yet
  Engine->>DB: append Checked, status = Running
  Engine->>SB: schedule recheck at now + intervalSeconds
  Note over Engine,SB: repeats until ConditionMet, ConditionFailed,<br/>or MaxDurationExceeded
  SB->>Engine: deliver scheduled message
  Engine->>Target: check condition
  Target-->>Engine: met
  Engine->>DB: node_execution.status = Succeeded
```

This is a scheduled Service Bus message (delayed enqueue), not a busy Function instance sitting in a loop — each check is its own short-lived invocation, which is both cheaper and survives a Function cold restart between checks. This is the shape for a single condition against a single value. §13.2 walks through what changes once a wait needs more than one phase — the GitHub Actions status check in the reference scenario is exactly that case, and gets its own kind (§6.2) rather than living here.

---

## 9. Connections

A connection's secret material lives in Key Vault and is never embedded in workflow JSON — a workflow only ever carries an alias.

```mermaid
flowchart LR
  WF["Workflow definition<br/>alias: github-deploy"]
  BIND["connection_binding<br/>(workflow, alias, environment) -&gt; connection_id"]
  CONN["connection<br/>metadata + Key Vault secret ref"]
  KV[("Key Vault")]

  WF --> BIND --> CONN --> KV
```

Alias resolution happens once, at trigger time — the API resolves every alias the pinned workflow version declares into a concrete connection for the current environment and snapshots that map onto the `workflow_run` row. The same published JSON promotes from dev to prod untouched; only the binding table changes per environment. A node's own `connections` map (§5.5) picks which alias fills which of its required roles.

---

## 10. Policies: Retry, Timeout, Idempotency

Retry and timeout live under a single `policies` object on the node rather than as top-level fields, since this is where a concurrency limit, a caching policy, or a circuit-breaker would land later without the node schema growing a new top-level key every time.

```json
"policies": {
  "retry":   { "maxAttempts": 3, "backoff": "exponential", "initialDelaySeconds": 15,
               "retryableErrors": ["Http5xx", "Timeout"] },
  "timeout": { "seconds": 60 }
}
```

Service Bus and Azure Functions are at-least-once — any action node can be invoked twice for the same logical step. The idempotency key (`run_id:node_id:iteration_key:attempt`) is what a connector implementation is expected to pass through to the target system where that system supports it (an idempotency header, a deterministic commit message tag), and at minimum it's what stops the Engine itself from creating a second `node_execution` row for a redelivered message. A node exceeding `policies.retry.maxAttempts` or `policies.timeout` without a defined failure branch fails the run; a workflow-level `onError` handler catches that instead of every node needing its own.

---

## 11. API Surface

| Method & path | Purpose |
|---|---|
| `POST /workflows/{id}/versions` | Save a new draft version. |
| `POST /workflows/{id}/versions/{v}/publish` | Freeze a version; assigns the permanent version number. |
| `POST /workflows/{id}/trigger` | Validate trigger payload, resolve connection bindings, create the run, publish `RunStarted`. |
| `GET /runs/{runId}` | Current run + node status. |
| `GET /runs/{runId}/events?nodeId=&eventType=&since=&cursor=` | Full ordered audit trail, filterable and paginated — backs the Audit Trail screen (§14.5) directly. |
| `POST /runs/{runId}/stop` | Sets `stop_requested_at`; Engine honors it before the next node starts. |
| `GET /approvals/pending?approverId=` | What a user or agent can act on right now. |
| `POST /approvals/{id}/decision` | Approve or reject; evaluates the node's gate. |
| `GET /node-kinds` | Registry, for the visual builder's node palette. |
| `POST /connections` | Register connection metadata (secret goes straight to Key Vault). |

---

## 12. Global Error Handling

A workflow may declare a top-level `onError` handler (`{ "to": "<nodeId>" }`). Any node that exhausts retries, times out, or otherwise fails **without** a defined failure branch in its own `next` routes to this handler instead of failing the run silently — see §13.4 for a concrete instance.

---

## 13. Reference Scenario: Azure Databricks Provisioning

A user requests an Azure Databricks workspace, enters the required data, the request goes to parallel approval from two groups, once approved the engine builds a deployment JSON, commits it to a specific branch/path in a GitHub repo, that commit triggers a GitHub Actions deployment workflow, the engine watches that run to completion, and finally emails the requester.

### 13.1 Node graph

```mermaid
flowchart TD
  T(["Trigger: request form"]) --> AP["approve-platform"]
  T --> AS["approve-security"]
  AP --> SW{"bothApproved"}
  AS --> SW
  SW -->|approved| BJ["buildJson"]
  SW -->|rejected| NR["notifyRejected"]
  BJ --> C["commit"]
  C --> AW["awaitWorkflowRun"]
  AW -->|"conclusion = success"| N["notify"]
  AW -->|"conclusion != success"| NF["notifyDeployFailed"]
```

This is exactly what the React Flow canvas renders — no translation layer between the published JSON and the visualization.

### 13.2 Why the GitHub Actions check is a custom kind

"Commit to GitHub, that triggers a workflow" hides a real gap: a push-triggered Actions run doesn't hand back a run id, and `workflow_dispatch` has the same gap from the other side — that call doesn't return one either. The fix is the commit's own SHA: GitHub's list-runs endpoint accepts `head_sha` as a filter, so once `commit` returns the SHA it just wrote, finding the run it caused is unambiguous no matter what else is happening in the repo.

That's two different GitHub calls, hit in sequence, with different intervals and different timeouts — expressing that as generic `checkExpression` strings on `control.pollUntil` would work but be unreadable and fragile. So it isn't `control.pollUntil`: it's its own kind, `action.github.awaitWorkflowRun` (§6.2), with the two-phase logic written into its executor rather than configured. It is still exactly **one node** — the phases live inside its own `state`, never as a second node in the graph (§6.3 has the full manifest and tick-by-tick trace).

### 13.3 Component-level walkthrough

The workflow JSON is the graph; it says nothing about which system does what. Three diagrams, one per phase, name every component that actually touches this scenario.

**Phase A — request, form, and approval**

```mermaid
sequenceDiagram
  participant User as Requester
  participant UI as React UI
  participant API as FastAPI
  participant DB as Postgres
  participant SB as Service Bus
  participant Engine
  participant PL as Platform-leads approver
  participant SR as Security-review approvers

  User->>UI: Open "Provision Databricks" form
  UI->>API: GET /workflows/wf_databricks_provision
  API->>DB: read published workflow_version
  API-->>UI: definition - payloadSchema drives the form (label, description, required, default)
  User->>UI: Fill form, submit
  UI->>API: POST /workflows/.../trigger { resourceName, region, requesterEmail }
  API->>DB: validate payload, resolve connection_bindings for this environment
  API->>DB: insert workflow_run (Created), snapshot bindings
  API->>SB: publish RunStarted(runId)
  API-->>UI: 201 { runId }
  SB->>Engine: deliver RunStarted
  Engine->>DB: insert approve-platform, approve-security (Running)
  Engine->>DB: raise Requested on both
  Engine->>DB: workflow_run.status = Running
  Note over Engine,DB: both approvals now wait on an external actor - nothing self-schedules
  PL->>UI: Open approval inbox, approve
  UI->>API: POST /approvals/id/decision
  API->>DB: DecisionRecorded, quorum(1) reached -> QuorumReached
  API->>SB: publish NodeEvent(approve-platform, QuorumReached)
  SB->>Engine: deliver
  Engine->>DB: approve-platform.status = Succeeded
  SR->>UI: Open approval inbox, approve (quorum 2)
  UI->>API: POST /approvals/id/decision, twice
  API->>DB: DecisionRecorded, quorum(2) reached -> QuorumReached
  API->>SB: publish NodeEvent(approve-security, QuorumReached)
  SB->>Engine: deliver
  Engine->>DB: approve-security.status = Succeeded
  Engine->>DB: bothApproved join="all" satisfied -> buildJson eligible
  Engine->>SB: publish NodeReady(buildJson)
```

**Phase B — build, commit, and watch the deployment run**

```mermaid
sequenceDiagram
  participant Engine
  participant DB as Postgres
  participant KV as Key Vault
  participant GH as GitHub REST API
  participant SB as Service Bus

  SB->>Engine: deliver NodeReady(buildJson)
  Engine->>DB: buildJson Running -> Succeeded (pure transform, no external call)
  Engine->>DB: commit Running
  Engine->>KV: resolve "github-deploy" connection
  KV-->>Engine: token
  Engine->>GH: PUT /repos/.../contents/path
  GH-->>Engine: 201 { commit: { sha: "abc123" } }
  Engine->>DB: commit Succeeded, output.commitSha = abc123
  Engine->>DB: awaitWorkflowRun Running, state = { phase: "locating" }
  loop every 10s, up to 5 min
    Engine->>GH: GET /actions/runs?head_sha=abc123
    GH-->>Engine: runs: []
    Engine->>DB: append LocateProgressed
    Engine->>SB: schedule recheck
  end
  Engine->>GH: GET /actions/runs?head_sha=abc123
  GH-->>Engine: runs: [{ id: 555 }]
  Engine->>DB: append RunLocated, state.phase = "watching"
  loop every 30s, up to 30 min
    Engine->>GH: GET /actions/runs/555
    GH-->>Engine: { status: "in_progress" }
    Engine->>DB: append RunProgressed
    Engine->>SB: schedule recheck
  end
  Engine->>GH: GET /actions/runs/555
  GH-->>Engine: { status: "completed", conclusion: "success" }
  Engine->>DB: append RunSucceeded, status = Succeeded, output = { ghRunId, conclusion, htmlUrl }
  Engine->>DB: notify eligible
```

**Phase C — live status in the UI, and notification**

```mermaid
sequenceDiagram
  participant User as Requester
  participant UI as React UI
  participant API as FastAPI
  participant DB as Postgres
  participant Engine
  participant SMTP as Email connector

  loop while awaitWorkflowRun is Running
    User->>UI: run status page (auto-refreshing)
    UI->>API: GET /runs/runId
    API->>DB: read node_execution.state + event log
    API-->>UI: { status: "Running", node: "awaitWorkflowRun", htmlUrl, lastStatus: "in_progress" }
    UI-->>User: status chip + "View on GitHub" link
  end
  Note over Engine,DB: awaitWorkflowRun reaches Succeeded (Phase B)
  Engine->>DB: notify Running
  Engine->>SMTP: send email (to: requesterEmail, subject includes runId)
  SMTP-->>Engine: sent
  Engine->>DB: notify Succeeded
  Engine->>DB: workflow_run.status = Completed - nothing left in next
  User->>UI: refresh
  UI->>API: GET /runs/runId
  API-->>UI: { status: "Completed" }
```

### 13.4 Error handling in this scenario

Two failure paths are modeled explicitly in the graph because they're expected business outcomes, not infrastructure faults: rejection (`notifyRejected`) and a deployment that ran but concluded unsuccessfully (`notifyDeployFailed`). Everything else — a commit that exhausts its retries, a locate phase that never finds a run inside 5 minutes — routes through the workflow-level `onError` handler (§12) to `notifyOpsFailure`, a platform-eng alert distinct from either requester-facing email.

---

## 14. UI Screens

The UI has two audiences — people building and publishing workflows, and people running, monitoring, and approving them. None of the screens below are a separate design exercise: each is a direct read or write against the schema and API surface already defined in §5–§11, so a new node kind or event type needs no UI change beyond what the generic mechanisms already handle.

### 14.1 Workflow Designer / Builder

- Canvas (React Flow) rendering `nodes` and each node's own `next` list (§5.4) directly — node position from `layout` when present, auto-laid-out when absent.
- Node palette sourced live from `GET /node-kinds`, grouped by the categories in §6.1.
- **Node inspector**: selecting a node renders a form generated from that kind's input/config schema, built from the §5.1 field shape (`type`/`label`/`description`/`required`/`default`) — a new node kind becomes editable with zero UI code.
- **Connections panel**: for a node's declared roles (§5.5), a dropdown per role picks a workflow-level alias; a separate workflow-level "Connections" tab manages the alias list and its per-environment bindings (§9).
- **Variables panel**: global variables (read-only list, with a request-access action) and run-scoped variables (add/edit), each scope-tagged per §5.2.
- **Validation surface**: publish-time errors — a dangling `next` target, a write to a `ro` variable, a connection type mismatch, an unreachable node — shown inline on the offending node; Publish stays disabled until they clear.
- Draft/Published version selector (§7.1 pinning is what makes switching versions here safe); raw JSON import/export as an escape hatch, kept in sync with the canvas.

### 14.2 Trigger / Run Form

Generated purely from `trigger.payloadSchema` (§5.1, §5.6) — the same rendering component as the node inspector in §14.1, not a second implementation. Submitting calls `POST /workflows/{id}/trigger` and opens the Run Execution view (§14.3) for the returned `runId`.

### 14.3 Run Execution / Status View

- The same graph component as the Designer, read-only, each node color-coded by its live `status` (§7.2).
- Selecting a node opens a detail drawer showing `input_snapshot`, and once terminal, `output`. A node still `Running`/waiting shows a kind-specific summary rendered from its `state` — e.g. `awaitWorkflowRun` shows its current phase and a "View on GitHub" link straight from `state`, matching §13.3 Phase C.
- Run-level header: overall `workflow_run.status`, started-by/at, and a Stop button (`POST /runs/{runId}/stop`) disabled once the run is terminal.

### 14.4 Approval Inbox

- Personal list from `GET /approvals/pending?approverId=`, grouped by group, each row showing the relevant trigger-payload context and quorum progress ("1 of 2 needed").
- Approve/Reject with an optional comment, posting to `POST /approvals/{id}/decision`.

### 14.5 Audit Trail

- A chronological, filterable table reading `GET /runs/{runId}/events` directly — every row is one `event` table row: sequence, timestamp, node id, event type, and an expandable payload viewer.
- Filters by node, event type, and time range are necessary in practice: a single `pollUntil`/`awaitWorkflowRun` node can log dozens of non-terminal rows (`Checked`, `RunProgressed`) over one run.
- This is the completeness guarantee from §4 made visible — every event, terminal or not, shows up here.
- Linked both ways: a node's detail drawer in §14.3 deep-links into its slice of the trail, and the trail is also available as a standalone tab per run.

### 14.6 Workflow Catalogue

List of workflows with their draft/published versions and recent runs — the landing page tying the other screens together.

---

## 15. Open Items / Follow-ups

- Confirm the exact JSON shape `buildJson` hands to the target repo's deployment pipeline (Terraform tfvars vs. Bicep parameters vs. custom manifest).
- Define the `github-deploy` connection type's required scopes (`contents:write`, `actions:read` at minimum).
- Confirm the recipient list/routing behind `notifyOpsFailure`.
- Platform-level global variable registry: how globals are registered, and who can grant a workflow read/write access to one.
- Node kind SDK / plugin packaging for third parties adding new connectors.
- Visual wireframes/mockups for the six screens in §14, if useful ahead of frontend implementation.

---

## Appendix A: Decision Log

| # | Decision | Rationale |
|---|---|---|
| 1 | Approval gating is configurable per node — AND/OR across groups, quorum within a group — later refined to be **composed structurally**: one `approval` node per group plus a downstream `control.switch` with a `join`/`when` policy, rather than a gate config baked into the approval kind. | Generalizes to any "did enough prior steps succeed" gate, not just approvals; keeps the approval kind itself trivial. |
| 2 | Expression language is JSONata everywhere (inputs, `when`, edge `metadata`, config). | One consistent, learnable syntax; enables static reference-validation at publish time. |
| 3 | Agentic loop is its own node kind — LLM chooses actions and the stop point, bounded by iteration/cost/timeout caps. | Distinct failure modes and guardrails from a deterministic loop; needs its own event vocabulary. |
| 4 | A run pins to the exact workflow version it started on; edits never affect in-flight runs. | Matches Temporal/Step Functions semantics; avoids a running instance hitting a graph shape it didn't start with. |
| 5 | Stop is graceful only, with an optional per-node compensation handler; no automatic rollback of committed side effects. | Side effects (e.g. a GitHub commit) generally can't be safely auto-reverted; explicit is safer than silent. |
| 6 | Connections are referenced by a logical alias, resolved to a concrete connection per environment at trigger time. | Same published JSON promotes across environments untouched. |
| 7 | Sub-workflow composition deferred to a later phase. | Keeps v1 scope and schema smaller; revisit once the core engine is proven. |
| 8 | Fan-in defaults to `join: "all"`, overridable to `any` or an explicit N-of-M. | Matches the common case; explicit override covers races and partial-completion patterns. |
| 9 | Control flow lives on the node as `next` (with `when` and `metadata`), not a separate top-level edge array. | One source of truth per node; matches the shape a graph-based visual builder wants natively. |
| 10 | `variables` is a dictionary keyed by scope then name, not an array; scopes are `trigger` / `global` / `run` / `iteration`. | Removes ambiguity about which scope a variable belongs to; array-of-objects made scope an easy-to-miss field. |
| 11 | A `$context` object (workflowId, runId, environment, node identity) is a fourth expression root alongside `$trigger`/`$vars`/`$node`. | Needed for traceability (tagging commits/emails with the run id) without polluting `$vars`. |
| 12 | Every schema-defining block uses one field shape: `{ type, label, description, required, default }`. | One convention for the visual builder to render forms from, wherever a schema appears. |
| 13 | `type`/`typeVersion` on a node instance renamed to `kind`/`version`. | Naming clarity — "type" was overloaded with data-type usage elsewhere in the schema. |
| 14 | A node's connection reference is a `connections` map of named roles, not a single `connection` string. | A node can require more than one connection (e.g. source + destination). |
| 15 | Retry and timeout live under a single `policies` object on the node. | Leaves room for future policies (concurrency, caching, circuit-breaker) without new top-level node keys. |
| 16 | The GitHub Actions status check is a single custom node kind (`action.github.awaitWorkflowRun`) with an internal two-phase state machine, not two graph nodes and not generic `control.pollUntil` config. | Two sequential GitHub API calls with different cadences/timeouts is API-specific logic that belongs in an executor, not in fragile config strings; the graph should show one status change for one logical wait. |
| 17 | `node_execution` has both `state` (opaque, executor-only, mutable while non-terminal) and `output` (set once, on the terminal event, the only one exposed downstream). | Makes explicit which data is a kind's private working scratchpad versus its public result — the root of "how terminal events are handled." |

---

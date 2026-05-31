//! The MCP server exposed over stdio to Claude Desktop / Claude Code. Tools
//! translate into broker commands routed to a session's plugin (§2, §3).
//!
//! Step 2 surfaces the session-management tools plus two round-trip probes
//! (`get_place_info`, `ping_session`) that exercise the full
//! adapter → broker → plugin → result loop and the disambiguation rule. Search,
//! playtest, and the rest land in later steps.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, Content, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::broker_client::{BrokerClient, resolve_session};
use crate::protocol::{LiveState, Role};

/// Default time the broker waits for a plugin to answer a command.
const COMMAND_TIMEOUT_MS: u64 = 30_000;

/// Schema for a field that accepts any JSON value (Roblox property / attribute
/// values: scalars, typed specs like `{type,value}`, arrays, or maps). schemars
/// renders a bare `serde_json::Value` with no `type`, which Claude Code's tool
/// validator rejects — one such property fails the entire `tools/list` fetch, so
/// every tool disappears. Emitting an explicit type union keeps it valid.
fn any_json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": ["string", "integer", "number", "boolean", "array", "object", "null"]
    })
}

/// Schema for a field that accepts a JSON object (a name->value map or nested spec).
/// Same rationale as [`any_json_schema`]: a typed schema, not a bare `Value`.
fn json_object_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "type": "object" })
}

#[derive(Clone)]
pub struct DustServer {
    broker: Arc<BrokerClient>,
    tool_router: ToolRouter<Self>,
}

/// Optional session selector shared by every place-targeting tool (§3).
#[derive(Debug, Deserialize, JsonSchema)]
struct SessionSelector {
    /// Session id or label to target. May be omitted only when exactly one
    /// Studio session is live; otherwise the call errors and asks you to run
    /// list_sessions first.
    #[serde(default)]
    session: Option<String>,
}

/// Arguments for `grep_scripts`.
#[derive(Debug, Deserialize, JsonSchema)]
struct GrepArgs {
    #[serde(default)]
    session: Option<String>,
    /// Pattern to find in script source. A Lua string pattern unless `plain` is set.
    pattern: String,
    /// Treat `pattern` as a literal substring instead of a Lua pattern.
    #[serde(default)]
    plain: Option<bool>,
    /// Case-insensitive matching.
    #[serde(default)]
    ignore_case: Option<bool>,
    /// Restrict to scripts of this class (e.g. "Script", "ModuleScript", "LocalScript").
    #[serde(default)]
    class_filter: Option<String>,
    /// Lines of surrounding context to include with each match.
    #[serde(default)]
    context_lines: Option<u32>,
    /// Maximum number of matches to return.
    #[serde(default)]
    limit: Option<u32>,
    /// Restrict the search to the subtree under this instance handle (default: whole game).
    #[serde(default)]
    root: Option<String>,
}

/// Arguments for `search_instances`.
#[derive(Debug, Deserialize, JsonSchema)]
struct SearchInstancesArgs {
    #[serde(default)]
    session: Option<String>,
    /// Only match instances of this class (uses IsA, so subclasses match).
    #[serde(default)]
    class_name: Option<String>,
    /// Lua pattern matched against each instance's Name.
    #[serde(default)]
    name_pattern: Option<String>,
    /// Only match instances carrying this CollectionService tag.
    #[serde(default)]
    tag: Option<String>,
    /// Only match instances whose `property` equals `value`.
    #[serde(default)]
    property: Option<String>,
    /// Expected value for `property` (string / number / bool).
    #[serde(default)]
    #[schemars(schema_with = "any_json_schema")]
    value: Option<Value>,
    /// Maximum depth below the root to descend.
    #[serde(default)]
    max_depth: Option<u32>,
    /// Results per page.
    #[serde(default)]
    limit: Option<u32>,
    /// Restrict to the subtree under this instance handle (default: whole game).
    #[serde(default)]
    root: Option<String>,
    /// Opaque cursor from a previous call to fetch the next page of results.
    #[serde(default)]
    cursor: Option<String>,
}

/// Arguments for `search_by_property`.
#[derive(Debug, Deserialize, JsonSchema)]
struct SearchByPropertyArgs {
    #[serde(default)]
    session: Option<String>,
    /// Property name to read on each instance (e.g. "Anchored", "Transparency").
    property: String,
    /// Value to match. Omit to match any instance where the property is readable.
    #[serde(default)]
    #[schemars(schema_with = "any_json_schema")]
    value: Option<Value>,
    /// Only consider instances of this class (uses IsA).
    #[serde(default)]
    class_name: Option<String>,
    /// Maximum number of matches to return.
    #[serde(default)]
    limit: Option<u32>,
    /// Restrict to the subtree under this instance handle (default: whole game).
    #[serde(default)]
    root: Option<String>,
}

/// Arguments for `get_script_source`.
#[derive(Debug, Deserialize, JsonSchema)]
struct GetScriptSourceArgs {
    #[serde(default)]
    session: Option<String>,
    /// Handle of a script instance (from a grep/search result).
    handle: String,
    /// Optional 1-based first line to return.
    #[serde(default)]
    start_line: Option<u32>,
    /// Optional 1-based last line to return.
    #[serde(default)]
    end_line: Option<u32>,
}

/// Arguments for `read_output`.
#[derive(Debug, Deserialize, JsonSchema)]
struct ReadOutputArgs {
    #[serde(default)]
    session: Option<String>,
    /// Only return lines of this level: "output", "info", "warning", or "error".
    #[serde(default)]
    level: Option<String>,
    /// Maximum number of (most recent) lines to return.
    #[serde(default)]
    limit: Option<u32>,
    /// Clear the plugin's buffer after returning these lines.
    #[serde(default)]
    clear: Option<bool>,
}

/// Arguments for `run_luau`.
#[derive(Debug, Deserialize, JsonSchema)]
struct RunLuauArgs {
    #[serde(default)]
    session: Option<String>,
    /// Luau source to execute in the edit / F8 DataModel. Returns stringified
    /// return values. Dev-only: arbitrary code execution.
    code: String,
}

/// Arguments for `run_server_code`.
#[derive(Debug, Deserialize, JsonSchema)]
struct RunServerCodeArgs {
    #[serde(default)]
    session: Option<String>,
    /// Luau source to execute in the running F5 server DataModel (via the server
    /// helper). Returns stringified return values. Dev-only.
    code: String,
}

/// Arguments for `read_server_output`.
#[derive(Debug, Deserialize, JsonSchema)]
struct ReadServerOutputArgs {
    #[serde(default)]
    session: Option<String>,
    /// Maximum number of (most recent) server log lines to return.
    #[serde(default)]
    limit: Option<u32>,
}

/// Arguments for `read_client_output`.
#[derive(Debug, Deserialize, JsonSchema)]
struct ReadClientOutputArgs {
    #[serde(default)]
    session: Option<String>,
    /// Maximum number of (most recent) client log lines to return.
    #[serde(default)]
    limit: Option<u32>,
}

/// One simulated key event for `keyboard_input`.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct KeyEvent {
    /// KeyCode name, e.g. "W", "Space", "LeftShift" (resolved via `Enum.KeyCode`).
    key: String,
    /// "down", "up", or "tap" (press + release). Defaults to "tap".
    #[serde(default)]
    action: Option<String>,
    /// For "tap": seconds to hold before releasing. Defaults to a brief tap.
    #[serde(default)]
    duration: Option<f64>,
}

/// Arguments for `keyboard_input`.
#[derive(Debug, Deserialize, JsonSchema)]
struct KeyboardInputArgs {
    #[serde(default)]
    session: Option<String>,
    /// Ordered key events to simulate in the play client (via VirtualUser).
    #[serde(default)]
    keys: Vec<KeyEvent>,
    /// Optional text to type character-by-character after the key events.
    #[serde(default)]
    text: Option<String>,
}

/// Arguments for `mouse_input`.
#[derive(Debug, Deserialize, JsonSchema)]
struct MouseInputArgs {
    #[serde(default)]
    session: Option<String>,
    /// "move", "click", "down", or "up".
    action: String,
    /// Target X in viewport pixels.
    #[serde(default)]
    x: Option<f64>,
    /// Target Y in viewport pixels.
    #[serde(default)]
    y: Option<f64>,
    /// "left" or "right" (defaults to "left").
    #[serde(default)]
    button: Option<String>,
}

/// Arguments for `character_navigation`.
#[derive(Debug, Deserialize, JsonSchema)]
struct CharacterNavigationArgs {
    #[serde(default)]
    session: Option<String>,
    /// "move" (a direction for a duration), "move_to" (walk to x,y,z), "jump", or "stop".
    action: String,
    /// For "move": "forward" / "back" / "left" / "right"; omit to use a raw x,z direction.
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    x: Option<f64>,
    #[serde(default)]
    y: Option<f64>,
    #[serde(default)]
    z: Option<f64>,
    /// For "move": seconds to hold the movement before stopping.
    #[serde(default)]
    duration: Option<f64>,
    /// For "move" with a named direction: interpret it relative to the camera.
    #[serde(default)]
    relative_to_camera: Option<bool>,
}

// ---- Editor-suite argument types (instances addressed by `path` dot-notation
// or a DebugId `handle` from a search result). ----

#[derive(Debug, Deserialize, JsonSchema)]
struct PathArgs {
    #[serde(default)]
    session: Option<String>,
    /// Dot-path, e.g. "game.Workspace.Part".
    #[serde(default)]
    path: Option<String>,
    /// DebugId handle from a search/lookup result.
    #[serde(default)]
    handle: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PathLimitArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct GetPropsArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    /// Specific property names to read; omit for a common curated set.
    #[serde(default)]
    properties: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ProjectArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    #[serde(default)]
    max_depth: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct TagArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    tag: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct GetTaggedArgs {
    #[serde(default)]
    session: Option<String>,
    tag: String,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AttrNameArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SetAttrArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    name: String,
    /// Attribute value (string / number / bool).
    #[schemars(schema_with = "any_json_schema")]
    value: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct BulkAttrArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    /// Map of attribute name -> value.
    #[schemars(schema_with = "json_object_schema")]
    attributes: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CreateArgs {
    #[serde(default)]
    session: Option<String>,
    class_name: String,
    /// Parent as a dot-path or handle.
    parent: String,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CloneArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    /// Destination parent (path or handle); defaults to the source's parent.
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RenameArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MoveArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    /// New parent (path or handle).
    parent: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SetPropArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    property: String,
    /// Primitive, a typed spec {type,value} (e.g. {"type":"Vector3","value":[1,2,3]},
    /// {"type":"Color3uint8","value":[255,0,0]}, {"type":"Enum","value":"Material.Wood"}),
    /// or a bare string for an enum property.
    #[schemars(schema_with = "any_json_schema")]
    value: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SetPropsArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    /// Map of property name -> value (same value forms as set_property).
    #[schemars(schema_with = "json_object_schema")]
    properties: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MassSetArgs {
    #[serde(default)]
    session: Option<String>,
    property: String,
    #[schemars(schema_with = "any_json_schema")]
    value: Value,
    /// Instances to set, each a dot-path or handle.
    targets: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MassGetArgs {
    #[serde(default)]
    session: Option<String>,
    property: String,
    /// Instances to read, each a dot-path or handle.
    targets: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SetSourceArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    source: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct EditLinesArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    start_line: u32,
    #[serde(default)]
    end_line: Option<u32>,
    new_text: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct InsertLinesArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    /// Insert after this line (0 = top of file).
    after_line: u32,
    text: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct DeleteLinesArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    start_line: u32,
    #[serde(default)]
    end_line: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct FindReplaceArgs {
    #[serde(default)]
    session: Option<String>,
    pattern: String,
    replacement: String,
    /// Treat pattern/replacement as Lua patterns instead of literal text.
    #[serde(default)]
    use_pattern: Option<bool>,
    /// Restrict to scripts of this class (Script/LocalScript/ModuleScript).
    #[serde(default)]
    class_filter: Option<String>,
    /// Limit scope to this subtree (path or handle); default whole game.
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchObjArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    /// Lua pattern matched against instance names.
    #[serde(default)]
    name_pattern: Option<String>,
    #[serde(default)]
    class_name: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchFilesArgs {
    #[serde(default)]
    session: Option<String>,
    /// Substring matched against script names and full paths.
    query: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CompareArgs {
    #[serde(default)]
    session: Option<String>,
    /// First instance (path or handle).
    a: String,
    /// Second instance (path or handle).
    b: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MassDupArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    count: u32,
    #[serde(default)]
    parent: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SmartDupArgs {
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    count: u32,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    name_prefix: Option<String>,
    /// Per-copy positional offset, e.g. {"type":"Vector3","value":[5,0,0]}.
    #[serde(default)]
    #[schemars(schema_with = "json_object_schema")]
    offset: Option<Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UiTreeArgs {
    #[serde(default)]
    session: Option<String>,
    /// Nested spec: {className, name?, properties?{}, children?[]}.
    #[schemars(schema_with = "json_object_schema")]
    tree: Value,
    /// Parent for the tree root (path or handle).
    #[serde(default)]
    parent: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ClassInfoArgs {
    #[serde(default)]
    session: Option<String>,
    class_name: String,
}

/// Build the shared `{path?, handle?}` target map most editor tools forward.
fn target(path: &Option<String>, handle: &Option<String>) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    if let Some(p) = path {
        m.insert("path".into(), json!(p));
    }
    if let Some(h) = handle {
        m.insert("handle".into(), json!(h));
    }
    m
}

#[tool_router]
impl DustServer {
    pub fn new(broker: Arc<BrokerClient>) -> Self {
        Self { broker, tool_router: Self::tool_router() }
    }

    #[tool(
        description = "List every Roblox Studio session connected to the broker, with place info \
                       (placeId, gameId, name) and per-role liveness (live/stale/dead). Use this to \
                       discover sessions and to pick a `session` when more than one place is open."
    )]
    async fn list_sessions(&self) -> Result<CallToolResult, ErrorData> {
        match self.broker.list_sessions().await {
            Ok(sessions) => ok_json(&sessions),
            Err(e) => Ok(tool_error(format!("failed to list sessions: {e}"))),
        }
    }

    #[tool(
        description = "Read place/game info (placeId, gameId, name, creatorId) from a Studio \
                       session's edit context. Pass `session` when multiple places are live."
    )]
    async fn get_place_info(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_place_info", json!({}), args.session.as_deref()).await
    }

    #[tool(
        description = "Ping a Studio session's plugin to verify the end-to-end command loop is \
                       working. Returns the plugin's echo. Pass `session` when multiple are live."
    )]
    async fn ping_session(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("ping", json!({ "hello": "dust" }), args.session.as_deref()).await
    }

    #[tool(
        description = "Search Luau script source across a Studio session. Walks LuaSourceContainer \
                       descendants and returns matches as {path, handle, class, line, col, text}. \
                       `pattern` is a Lua string pattern unless `plain` is set. Each result's \
                       `handle` can be passed to get_script_source or future edit tools."
    )]
    async fn grep_scripts(
        &self,
        Parameters(a): Parameters<GrepArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut args = json!({
            "pattern": a.pattern,
            "plain": a.plain.unwrap_or(false),
            "ignoreCase": a.ignore_case.unwrap_or(false),
            "limit": a.limit.unwrap_or(200),
            "contextLines": a.context_lines.unwrap_or(0),
        });
        if let Some(c) = a.class_filter {
            args["classFilter"] = json!(c);
        }
        if let Some(r) = a.root {
            args["root"] = json!(r);
        }
        self.run_plugin_tool("grep", args, a.session.as_deref()).await
    }

    #[tool(
        description = "Search the instance tree by class / name pattern / CollectionService tag / \
                       property, bounded by depth. Returns {path, handle, class, name} pages with an \
                       opaque `cursor`; pass it back to fetch the next page."
    )]
    async fn search_instances(
        &self,
        Parameters(a): Parameters<SearchInstancesArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut args = json!({ "limit": a.limit.unwrap_or(100) });
        if let Some(v) = a.class_name {
            args["className"] = json!(v);
        }
        if let Some(v) = a.name_pattern {
            args["namePattern"] = json!(v);
        }
        if let Some(v) = a.tag {
            args["tag"] = json!(v);
        }
        if let Some(v) = a.property {
            args["property"] = json!(v);
        }
        if let Some(v) = a.value {
            args["value"] = v;
        }
        if let Some(v) = a.max_depth {
            args["maxDepth"] = json!(v);
        }
        if let Some(v) = a.root {
            args["root"] = json!(v);
        }
        if let Some(v) = a.cursor {
            args["cursor"] = json!(v);
        }
        self.run_plugin_tool("search_instances", args, a.session.as_deref()).await
    }

    #[tool(
        description = "Find instances whose property equals a value (cheap; handy for refactors). \
                       Returns {path, handle, class, name, value}."
    )]
    async fn search_by_property(
        &self,
        Parameters(a): Parameters<SearchByPropertyArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut args = json!({ "property": a.property, "limit": a.limit.unwrap_or(200) });
        if let Some(v) = a.value {
            args["value"] = v;
        }
        if let Some(v) = a.class_name {
            args["className"] = json!(v);
        }
        if let Some(v) = a.root {
            args["root"] = json!(v);
        }
        self.run_plugin_tool("search_by_property", args, a.session.as_deref()).await
    }

    #[tool(
        description = "Read a script's source by handle (from a grep/search result), optionally a \
                       line range. Returns {path, class, lineCount, source}."
    )]
    async fn get_script_source(
        &self,
        Parameters(a): Parameters<GetScriptSourceArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut args = json!({ "handle": a.handle });
        if let Some(v) = a.start_line {
            args["startLine"] = json!(v);
        }
        if let Some(v) = a.end_line {
            args["endLine"] = json!(v);
        }
        self.run_plugin_tool("get_script_source", args, a.session.as_deref()).await
    }

    #[tool(
        description = "Start an F8-style simulation (Run): physics in the edit DataModel with no \
                       player. Idempotent. Use stop_simulation to end it."
    )]
    async fn start_simulation(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("start_simulation", json!({}), args.session.as_deref()).await
    }

    #[tool(description = "Stop a running F8-style simulation. Idempotent.")]
    async fn stop_simulation(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("stop_simulation", json!({}), args.session.as_deref()).await
    }

    #[tool(
        description = "Read recent Studio output (Output window log lines), optionally filtered by \
                       level (output/info/warning/error). Captures edit and F8 output."
    )]
    async fn read_output(
        &self,
        Parameters(a): Parameters<ReadOutputArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut args = json!({ "limit": a.limit.unwrap_or(200), "clear": a.clear.unwrap_or(false) });
        if let Some(v) = a.level {
            args["level"] = json!(v);
        }
        self.run_plugin_tool("read_output", args, a.session.as_deref()).await
    }

    #[tool(
        description = "Execute Luau in the session's edit / F8 DataModel and return stringified \
                       results. Dev-only (arbitrary code execution)."
    )]
    async fn run_luau(
        &self,
        Parameters(a): Parameters<RunLuauArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("run_luau", json!({ "code": a.code }), a.session.as_deref()).await
    }

    #[tool(
        description = "Prepare an F5 playtest: inject the server/client helpers into the edit \
                       DataModel (idempotent), then the user presses F5 — the helpers connect \
                       automatically. Requires 'Allow HTTP Requests' enabled. Use stop_playtest to \
                       end and clean up."
    )]
    async fn start_playtest(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("start_playtest", json!({}), args.session.as_deref()).await
    }

    #[tool(
        description = "End an F5 playtest (via the server helper's EndTest) and strip the injected \
                       helpers from the edit DataModel. Reports clearly if no server helper is live."
    )]
    async fn stop_playtest(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.stop_playtest_impl(args.session.as_deref()).await
    }

    #[tool(
        description = "Run Luau in the live F5 server DataModel (via the server helper) and return \
                       stringified results. Requires an active playtest. Dev-only."
    )]
    async fn run_server_code(
        &self,
        Parameters(a): Parameters<RunServerCodeArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_tool("run_server_code", json!({ "code": a.code }), Role::Server, COMMAND_TIMEOUT_MS, a.session.as_deref())
            .await
    }

    #[tool(
        description = "Read live server state during an F5 playtest (player count, names, game \
                       time) via the server helper."
    )]
    async fn get_server_state(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_tool("get_server_state", json!({}), Role::Server, COMMAND_TIMEOUT_MS, args.session.as_deref())
            .await
    }

    #[tool(
        description = "Read recent server-side Output during an F5 playtest via the server helper \
                       (server logs may differ from the edit-context output)."
    )]
    async fn read_server_output(
        &self,
        Parameters(a): Parameters<ReadServerOutputArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_tool("read_server_output", json!({ "limit": a.limit.unwrap_or(200) }), Role::Server, COMMAND_TIMEOUT_MS, a.session.as_deref())
            .await
    }

    // ---- Client playtest suite (role=client, proxied to the play client via the relay) ----

    #[tool(
        description = "Read recent client-side Output during a playtest (the local player's \
                       LogService; differs from server output). Routed to the play client via the relay."
    )]
    async fn read_client_output(
        &self,
        Parameters(a): Parameters<ReadClientOutputArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_tool("read_client_output", json!({ "limit": a.limit.unwrap_or(200) }), Role::Client, COMMAND_TIMEOUT_MS, a.session.as_deref())
            .await
    }

    #[tool(
        description = "Read live client state during a playtest: local player, character/Humanoid \
                       (health, position, state), camera, mouse, viewport. Via the client relay."
    )]
    async fn get_client_state(
        &self,
        Parameters(args): Parameters<SessionSelector>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_tool("get_client_state", json!({}), Role::Client, COMMAND_TIMEOUT_MS, args.session.as_deref())
            .await
    }

    #[tool(
        description = "Navigate the local player's character during a playtest: move in a direction \
                       for a duration, walk to a point (move_to), jump, or stop. Drives the client Humanoid."
    )]
    async fn character_navigation(
        &self,
        Parameters(a): Parameters<CharacterNavigationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = json!({
            "action": a.action,
            "direction": a.direction,
            "x": a.x,
            "y": a.y,
            "z": a.z,
            "duration": a.duration,
            "relativeToCamera": a.relative_to_camera,
        });
        self.run_tool("character_navigation", args, Role::Client, COMMAND_TIMEOUT_MS, a.session.as_deref())
            .await
    }

    #[tool(
        description = "Simulate keyboard input in the play client (via VirtualUser): a sequence of \
                       key down/up/tap events and/or typed text. Requires an active playtest."
    )]
    async fn keyboard_input(
        &self,
        Parameters(a): Parameters<KeyboardInputArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = json!({ "keys": a.keys, "text": a.text });
        self.run_tool("keyboard_input", args, Role::Client, COMMAND_TIMEOUT_MS, a.session.as_deref())
            .await
    }

    #[tool(
        description = "Simulate mouse input in the play client (via VirtualUser): move / click / \
                       down / up at viewport pixel coordinates. Requires an active playtest."
    )]
    async fn mouse_input(
        &self,
        Parameters(a): Parameters<MouseInputArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = json!({
            "action": a.action,
            "x": a.x,
            "y": a.y,
            "button": a.button,
        });
        self.run_tool("mouse_input", args, Role::Client, COMMAND_TIMEOUT_MS, a.session.as_deref())
            .await
    }

    // ---- Editor suite: instance introspection ----

    #[tool(description = "Get an instance's direct children (by path or handle). Each child as {name, class, path, handle}.")]
    async fn get_instance_children(&self, Parameters(a): Parameters<PathArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_instance_children", Value::Object(target(&a.path, &a.handle)), a.session.as_deref()).await
    }

    #[tool(description = "Get all descendants of an instance (path or handle), bounded by `limit`.")]
    async fn get_descendants(&self, Parameters(a): Parameters<PathLimitArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        if let Some(l) = a.limit {
            m.insert("limit".into(), json!(l));
        }
        self.run_plugin_tool("get_descendants", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "List the top-level services in the DataModel.")]
    async fn get_services(&self, Parameters(a): Parameters<SessionSelector>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_services", json!({}), a.session.as_deref()).await
    }

    #[tool(description = "Get the instances currently selected in the Studio Explorer.")]
    async fn get_selection(&self, Parameters(a): Parameters<SessionSelector>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_selection", json!({}), a.session.as_deref()).await
    }

    #[tool(description = "Read a curated set of an instance's properties (or pass `properties` for specific names). Pure-Luau has no full reflection; use run_luau for exhaustive reads.")]
    async fn get_instance_properties(&self, Parameters(a): Parameters<GetPropsArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        if let Some(p) = a.properties {
            m.insert("properties".into(), json!(p));
        }
        self.run_plugin_tool("get_instance_properties", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Depth-bounded instance tree (name/class/children) under a root (default game).")]
    async fn get_project_structure(&self, Parameters(a): Parameters<ProjectArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        if let Some(d) = a.max_depth {
            m.insert("maxDepth".into(), json!(d));
        }
        self.run_plugin_tool("get_project_structure", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Flat listing of every script (LuaSourceContainer) under a root (default game).")]
    async fn get_file_tree(&self, Parameters(a): Parameters<PathArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_file_tree", Value::Object(target(&a.path, &a.handle)), a.session.as_deref()).await
    }

    #[tool(description = "Best-effort class info: creatability + IsA over common base classes.")]
    async fn get_class_info(&self, Parameters(a): Parameters<ClassInfoArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_class_info", json!({ "className": a.class_name }), a.session.as_deref()).await
    }

    #[tool(description = "Get the BaseParts rigidly connected (joints/welds) to a part.")]
    async fn get_connected_instances(&self, Parameters(a): Parameters<PathArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_connected_instances", Value::Object(target(&a.path, &a.handle)), a.session.as_deref()).await
    }

    #[tool(description = "Diff two instances `a` and `b` (paths or handles) across a common property set.")]
    async fn compare_instances(&self, Parameters(a): Parameters<CompareArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("compare_instances", json!({ "a": a.a, "b": a.b }), a.session.as_deref()).await
    }

    // ---- Editor suite: search ----

    #[tool(description = "Find instances by name pattern and/or class under a root. Returns {name, class, path, handle} pages.")]
    async fn search_objects(&self, Parameters(a): Parameters<SearchObjArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        if let Some(v) = a.name_pattern {
            m.insert("namePattern".into(), json!(v));
        }
        if let Some(v) = a.class_name {
            m.insert("className".into(), json!(v));
        }
        if let Some(v) = a.limit {
            m.insert("limit".into(), json!(v));
        }
        self.run_plugin_tool("search_objects", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Find scripts whose name or full path contains `query`.")]
    async fn search_files(&self, Parameters(a): Parameters<SearchFilesArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("query".into(), json!(a.query));
        if let Some(v) = a.limit {
            m.insert("limit".into(), json!(v));
        }
        self.run_plugin_tool("search_files", Value::Object(m), a.session.as_deref()).await
    }

    // ---- Editor suite: tags ----

    #[tool(description = "Get an instance's CollectionService tags.")]
    async fn get_tags(&self, Parameters(a): Parameters<PathArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_tags", Value::Object(target(&a.path, &a.handle)), a.session.as_deref()).await
    }

    #[tool(description = "Add a CollectionService tag to an instance.")]
    async fn add_tag(&self, Parameters(a): Parameters<TagArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("tag".into(), json!(a.tag));
        self.run_plugin_tool("add_tag", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Remove a CollectionService tag from an instance.")]
    async fn remove_tag(&self, Parameters(a): Parameters<TagArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("tag".into(), json!(a.tag));
        self.run_plugin_tool("remove_tag", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Get every instance carrying a CollectionService tag.")]
    async fn get_tagged(&self, Parameters(a): Parameters<GetTaggedArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = serde_json::Map::new();
        m.insert("tag".into(), json!(a.tag));
        if let Some(l) = a.limit {
            m.insert("limit".into(), json!(l));
        }
        self.run_plugin_tool("get_tagged", Value::Object(m), a.session.as_deref()).await
    }

    // ---- Editor suite: attributes ----

    #[tool(description = "Read one attribute of an instance.")]
    async fn get_attribute(&self, Parameters(a): Parameters<AttrNameArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("name".into(), json!(a.name));
        self.run_plugin_tool("get_attribute", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Get all attributes of an instance.")]
    async fn get_attributes(&self, Parameters(a): Parameters<PathArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_attributes", Value::Object(target(&a.path, &a.handle)), a.session.as_deref()).await
    }

    #[tool(description = "Set one attribute (value: string / number / bool).")]
    async fn set_attribute(&self, Parameters(a): Parameters<SetAttrArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("name".into(), json!(a.name));
        m.insert("value".into(), a.value);
        self.run_plugin_tool("set_attribute", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Delete one attribute from an instance.")]
    async fn delete_attribute(&self, Parameters(a): Parameters<AttrNameArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("name".into(), json!(a.name));
        self.run_plugin_tool("delete_attribute", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Set several attributes at once (`attributes` is a name->value map).")]
    async fn bulk_set_attributes(&self, Parameters(a): Parameters<BulkAttrArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("attributes".into(), a.attributes);
        self.run_plugin_tool("bulk_set_attributes", Value::Object(m), a.session.as_deref()).await
    }

    // ---- Editor suite: properties ----

    #[tool(description = "Set one property. `value` may be a primitive, a typed {type,value} spec, or a bare string for an enum.")]
    async fn set_property(&self, Parameters(a): Parameters<SetPropArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("property".into(), json!(a.property));
        m.insert("value".into(), a.value);
        self.run_plugin_tool("set_property", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Set several properties on one instance (`properties` is a name->value map).")]
    async fn set_properties(&self, Parameters(a): Parameters<SetPropsArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("properties".into(), a.properties);
        self.run_plugin_tool("set_properties", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Set the same property on many instances (`targets`: paths or handles).")]
    async fn mass_set_property(&self, Parameters(a): Parameters<MassSetArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("mass_set_property", json!({ "property": a.property, "value": a.value, "targets": a.targets }), a.session.as_deref()).await
    }

    #[tool(description = "Read one property from many instances (`targets`: paths or handles).")]
    async fn mass_get_property(&self, Parameters(a): Parameters<MassGetArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("mass_get_property", json!({ "property": a.property, "targets": a.targets }), a.session.as_deref()).await
    }

    // ---- Editor suite: instance lifecycle (undoable) ----

    #[tool(description = "Create an instance of `class_name` under `parent` (path or handle), optionally named.")]
    async fn create_object(&self, Parameters(a): Parameters<CreateArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = serde_json::Map::new();
        m.insert("className".into(), json!(a.class_name));
        m.insert("parent".into(), json!(a.parent));
        if let Some(n) = a.name {
            m.insert("name".into(), json!(n));
        }
        self.run_plugin_tool("create_object", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Delete (Destroy) an instance.")]
    async fn delete_object(&self, Parameters(a): Parameters<PathArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("delete_object", Value::Object(target(&a.path, &a.handle)), a.session.as_deref()).await
    }

    #[tool(description = "Clone an instance into `parent` (default same parent), optionally renamed.")]
    async fn clone_object(&self, Parameters(a): Parameters<CloneArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        if let Some(p) = a.parent {
            m.insert("parent".into(), json!(p));
        }
        if let Some(n) = a.name {
            m.insert("name".into(), json!(n));
        }
        self.run_plugin_tool("clone_object", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Rename an instance.")]
    async fn rename_object(&self, Parameters(a): Parameters<RenameArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("name".into(), json!(a.name));
        self.run_plugin_tool("rename_object", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Reparent an instance to `parent` (path or handle).")]
    async fn move_object(&self, Parameters(a): Parameters<MoveArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("parent".into(), json!(a.parent));
        self.run_plugin_tool("move_object", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Clone an instance `count` times into a parent (default same).")]
    async fn mass_duplicate(&self, Parameters(a): Parameters<MassDupArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("count".into(), json!(a.count));
        if let Some(p) = a.parent {
            m.insert("parent".into(), json!(p));
        }
        self.run_plugin_tool("mass_duplicate", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Clone `count` times with an incrementing `name_prefix` and an optional per-copy `offset` ({type:Vector3,value:[..]}).")]
    async fn smart_duplicate(&self, Parameters(a): Parameters<SmartDupArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("count".into(), json!(a.count));
        if let Some(p) = a.parent {
            m.insert("parent".into(), json!(p));
        }
        if let Some(n) = a.name_prefix {
            m.insert("namePrefix".into(), json!(n));
        }
        if let Some(o) = a.offset {
            m.insert("offset".into(), o);
        }
        self.run_plugin_tool("smart_duplicate", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Build a nested instance tree from a `tree` spec {className, name?, properties?, children?} under `parent`.")]
    async fn create_ui_tree(&self, Parameters(a): Parameters<UiTreeArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = serde_json::Map::new();
        m.insert("tree".into(), a.tree);
        if let Some(p) = a.parent {
            m.insert("parent".into(), json!(p));
        }
        self.run_plugin_tool("create_ui_tree", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Undo the last ChangeHistoryService waypoint.")]
    async fn undo(&self, Parameters(a): Parameters<SessionSelector>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("undo", json!({}), a.session.as_deref()).await
    }

    #[tool(description = "Redo the next ChangeHistoryService waypoint.")]
    async fn redo(&self, Parameters(a): Parameters<SessionSelector>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("redo", json!({}), a.session.as_deref()).await
    }

    // ---- Editor suite: script editing ----

    #[tool(description = "Replace a script's entire source.")]
    async fn set_script_source(&self, Parameters(a): Parameters<SetSourceArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("source".into(), json!(a.source));
        self.run_plugin_tool("set_script_source", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Replace the inclusive line range [start_line, end_line] of a script with new_text.")]
    async fn edit_script_lines(&self, Parameters(a): Parameters<EditLinesArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("startLine".into(), json!(a.start_line));
        if let Some(e) = a.end_line {
            m.insert("endLine".into(), json!(e));
        }
        m.insert("newText".into(), json!(a.new_text));
        self.run_plugin_tool("edit_script_lines", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Insert text into a script after `after_line` (0 = top of file).")]
    async fn insert_script_lines(&self, Parameters(a): Parameters<InsertLinesArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("afterLine".into(), json!(a.after_line));
        m.insert("text".into(), json!(a.text));
        self.run_plugin_tool("insert_script_lines", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Delete the inclusive line range [start_line, end_line] from a script.")]
    async fn delete_script_lines(&self, Parameters(a): Parameters<DeleteLinesArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("startLine".into(), json!(a.start_line));
        if let Some(e) = a.end_line {
            m.insert("endLine".into(), json!(e));
        }
        self.run_plugin_tool("delete_script_lines", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Find/replace across a root's scripts. Literal by default; set use_pattern for Lua patterns. Scope with path/handle (default whole game).")]
    async fn find_and_replace_in_scripts(&self, Parameters(a): Parameters<FindReplaceArgs>) -> Result<CallToolResult, ErrorData> {
        let mut m = target(&a.path, &a.handle);
        m.insert("pattern".into(), json!(a.pattern));
        m.insert("replacement".into(), json!(a.replacement));
        if let Some(v) = a.use_pattern {
            m.insert("usePattern".into(), json!(v));
        }
        if let Some(v) = a.class_filter {
            m.insert("classFilter".into(), json!(v));
        }
        self.run_plugin_tool("find_and_replace_in_scripts", Value::Object(m), a.session.as_deref()).await
    }

    #[tool(description = "Compile-check a script (or every script under a container) and report syntax errors with line numbers.")]
    async fn get_script_analysis(&self, Parameters(a): Parameters<PathArgs>) -> Result<CallToolResult, ErrorData> {
        self.run_plugin_tool("get_script_analysis", Value::Object(target(&a.path, &a.handle)), a.session.as_deref()).await
    }
}

impl DustServer {
    /// Orchestrate stopping a playtest: end F5 via the server helper (only if one
    /// is live — otherwise report clearly instead of hanging, §6), then strip the
    /// injected helpers from the edit DataModel via the plugin.
    async fn stop_playtest_impl(&self, selector: Option<&str>) -> Result<CallToolResult, ErrorData> {
        let sessions = match self.broker.list_sessions().await {
            Ok(sessions) => sessions,
            Err(e) => return Ok(tool_error(format!("failed to reach broker: {e}"))),
        };
        let session_id = match resolve_session(&sessions, selector) {
            Ok(id) => id,
            Err(msg) => return Ok(tool_error(msg)),
        };

        let server_live = sessions
            .iter()
            .find(|s| s.session_id == session_id)
            .is_some_and(|s| {
                s.roles.iter().any(|r| r.role == Role::Server && r.state == LiveState::Live)
            });

        let mut report = serde_json::Map::new();
        report.insert("sessionId".into(), json!(session_id));

        if server_live {
            let end = self
                .broker
                .command(&session_id, "end_playtest", json!({}), Role::Server, 10_000)
                .await;
            report.insert(
                "endPlaytest".into(),
                match end {
                    Ok(r) => json!({ "ok": r.ok, "result": r.result, "error": r.error }),
                    Err(e) => json!({ "ok": false, "error": e.to_string() }),
                },
            );
        } else {
            report.insert(
                "endPlaytest".into(),
                json!({ "skipped": "no live server helper — playtest not running, or injection/HTTP not ready" }),
            );
        }

        // Strip helpers from the edit DataModel regardless (plugin role).
        let cleanup = self
            .broker
            .command(&session_id, "cleanup_helpers", json!({}), Role::Plugin, 15_000)
            .await;
        report.insert(
            "cleanup".into(),
            match cleanup {
                Ok(r) => json!({ "ok": r.ok, "result": r.result, "error": r.error }),
                Err(e) => json!({ "ok": false, "error": e.to_string() }),
            },
        );

        ok_json(&Value::Object(report))
    }

    /// Dispatch a command to a session's `plugin` role and turn the result into
    /// a tool response. The common case for edit-context tools.
    async fn run_plugin_tool(
        &self,
        tool: &str,
        args: Value,
        selector: Option<&str>,
    ) -> Result<CallToolResult, ErrorData> {
        self.run_tool(tool, args, Role::Plugin, COMMAND_TIMEOUT_MS, selector).await
    }

    /// Resolve the target session, dispatch a command to a given role, and turn
    /// the result into a tool response.
    async fn run_tool(
        &self,
        tool: &str,
        args: Value,
        role: Role,
        timeout_ms: u64,
        selector: Option<&str>,
    ) -> Result<CallToolResult, ErrorData> {
        let sessions = match self.broker.list_sessions().await {
            Ok(sessions) => sessions,
            Err(e) => return Ok(tool_error(format!("failed to reach broker: {e}"))),
        };

        let session_id = match resolve_session(&sessions, selector) {
            Ok(id) => id,
            Err(msg) => return Ok(tool_error(msg)),
        };

        match self.broker.command(&session_id, tool, args, role, timeout_ms).await {
            Ok(result) if result.ok => {
                ok_json(&json!({ "sessionId": session_id, "result": result.result }))
            }
            Ok(result) => Ok(tool_error(format!(
                "{role:?} helper returned an error: {}",
                result.error.unwrap_or_else(|| "unknown".into())
            ))),
            Err(e) => Ok(tool_error(format!("command failed: {e}"))),
        }
    }
}

#[tool_handler]
impl ServerHandler for DustServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::default(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "Dust bridges Claude to Roblox Studio via a local broker. Call list_sessions to \
                 discover open places. Place-targeting tools accept an optional `session` selector \
                 (id or label) and refuse to guess when several places are live."
                    .to_string(),
            ),
        }
    }
}

/// Wrap a serializable value as a successful tool result (JSON text content).
fn ok_json<T: serde::Serialize>(value: &T) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::success(vec![Content::json(value)?]))
}

/// A tool-level error the model can see and recover from (e.g. by calling
/// list_sessions), as opposed to a protocol error.
fn tool_error(message: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![Content::text(message.into())])
}

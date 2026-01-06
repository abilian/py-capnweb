"""Server implementation for Cap'n Web protocol."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import traceback
from collections import UserDict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

import aiohttp
from aiohttp import web

from capnweb.core.hooks import ErrorStubHook, PromiseStubHook, StubHook
from capnweb.core.payload import RpcPayload
from capnweb.core.resume import ResumeToken, ResumeTokenManager
from capnweb.core.session import RpcSession
from capnweb.error import ErrorCode, RpcError
from capnweb.protocol.ids import ImportId
from capnweb.protocol.wire import (
    PropertyKey,
    WireAbort,
    WireError,
    WireMessage,
    WirePipeline,
    WirePull,
    WirePush,
    WireReject,
    WireRelease,
    WireRemap,
    WireResolve,
    parse_wire_batch,
    serialize_wire_batch,
)

# Optional WebTransport support
try:
    from capnweb.transport.webtransport import WebTransportServer

    WEBTRANSPORT_AVAILABLE = True
except ImportError:
    WEBTRANSPORT_AVAILABLE = False

if TYPE_CHECKING:
    from capnweb.types import RpcTarget


class WebSocketServerSession(RpcSession):
    """WebSocket-specific session with bidirectional RPC support.

    This session tracks a single WebSocket connection and enables:
    - Client→Server RPC (client calls server methods)
    - Server→Client RPC (server calls client methods)

    Unlike HTTP Batch mode (stateless), WebSocket sessions are persistent
    and maintain state for the lifetime of the connection.
    """

    def __init__(
        self,
        ws: web.WebSocketResponse,
        server_exports: dict[int, StubHook],
        config: ServerConfig,
    ) -> None:
        """Initialize WebSocket session.

        Args:
            ws: The WebSocket connection
            server_exports: Shared server exports (capabilities registered on server)
            config: Server configuration
        """
        super().__init__()
        self._ws = ws
        self._server_exports = server_exports
        self._config = config
        # Track next import ID for server→client calls
        self._next_server_import_id = 1
        # Track pending server→client calls awaiting responses
        self._pending_calls: dict[int, asyncio.Future[RpcPayload]] = {}

    def get_export_hook(self, export_id: int) -> StubHook | None:
        """Get an export hook, checking both session and server exports."""
        # First check session exports (client capabilities passed to server)
        hook = super().get_export_hook(export_id)
        if hook is not None:
            return hook
        # Fall back to server exports (capabilities registered on server)
        return self._server_exports.get(export_id)

    def send_pipeline_call(
        self,
        import_id: int,
        path: list[str | int],
        args: RpcPayload,
        result_import_id: int,
    ) -> None:
        """Send a pipelined call to the client over WebSocket.

        This enables server→client RPC by sending WirePush messages.
        """
        # Serialize arguments
        args_value = self.serializer.serialize_payload(args)

        # Build property path
        property_path = [PropertyKey(str(p)) for p in path] if path else None

        # Look up the remote export_id from our import_id
        # The client needs to see its own export_id, not our import_id
        remote_export_id = self._import_to_remote_export.get(import_id, import_id)

        # Create pipeline expression using the remote's export_id
        # Note: field is named import_id but holds the remote export_id
        expression = WirePipeline(
            import_id=remote_export_id,
            property_path=property_path,
            args=args_value,
        )

        # Create push message with result import ID
        push_msg = WirePush(expression)

        # Send message over WebSocket (will be handled by background task)
        # We need to schedule this asynchronously
        asyncio.create_task(self._send_message(push_msg))

    def send_pipeline_get(
        self,
        import_id: int,
        path: list[str | int],
        result_import_id: int,
    ) -> None:
        """Send a pipelined property get to the client over WebSocket."""
        # Build property path
        property_path = [PropertyKey(str(p)) for p in path]

        # Look up the remote export_id from our import_id
        remote_export_id = self._import_to_remote_export.get(import_id, import_id)

        # Create pipeline expression (no args for property access)
        # Note: field is named import_id but holds the remote export_id
        expression = WirePipeline(
            import_id=remote_export_id,
            property_path=property_path,
            args=None,
        )

        # Create push message
        push_msg = WirePush(expression)

        # Send message over WebSocket
        asyncio.create_task(self._send_message(push_msg))

    async def pull_import(self, import_id: int) -> RpcPayload:
        """Pull the value from a server-side import (result of server→client call).

        This sends a WirePull to the client and awaits the response.
        """
        # Create a future to await the response
        future: asyncio.Future[RpcPayload] = asyncio.Future()
        self._pending_calls[import_id] = future

        # Send pull message
        pull_msg = WirePull(import_id)
        await self._send_message(pull_msg)

        # Wait for response (WireResolve or WireReject)
        return await future

    async def _send_message(self, msg: WireMessage) -> None:
        """Send a wire message to the client over WebSocket."""
        message_text = serialize_wire_batch([msg])
        await self._ws.send_str(message_text)

    def handle_resolve(self, import_id: int, value: Any) -> None:
        """Handle a resolve message from the client (response to server→client call)."""
        future = self._pending_calls.pop(import_id, None)
        if future and not future.done():
            # Parse the value and set result
            payload = self.parser.parse(value)
            future.set_result(payload)

    def handle_reject(self, import_id: int, error: Any) -> None:
        """Handle a reject message from the client (error response to server→client call)."""
        future = self._pending_calls.pop(import_id, None)
        if future and not future.done():
            # Convert error to RpcError and set exception
            if isinstance(error, WireError):
                err = RpcError(
                    ErrorCode(error.error_type),
                    error.message,
                    error.data,
                )
            else:
                err = RpcError.internal(f"Call failed: {error}")

            future.set_exception(err)


class ExportsProtocol(Protocol):
    """Protocol for exports that support both dict and ExportTable API."""

    def contains(self, export_id: Any) -> bool:
        """Check if export ID exists."""
        ...

    @property
    def _entries(self) -> dict[int, StubHook]:
        """Get raw entries dict."""
        ...

    def __getitem__(self, key: int) -> StubHook:
        """Get item by key."""
        ...

    def __setitem__(self, key: int, value: StubHook) -> None:
        """Set item by key."""
        ...

    def keys(self) -> Any:
        """Get keys."""
        ...

    def clear(self) -> None:
        """Clear all items."""
        ...

    def update(self, other: Any) -> None:
        """Update with other dict."""
        ...


@dataclass(frozen=True)
class ServerConfig:
    """Configuration for the Cap'n Web server."""

    host: str = "127.0.0.1"
    port: int = 8080
    max_batch_size: int = 100
    include_stack_traces: bool = False  # Security: disabled by default
    resume_token_ttl: float = 3600.0  # Resume token TTL in seconds (1 hour)

    # WebTransport configuration (optional)
    enable_webtransport: bool = False  # Enable WebTransport/HTTP/3
    webtransport_port: int = 4433  # Port for WebTransport (default: 4433)
    webtransport_cert_path: str | None = None  # Path to SSL certificate
    webtransport_key_path: str | None = None  # Path to SSL private key


class Server(RpcSession):
    """Cap'n Web server implementation.

    Supports multiple transport protocols:
    - POST /rpc/batch: HTTP batch RPC (full bidirectional support)
    - GET /rpc/ws: WebSocket RPC (partial support - client→server only)
    - WebTransport /rpc/wt: HTTP/3/QUIC RPC (optional, requires aioquic)

    Extends RpcSession to get unified import/export table management.

    Session State and Transport Considerations:
    -------------------------------------------
    This server manages session state (Import/Export tables) that persists between
    requests when using resume tokens. This creates important architectural considerations:

    1. **HTTP Batch (Stateless Transport)**:
       - Each HTTP request is independent
       - Server holds session state in memory between requests
       - Memory usage grows with number of active sessions
       - Resume tokens enable "sessionful" model over stateless HTTP
       - ✅ Full bidirectional RPC support
       - Best for: Short-lived sessions, single-server deployments

    2. **WebSocket (Stateful Transport)**:
       - Long-lived connection with natural session lifecycle
       - Session state lifetime tied to connection
       - More resource-efficient (session ends when connection closes)
       - No need for resume tokens within same connection
       - ⚠️ **Limitation**: Client→server RPC only (no server-initiated calls yet)
       - Best for: Long-running sessions where only client initiates calls

    3. **WebTransport (HTTP/3/QUIC)**:
       - High-performance multiplexed streams
       - 0-RTT reconnection support
       - ✅ Full bidirectional RPC support
       - Best for: High-throughput, low-latency applications

    WebSocket Bidirectional RPC Status:
    -----------------------------------
    Current WebSocket implementation supports REQUEST-RESPONSE pattern:
    - Client calls server methods: ✅ Works
    - Server responds to client: ✅ Works
    - Server initiates calls to client: ❌ Not yet implemented

    For bidirectional RPC (server calling client methods), use HTTP Batch or WebTransport.

    For production deployments:
    - Consider memory implications of holding sessions for HTTP clients
    - Use WebSocket for client→server RPC with persistent connections
    - Use HTTP Batch or WebTransport for bidirectional RPC
    - Use distributed session store (Redis) for multi-server HTTP deployments
    - Set appropriate `resume_token_ttl` to balance UX and resource usage
    """

    def __init__(self, config: ServerConfig) -> None:
        super().__init__()
        self.config = config
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._resume_manager = ResumeTokenManager(
            default_ttl=self.config.resume_token_ttl
        )
        # Track batch-local import tables for HTTP batch requests
        self._batch_imports: dict[int, StubHook] = {}

        # WebTransport server (optional)
        self._webtransport_server: Any = None
        self._webtransport_task: asyncio.Task | None = None

        # Create a wrapper that exposes both dict and ExportTable API
        self._exports_wrapper = self._create_exports_wrapper()

    @property
    def _exports_typed(self) -> ExportsProtocol:
        """Get _exports as ExportsProtocol for type checking."""
        return cast("ExportsProtocol", self._exports)

    def _create_exports_wrapper(self):
        """Create a wrapper that provides both dict and ExportTable API."""
        # Get the parent's _exports dict
        parent_exports = self.__dict__.get("_exports", {})

        class ExportsWrapper(UserDict):
            """Wrapper that acts like both a dict and ExportTable."""

            def contains(self, export_id):
                """ExportTable API compatibility."""
                if hasattr(export_id, "value"):
                    return export_id.value in self
                return export_id in self

            @property
            def _entries(self):
                """ExportTable API compatibility."""
                return self

        # Create wrapper with parent's exports
        wrapper = ExportsWrapper(parent_exports)
        # Store it directly in __dict__
        self.__dict__["_exports"] = wrapper
        return wrapper

    def register_capability(self, export_id: int, target: RpcTarget) -> None:
        """Register a capability with the given export ID.

        Args:
            export_id: The export ID (typically 0 for main capability)
            target: The RPC target implementation
        """
        self.register_target(export_id, target)

    async def __aenter__(self) -> Self:
        """Enter async context manager - starts the server."""
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context manager - stops the server."""
        await self.stop()

    @property
    def port(self) -> int:
        """Get the actual bound port (useful when port=0 for dynamic allocation)."""
        if self._site is None:
            return self.config.port
        # Get the first server socket from the site
        if self._site._server:
            return self._site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        return self.config.port

    async def start(self) -> None:
        """Start the server."""
        self._app = web.Application()
        self._app.router.add_post("/rpc/batch", self._handle_batch)
        self._app.router.add_get("/rpc/ws", self._handle_websocket)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        self._site = web.TCPSite(self._runner, self.config.host, self.config.port)
        await self._site.start()

        print(f"Server listening on {self.config.host}:{self.port}")

        # Start WebTransport server if enabled
        if self.config.enable_webtransport:
            await self._start_webtransport()

    async def stop(self) -> None:
        """Stop the server."""
        # Stop WebTransport server if running
        if self._webtransport_task:
            self._webtransport_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._webtransport_task
            self._webtransport_task = None

        if self._webtransport_server:
            await self._webtransport_server.close()
            self._webtransport_server = None

        if self._runner:
            await self._runner.cleanup()

    async def _handle_batch(self, request: web.Request) -> web.Response:
        """Handle HTTP batch requests."""
        try:
            body = await request.text()

            # Parse messages
            messages = parse_wire_batch(body)

            # DEBUG: Log for interop debugging
            logger = logging.getLogger(__name__)
            logger.debug("[BATCH] Received %d messages", len(messages))
            for i, msg in enumerate(messages):
                if isinstance(msg, WirePush):
                    logger.debug("  [%d] WirePush: %s", i, msg.expression)
                elif isinstance(msg, WirePull):
                    logger.debug("  [%d] WirePull: import_id=%d", i, msg.import_id)
                else:
                    logger.debug("  [%d] %s: %s", i, type(msg).__name__, msg)

            if len(messages) > self.config.max_batch_size:
                error = WireAbort(f"Batch size {len(messages)} exceeds maximum")
                return web.Response(
                    text=serialize_wire_batch([error]),
                    content_type="application/x-ndjson",
                    status=400,
                )

            # Process messages
            # Create a batch-local import table for this request
            # (HTTP batch is stateless - each request is a micro-session)
            batch_imports: dict[int, StubHook] = {}

            # Track push sequence for this batch (client's import ID space)
            next_push_import_id = 1
            responses: list[WireMessage] = []
            for msg in messages:
                match msg:
                    case WirePush():
                        # Assign the next sequential import ID for this push
                        import_id = next_push_import_id
                        next_push_import_id += 1
                        response = await self._handle_push(
                            msg.expression, import_id, batch_imports
                        )
                    case WirePull():
                        response = await self._handle_pull(msg.import_id, batch_imports)
                    case _:
                        response = await self._process_message(msg)

                if response:
                    responses.append(response)

            # DEBUG: Log responses
            logger.debug("[BATCH] Sending %d responses", len(responses))
            for i, resp in enumerate(responses):
                if isinstance(resp, WireResolve):
                    logger.debug("  [%d] WireResolve: export_id=%d", i, resp.export_id)
                elif isinstance(resp, WireReject):
                    logger.debug(
                        "  [%d] WireReject: export_id=%d, error=%s",
                        i,
                        resp.export_id,
                        resp.error,
                    )
                else:
                    logger.debug("  [%d] %s: %s", i, type(resp).__name__, resp)

            # Send responses
            if responses:
                response_body = serialize_wire_batch(responses)
                logger.debug("[BATCH] Response body:\n%s", response_body)
                return web.Response(
                    text=response_body,
                    content_type="application/x-ndjson",
                )
            return web.Response(status=204)

        except Exception as e:
            error = WireAbort(f"Server error: {e}")
            return web.Response(
                text=serialize_wire_batch([error]),
                content_type="application/x-ndjson",
                status=500,
            )

    async def _handle_websocket(self, request: web.Request) -> web.WebSocketResponse:
        """Handle WebSocket connections for bidirectional RPC.

        This provides a persistent connection for long-lived sessions with
        full bidirectional communication support:
        - Client→Server RPC: Client can call server methods
        - Server→Client RPC: Server can call client methods

        Session Management:
        - Creates WebSocketServerSession for connection lifetime
        - Maintains state across multiple messages within same connection
        - Cleans up resources automatically when connection closes
        """
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Create WebSocket-specific session with bidirectional RPC support
        session = WebSocketServerSession(ws, self._exports, self.config)

        try:
            # Track push sequence for client→server calls
            next_client_push_id = 1

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    next_client_push_id = await self._process_websocket_message(
                        msg.data, ws, session, next_client_push_id
                    )

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    next_client_push_id = await self._process_websocket_message(
                        msg.data.decode("utf-8"), ws, session, next_client_push_id
                    )

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger = logging.getLogger(__name__)
                    logger.error("WebSocket error: %s", ws.exception())
                    break

                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    break

        finally:
            # Clean up session resources
            # Make a copy of the values to avoid "dictionary changed size during iteration" error
            for hook in list(session._imports.values()):
                with contextlib.suppress(Exception):
                    hook.dispose()
            for hook in list(session._exports.values()):
                with contextlib.suppress(Exception):
                    hook.dispose()

        return ws

    async def _process_websocket_message(
        self,
        data: str,
        ws: web.WebSocketResponse,
        session: WebSocketServerSession,
        next_push_id: int,
    ) -> int:
        """Process a WebSocket message (either text or binary).

        Args:
            data: Message data as string
            ws: WebSocket connection
            session: WebSocket session
            next_push_id: Next push ID for client→server calls

        Returns:
            Updated next_push_id
        """
        try:
            messages = parse_wire_batch(data)

            if len(messages) > self.config.max_batch_size:
                error = WireAbort(f"Batch size {len(messages)} exceeds maximum")
                await ws.send_str(serialize_wire_batch([error]))
                return next_push_id

            # Process messages and collect responses
            responses, push_count = await self._handle_ws_messages(
                messages, next_push_id, session
            )

            # Send responses back over WebSocket
            if responses:
                response_text = serialize_wire_batch(responses)
                await ws.send_str(response_text)

            return next_push_id + push_count

        except Exception as e:
            # Send error and continue (don't break connection on parse errors)
            logger = logging.getLogger(__name__)
            logger.exception("WebSocket message processing error: %s", e)
            error = WireAbort(f"Error processing message: {e}")
            await ws.send_str(serialize_wire_batch([error]))
            return next_push_id

    async def _handle_ws_messages(
        self,
        messages: list[WireMessage],
        next_push_id: int,
        session: WebSocketServerSession,
    ) -> tuple[list[WireMessage], int]:
        """Handle a batch of WebSocket messages.

        Args:
            messages: List of wire messages to process
            next_push_id: Next push ID for client→server calls
            session: WebSocket session

        Returns:
            Tuple of (responses, push_count)
        """
        responses: list[WireMessage] = []
        push_count = 0

        for wire_msg in messages:
            match wire_msg:
                case WirePush():
                    # Client→Server call
                    import_id = next_push_id + push_count
                    push_count += 1
                    response = await self._handle_ws_push(
                        wire_msg.expression, import_id, session
                    )
                    if response:
                        responses.append(response)

                case WirePull():
                    # Client requests resolution
                    response = await self._handle_ws_pull(wire_msg.import_id, session)
                    if response:
                        responses.append(response)

                case WireResolve():
                    # Client resolving a server→client call
                    session.handle_resolve(wire_msg.export_id, wire_msg.value)

                case WireReject():
                    # Client rejecting a server→client call
                    session.handle_reject(wire_msg.export_id, wire_msg.error)

                case WireRelease():
                    # Client releasing an import
                    response = await self._handle_release(
                        ImportId(wire_msg.import_id), wire_msg.refcount
                    )
                    if response:
                        responses.append(response)

        return responses, push_count

    async def _handle_ws_push(
        self, expression: Any, import_id: int, session: WebSocketServerSession
    ) -> WireMessage | None:
        """Handle a push message from client over WebSocket.

        Args:
            expression: The wire expression (WirePipeline or WireRemap)
            import_id: The import ID assigned for this push
            session: The WebSocket session
        """
        try:
            # Handle WireRemap expressions
            if isinstance(expression, WireRemap):
                # Import here to avoid circular import at module level
                from capnweb.core.hooks import PayloadStubHook  # noqa: PLC0415
                from capnweb.core.mapper import MapApplicator  # noqa: PLC0415

                # Get the target hook
                target_hook = session.get_import_hook(expression.import_id)
                if target_hook is None:
                    target_hook = session.get_export_hook(expression.import_id)

                if target_hook is None:
                    msg = f"Capability {expression.import_id} not found"
                    raise RpcError.not_found(msg)

                # Navigate property path if provided
                collection_hook = target_hook
                if expression.property_path:
                    path = [str(pk.value) for pk in expression.property_path]
                    collection_hook = collection_hook.get(path)

                # Execute map asynchronously
                async def execute_remap() -> StubHook:
                    """Execute the remap operation and return the result hook."""
                    try:
                        # Pull the collection
                        payload = await collection_hook.pull()
                        collection = payload.value

                        # Convert captures to JSON format
                        captures_json = [cap.to_json() for cap in expression.captures]

                        # Convert instructions to JSON format
                        instructions_json = [
                            instr.to_json() if hasattr(instr, "to_json") else instr
                            for instr in expression.instructions
                        ]

                        # Create MapApplicator
                        applicator = MapApplicator(
                            session, captures_json, instructions_json
                        )

                        # Apply transformation to each item
                        results = []
                        for item in collection:
                            result = await applicator.execute(item)
                            results.append(result)

                        # Return result as PayloadStubHook
                        return PayloadStubHook(RpcPayload.owned(results))

                    except RpcError as e:
                        return ErrorStubHook(e)
                    except Exception as e:
                        logger = logging.getLogger(__name__)
                        logger.exception("Remap execution failed: %s", e)
                        return ErrorStubHook(RpcError.internal(f"Remap failed: {e}"))

                # Create a future for the result
                result_future: asyncio.Future[StubHook] = asyncio.create_task(
                    execute_remap()
                )

                # Store the future in the session imports
                session._imports[import_id] = PromiseStubHook(result_future)

                # No immediate response - client will pull when ready
                return None

            # Handle WirePipeline expressions
            if isinstance(expression, WirePipeline):
                # Get the target hook (either from session imports or server exports)
                target_hook = session.get_import_hook(expression.import_id)
                if target_hook is None:
                    target_hook = session.get_export_hook(expression.import_id)

                if target_hook is None:
                    msg = f"Capability {expression.import_id} not found"
                    raise RpcError.not_found(msg)

                # Parse arguments using the session's parser
                args_payload = (
                    session.parser.parse(expression.args)
                    if expression.args is not None
                    else RpcPayload.owned([])
                )

                # Extract the path
                path: list[str | int] = [
                    str(pk.value) for pk in (expression.property_path or [])
                ]

                # Execute the call asynchronously
                async def execute_call() -> StubHook:
                    """Execute the method call and return the result hook."""
                    try:
                        assert target_hook is not None
                        result_hook = await target_hook.call(path, args_payload)
                        return result_hook
                    except RpcError as e:
                        return ErrorStubHook(e)
                    except Exception as e:
                        logger = logging.getLogger(__name__)
                        logger.exception("Call execution failed: %s", e)
                        return ErrorStubHook(
                            RpcError.internal(f"Target call failed: {e}")
                        )

                # Create a future for the result
                result_future: asyncio.Future[StubHook] = asyncio.create_task(
                    execute_call()
                )

                # Store the future in the session imports as a PromiseStubHook
                session._imports[import_id] = PromiseStubHook(result_future)

                # No immediate response - client will pull when ready
                return None

            msg = "Expected WirePipeline or WireRemap expression in push"
            raise RpcError.bad_request(msg)

        except RpcError as e:
            stack = (
                str(e.data)
                if e.data
                else None
                if self.config.include_stack_traces
                else None
            )
            data = e.data if isinstance(e.data, dict) else None
            error_expr = WireError(str(e.code.value), e.message, stack, data)
            return WireReject(-import_id, error_expr)

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.exception("Unexpected error in push: %s", e)
            stack = traceback.format_exc() if self.config.include_stack_traces else None
            error_expr = WireError("internal", "Internal server error", stack)
            return WireReject(-import_id, error_expr)

    async def _handle_ws_pull(
        self, import_id: int, session: WebSocketServerSession
    ) -> WireMessage | None:
        """Handle a pull message from client over WebSocket.

        Args:
            import_id: The import ID the client wants to pull
            session: The WebSocket session
        """
        try:
            # Get the hook from session imports
            hook = session.get_import_hook(import_id)

            if hook is None:
                msg = f"Import {import_id} not found in session"
                raise RpcError.not_found(msg)

            # Pull the final payload from the hook
            payload = await hook.pull()

            # Serialize the payload using the session's serializer
            serialized_value = session.serializer.serialize_payload(payload)

            # Export ID matches the import ID in the response
            export_id = import_id

            # Send resolution
            return WireResolve(export_id, serialized_value)

        except RpcError as e:
            export_id = import_id
            stack = (
                str(e.data)
                if e.data
                else None
                if self.config.include_stack_traces
                else None
            )
            data = e.data if isinstance(e.data, dict) else None
            error_expr = WireError(str(e.code.value), e.message, stack, data)
            return WireReject(export_id, error_expr)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.exception("Unexpected error in pull: %s", e)
            export_id = import_id
            stack = traceback.format_exc() if self.config.include_stack_traces else None
            error_expr = WireError("internal", "Internal server error", stack)
            return WireReject(export_id, error_expr)

    async def _process_message(self, msg: WireMessage) -> WireMessage | None:
        """Process a single wire message.

        Returns a response message if needed, or None.
        Note: WirePush and WirePull are handled separately in _handle_batch
        to track sequential IDs and use batch-local import table.
        """
        match msg:
            case WireRelease(import_id, refcount):
                return await self._handle_release(ImportId(import_id), refcount)

            case _:
                # Push, Pull, Resolve, Reject, Abort are handled elsewhere or not expected
                return None

    async def _handle_push(
        self, expression: Any, import_id: int, imports: dict[int, StubHook]
    ) -> WireMessage | None:
        """Handle a push message - evaluate pipeline or remap expression and store result.

        Args:
            expression: The wire expression (WirePipeline or WireRemap)
            import_id: The import ID assigned sequentially for this push in the current batch
            imports: The batch-local import table

        The client's push messages are implicitly numbered sequentially (1, 2, 3...).
        The server tracks these in the import table so they can be pulled later.

        Note: WirePipeline and WireRemap expressions are handled directly here,
        not through the Parser, because they are server-side execution constructs.
        """
        try:
            # Handle WireRemap expressions
            if isinstance(expression, WireRemap):
                # Import here to avoid circular import at module level
                from capnweb.core.hooks import PayloadStubHook  # noqa: PLC0415
                from capnweb.core.mapper import MapApplicator  # noqa: PLC0415

                # Get the target hook
                target_hook = imports.get(expression.import_id)
                if target_hook is None:
                    target_hook = self.get_export_hook(expression.import_id)

                if target_hook is None:
                    msg = f"Capability {expression.import_id} not found"
                    raise RpcError.not_found(msg)

                # Navigate property path if provided
                collection_hook = target_hook
                if expression.property_path:
                    path = [str(pk.value) for pk in expression.property_path]
                    collection_hook = collection_hook.get(path)

                # Execute map asynchronously
                async def execute_remap() -> StubHook:
                    """Execute the remap operation and return the result hook."""
                    try:
                        # Pull the collection
                        payload = await collection_hook.pull()
                        collection = payload.value

                        # Convert captures to JSON format
                        captures_json = [cap.to_json() for cap in expression.captures]

                        # Convert instructions to JSON format
                        instructions_json = [
                            instr.to_json() if hasattr(instr, "to_json") else instr
                            for instr in expression.instructions
                        ]

                        # Create MapApplicator
                        applicator = MapApplicator(
                            self, captures_json, instructions_json
                        )

                        # Apply transformation to each item
                        results = []
                        for item in collection:
                            result = await applicator.execute(item)
                            results.append(result)

                        # Return result as PayloadStubHook
                        return PayloadStubHook(RpcPayload.owned(results))

                    except RpcError as e:
                        return ErrorStubHook(e)
                    except Exception as e:
                        logger = logging.getLogger(__name__)
                        logger.exception("Remap execution failed: %s", e)
                        return ErrorStubHook(RpcError.internal(f"Remap failed: {e}"))

                # Create a future for the result
                result_future: asyncio.Future[StubHook] = asyncio.create_task(
                    execute_remap()
                )

                # Store the future in the batch imports
                imports[import_id] = PromiseStubHook(result_future)

                # No immediate response - client will pull when ready
                return None

            # Handle WirePipeline expressions
            if isinstance(expression, WirePipeline):
                # Get the target hook (either from batch imports or our exports)
                target_hook = imports.get(expression.import_id)
                if target_hook is None:
                    target_hook = self.get_export_hook(expression.import_id)

                if target_hook is None:
                    msg = f"Capability {expression.import_id} not found"
                    raise RpcError.not_found(msg)

                # Parse arguments using the session's parser
                # This handles any ["export", id] references within the args
                args_payload = (
                    self.parser.parse(expression.args)
                    if expression.args is not None
                    else RpcPayload.owned([])
                )

                # Extract the path (method and property names)
                path: list[str | int] = [
                    str(pk.value) for pk in (expression.property_path or [])
                ]

                # Execute the call asynchronously
                async def execute_call() -> StubHook:
                    """Execute the method call and return the result hook."""
                    try:
                        # target_hook is guaranteed non-None at this point due to check above
                        assert target_hook is not None
                        # Call through the hook chain
                        result_hook = await target_hook.call(path, args_payload)
                        return result_hook

                    except RpcError as e:
                        # RPC errors become ErrorStubHook
                        return ErrorStubHook(e)
                    except Exception as e:
                        # Other errors become internal RPC errors
                        logger = logging.getLogger(__name__)
                        logger.exception("Call execution failed: %s", e)
                        return ErrorStubHook(
                            RpcError.internal(f"Target call failed: {e}")
                        )

                # Create a future for the result
                result_future: asyncio.Future[StubHook] = asyncio.create_task(
                    execute_call()
                )

                # Store the future in the batch imports as a PromiseStubHook
                # so the client can pull it later
                imports[import_id] = PromiseStubHook(result_future)

                # No immediate response - client will pull when ready
                return None

            msg = "Expected WirePipeline or WireRemap expression in push"
            raise RpcError.bad_request(msg)

        except RpcError as e:
            # If setup fails immediately, we should reject
            stack = (
                str(e.data)
                if e.data
                else None
                if self.config.include_stack_traces
                else None
            )
            data = e.data if isinstance(e.data, dict) else None
            error_expr = WireError(str(e.code.value), e.message, stack, data)
            return WireReject(-import_id, error_expr)

        except Exception as e:
            # Unexpected error - log it server-side but don't expose details to client
            logger = logging.getLogger(__name__)
            logger.exception("Unexpected error in push: %s", e)

            # Only include stack trace if configured (security)
            stack = traceback.format_exc() if self.config.include_stack_traces else None
            error_expr = WireError("internal", "Internal server error", stack)
            return WireReject(-import_id, error_expr)

    async def _handle_pull(
        self, import_id: int, imports: dict[int, StubHook]
    ) -> WireMessage | None:
        """Handle a pull message - resolve and send the result.

        Args:
            import_id: The import ID the client wants to pull
            imports: The batch-local import table

        Returns:
            WireResolve with the serialized result, or WireReject on error
        """
        try:
            # Get the hook from batch imports
            hook = imports.get(import_id)

            if hook is None:
                msg = f"Import {import_id} not found in batch"
                raise RpcError.not_found(msg)

            # Pull the final payload from the hook
            # This awaits the promise if it's a PromiseStubHook
            payload = await hook.pull()

            # Serialize the payload using the session's serializer
            # This will handle exporting any RpcStub/RpcPromise within the result
            serialized_value = self.serializer.serialize_payload(payload)

            # Export ID matches import ID (positive) per Cap'n Web protocol spec
            export_id = import_id

            # Send resolution
            return WireResolve(export_id, serialized_value)

        except RpcError as e:
            # Send rejection with matching export_id (positive)
            export_id = import_id
            stack = (
                str(e.data)
                if e.data
                else None
                if self.config.include_stack_traces
                else None
            )
            data = e.data if isinstance(e.data, dict) else None
            error_expr = WireError(str(e.code.value), e.message, stack, data)
            return WireReject(export_id, error_expr)
        except Exception as e:
            # Unexpected error - log but don't expose details
            logger = logging.getLogger(__name__)
            logger.exception("Unexpected error in pull: %s", e)

            export_id = import_id
            stack = traceback.format_exc() if self.config.include_stack_traces else None
            error_expr = WireError("internal", "Internal server error", stack)
            return WireReject(export_id, error_expr)

    def create_resume_token(
        self, metadata: dict[str, Any] | None = None
    ) -> ResumeToken:
        """Create a resume token for the current session.

        Args:
            metadata: Optional custom metadata to include in the token

        Returns:
            ResumeToken that can be used to restore this session
        """
        # Snapshot current session state
        # For now, we'll store just the export IDs since imports are batch-local
        imports_dict = dict.fromkeys(self._imports.keys())  # Placeholder
        exports_dict = dict.fromkeys(self._exports.keys())  # Placeholder

        return self._resume_manager.create_token(
            imports=imports_dict, exports=exports_dict, metadata=metadata
        )

    def restore_from_token(self, token: ResumeToken) -> bool:
        """Restore session state from a resume token.

        Args:
            token: Token to restore from

        Returns:
            True if restoration was successful and session was found, False otherwise
        """
        result = self._resume_manager.restore_session(token)
        if result is None:
            return False

        imports_dict, exports_dict, session_found = result

        # If session was not found, this is a failed restoration
        if not session_found:
            return False

        # Restore exports into the wrapper
        # The wrapper is a dict subclass, so we can update it directly
        self._exports.clear()
        self._exports.update(exports_dict)

        # Restore imports if any were saved
        # Note: For HTTP batch mode, imports are batch-local, so restoration
        # mainly applies to WebSocket/stateful connections
        if imports_dict:
            self._imports.clear()
            self._imports.update(imports_dict)

        return True

    def invalidate_resume_token(self, session_id: str) -> None:
        """Invalidate a resume token.

        Args:
            session_id: Session ID to invalidate
        """
        self._resume_manager.invalidate_token(session_id)

    def cleanup_expired_tokens(self) -> int:
        """Clean up expired resume tokens.

        Returns:
            Number of tokens cleaned up
        """
        return self._resume_manager.cleanup_expired()

    async def _handle_release(
        self, import_id: ImportId, refcount: int
    ) -> WireMessage | None:
        """Handle a release message - cleanup import table entries.

        Args:
            import_id: The import ID to release
            refcount: The total number of times this import has been introduced
        """
        # Release the import
        self.release_import(import_id.value)

        # No response needed for release
        return None

    # RpcSession abstract method implementations

    def send_pipeline_call(
        self,
        import_id: int,
        path: list[str | int],
        args: RpcPayload,
        result_import_id: int,
    ) -> None:
        """Send a pipelined call message.

        Not used in HTTP batch mode - raises NotImplementedError.
        """
        msg = "Pipelining from server not supported in HTTP batch mode"
        raise NotImplementedError(msg)

    def send_pipeline_get(
        self,
        import_id: int,
        path: list[str | int],
        result_import_id: int,
    ) -> None:
        """Send a pipelined property get message.

        Not used in HTTP batch mode - raises NotImplementedError.
        """
        msg = "Pipelining from server not supported in HTTP batch mode"
        raise NotImplementedError(msg)

    async def pull_import(self, import_id: int) -> RpcPayload:
        """Pull the value from a remote capability.

        Not used in HTTP batch mode - raises NotImplementedError.
        """
        msg = "Pull from server not supported in HTTP batch mode"
        raise NotImplementedError(msg)

    # WebTransport support methods

    async def _start_webtransport(self) -> None:
        """Start the WebTransport server."""
        if (
            not self.config.webtransport_cert_path
            or not self.config.webtransport_key_path
        ):
            print("WARNING: WebTransport enabled but certificate paths not provided")
            print("         Skipping WebTransport server startup")
            return

        if not WEBTRANSPORT_AVAILABLE:
            print("WARNING: WebTransport requires aioquic library")
            print("         Install with: uv pip install aioquic")
            print("         Skipping WebTransport server startup")
            return

        try:
            # Create WebTransport server
            self._webtransport_server = WebTransportServer(
                host=self.config.host,
                port=self.config.webtransport_port,
                cert_path=Path(self.config.webtransport_cert_path),
                key_path=Path(self.config.webtransport_key_path),
                handler=self._handle_webtransport_session,
            )

            # Start in background task
            async def run_webtransport() -> None:
                await self._webtransport_server.serve()

            self._webtransport_task = asyncio.create_task(run_webtransport())

            print(
                f"WebTransport server listening on https://{self.config.host}:{self.config.webtransport_port}/rpc/wt"
            )

        except Exception as e:
            print(f"ERROR: Failed to start WebTransport server: {e}")
            raise

    async def _handle_webtransport_session(self, protocol: Any, stream_id: int) -> None:
        """Handle a WebTransport session.

        Args:
            protocol: The WebTransport protocol instance
            stream_id: The stream ID for this session
        """
        try:
            while True:
                # Receive request data
                data = await protocol.receive_data(stream_id, timeout=60.0)

                if not data:
                    # Client closed connection
                    break

                # Parse NDJSON batch
                messages = parse_wire_batch(data.decode("utf-8"))

                if len(messages) > self.config.max_batch_size:
                    error = WireAbort(f"Batch size {len(messages)} exceeds maximum")
                    response_data = serialize_wire_batch([error]).encode("utf-8")
                    await protocol.send_data(stream_id, response_data)
                    break

                # Process messages
                responses: list[WireMessage] = []
                for msg in messages:
                    response = await self._process_message(msg)
                    if response is not None:
                        responses.append(response)

                # Send responses
                response_data = serialize_wire_batch(responses).encode("utf-8")
                await protocol.send_data(stream_id, response_data)

        except TimeoutError:
            # Session timed out
            pass
        except Exception as e:
            # Send error and close
            error = WireAbort(f"Server error: {e}")
            error_data = serialize_wire_batch([error]).encode("utf-8")
            with contextlib.suppress(Exception):
                # Best effort - ignore if sending error fails
                await protocol.send_data(stream_id, error_data)

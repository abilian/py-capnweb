"""Client implementation for Cap'n Web protocol."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from capnweb.core.hooks import ErrorStubHook, PayloadStubHook, TargetStubHook
from capnweb.core.payload import RpcPayload
from capnweb.core.pipeline import PendingCall, PipelineBatch, PipelinePromise
from capnweb.core.resume import ResumeToken  # noqa: TC001
from capnweb.core.session import RpcSession
from capnweb.core.stubs import RpcStub
from capnweb.error import ErrorCode, RpcError
from capnweb.protocol.ids import ExportId
from capnweb.protocol.wire import (
    PropertyKey,
    WireAbort,
    WireCapture,
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
from capnweb.transport.transports import (
    HttpBatchTransport,
    WebSocketTransport,
    WebTransportTransport,
    create_transport,
)

if TYPE_CHECKING:
    from capnweb.core.hooks import StubHook
    from capnweb.types import RpcTarget


@dataclass
class ClientConfig:
    """Configuration for the Cap'n Web client."""

    url: str
    timeout: float = 30.0


class Client(RpcSession):
    """Cap'n Web client implementation.

    Supports multiple transports via the Transport abstraction.
    Extends RpcSession to get unified import/export table management.
    """

    def __init__(self, config: ClientConfig) -> None:
        super().__init__()
        self.config = config
        self._transport: (
            HttpBatchTransport | WebSocketTransport | WebTransportTransport | None
        ) = None
        self._import_ref_counts: dict[int, int] = {}
        # WebSocket bidirectional RPC support
        self._ws_listener_task: asyncio.Task | None = None
        self._ws_pending_client_calls: dict[int, asyncio.Future[list[WireMessage]]] = {}
        self._next_server_push_id = 1  # Track server→client push IDs
        self._next_client_import_id = 1  # Track client→server import IDs for WebSocket
        self._ws_listener_ready = asyncio.Event()  # Signal when listener is ready

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        self._transport = create_transport(self.config.url, timeout=self.config.timeout)
        # Manually manage transport lifecycle - we're composing context managers
        await self._transport.__aenter__()

        # Start WebSocket listener for bidirectional RPC
        if isinstance(self._transport, WebSocketTransport):
            self._ws_listener_task = asyncio.create_task(self._ws_listen_loop())
            # Wait for listener to be ready before proceeding
            await self._ws_listener_ready.wait()

        return self

    async def __aexit__(self, *args: object) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the client connection."""
        # Cancel WebSocket listener task
        if self._ws_listener_task and not self._ws_listener_task.done():
            self._ws_listener_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ws_listener_task

        if self._transport:
            await self._transport.close()
            self._transport = None

    async def call(
        self,
        cap_id: int,
        method: str,
        args: list[Any],
        property_path: list[str] | None = None,
    ) -> Any:
        """Call a method on a remote capability.

        Args:
            cap_id: The capability ID (use 0 for main capability)
            method: The method name
            args: List of arguments
            property_path: Optional property path to navigate before calling method

        Returns:
            The result of the method call

        Raises:
            RpcError: If the call fails
        """
        if not self._transport:
            # Auto-create transport if not using context manager
            self._transport = create_transport(
                self.config.url, timeout=self.config.timeout
            )
            await self._transport.__aenter__()  # noqa: PLC2801

        # Allocate import ID
        import_id = self._allocate_call_import_id()

        # Build and send request
        batch = self._build_call_batch(cap_id, method, args, property_path, import_id)

        try:
            # Get messages based on transport type
            messages = await self._send_call_batch(batch, import_id)

            # Process responses
            return self._process_call_responses(messages, import_id)

        except RpcError:
            raise
        except Exception as e:
            msg = f"Transport error: {e}"
            raise RpcError.internal(msg) from e

    def _allocate_call_import_id(self) -> int:
        """Allocate an import ID for a call."""
        if isinstance(self._transport, WebSocketTransport):
            import_id = self._next_client_import_id
            self._next_client_import_id += 1
            return import_id
        return 1

    def _build_call_batch(
        self,
        cap_id: int,
        method: str,
        args: list[Any],
        property_path: list[str] | None,
        import_id: int,
    ) -> str:
        """Build a wire batch for a call."""
        # Build property path including method name
        full_path = (property_path or []) + [method]
        path_keys = [PropertyKey(p) for p in full_path]

        # Serialize arguments
        args_payload = RpcPayload.from_app_params(args)
        serialized_args = self.serializer.serialize_payload(args_payload)

        # Create pipeline expression
        pipeline_expr = WirePipeline(
            import_id=cap_id,
            property_path=path_keys,
            args=serialized_args,
        )

        # Create push and pull messages
        push_msg = WirePush(pipeline_expr)
        pull_msg = WirePull(import_id)

        return serialize_wire_batch([push_msg, pull_msg])

    async def _send_call_batch(self, batch: str, import_id: int) -> list[WireMessage]:
        """Send a call batch and receive responses."""
        if isinstance(self._transport, WebSocketTransport):
            return await self._send_ws_call(batch, import_id)
        return await self._send_http_call(batch)

    async def _send_ws_call(self, batch: str, import_id: int) -> list[WireMessage]:
        """Send a call over WebSocket."""
        logger = logging.getLogger(__name__)
        logger.debug("WebSocket call: registering import_id=%s", import_id)

        # Register future for this call
        future: asyncio.Future[list[WireMessage]] = asyncio.Future()
        self._ws_pending_client_calls[import_id] = future

        # Send messages
        logger.debug("Sending batch: %s", batch[:200])
        await self._transport.send(batch.encode("utf-8"))  # type: ignore[union-attr]

        # Wait for response
        logger.debug("Waiting for response for import_id=%s", import_id)
        try:
            messages = await asyncio.wait_for(future, timeout=10.0)
            logger.debug("Received response: %s", messages)
            return messages
        except TimeoutError:
            logger.error("Timeout waiting for response for import_id=%s", import_id)
            logger.error(
                "Pending calls: %s", list(self._ws_pending_client_calls.keys())
            )
            msg = f"Timeout waiting for response for import_id={import_id}"
            raise RpcError.internal(msg) from None

    async def _send_http_call(self, batch: str) -> list[WireMessage]:
        """Send a call over HTTP."""
        response_bytes = await self._transport.send_and_receive(  # type: ignore[union-attr]
            batch.encode("utf-8")
        )
        response_text = response_bytes.decode("utf-8")

        if not response_text:
            return []

        return parse_wire_batch(response_text)

    def _process_call_responses(
        self, messages: list[WireMessage], import_id: int
    ) -> Any:
        """Process call response messages."""
        result = None
        error = None

        for msg in messages:
            if isinstance(msg, WireResolve) and abs(msg.export_id) == import_id:
                result_payload = self.parser.parse(msg.value)
                result = result_payload.value
            elif isinstance(msg, WireReject) and abs(msg.export_id) == import_id:
                error = self._parse_error(msg.error)

        if error:
            raise error

        return result

    async def _process_message(self, msg: WireMessage) -> None:
        """Process a response message from the server."""
        match msg:
            case WireResolve(export_id, value):
                await self._handle_resolve(ExportId(export_id), value)

            case WireReject(export_id, error):
                await self._handle_reject(ExportId(export_id), error)

            case WireAbort(error):
                await self._handle_abort(error)

            case _:
                # Other message types not expected in client responses
                pass

    async def _handle_resolve(self, export_id: ExportId, value: Any) -> None:
        """Handle a resolve message from the server."""
        # Convert export ID to import ID (they're negatives of each other)
        import_id = -export_id.value

        # Parse the value using the parser
        result_payload = self.parser.parse(value)

        # Create a PayloadStubHook and resolve the promise
        hook = PayloadStubHook(result_payload)
        self.resolve_promise(import_id, hook)

    def _parse_error(self, error_expr: Any) -> RpcError:
        """Parse an error expression into an RpcError."""
        if isinstance(error_expr, WireError):
            # Try to map error type to ErrorCode, default to INTERNAL if unknown
            error_type = error_expr.error_type.lower().replace(" ", "_")
            try:
                code = ErrorCode(error_type)
            except ValueError:
                # Unknown error code, use INTERNAL
                code = ErrorCode.INTERNAL
            return RpcError(code, error_expr.message, error_expr.stack)
        return RpcError.internal(f"Unknown error: {error_expr}")

    async def _handle_reject(self, export_id: ExportId, error_expr: Any) -> None:
        """Handle a reject message from the server."""
        # Convert export ID to import ID
        import_id = -export_id.value

        # Parse error
        error = self._parse_error(error_expr)

        # Reject the promise
        self.reject_promise(import_id, error)

    async def _handle_abort(self, error_expr: Any) -> None:
        """Handle an abort message from the server."""
        # Parse error
        if isinstance(error_expr, WireError):
            error = RpcError(
                ErrorCode.INTERNAL,
                f"Server aborted: {error_expr.message}",
                error_expr.stack,
            )
        else:
            error = RpcError.internal(f"Server aborted: {error_expr}")

        # Reject all pending promises
        for _promise_id, future in list(self._pending_promises.items()):
            if not future.done():
                future.set_exception(error)

        self._pending_promises.clear()

    def _send_release_message(self, import_id: int) -> None:
        """Send a release message to the remote side.

        Args:
            import_id: The import ID to release
        """
        if not self._transport:
            return

        # Get reference count
        refcount = self._import_ref_counts.pop(import_id, 1)

        # Send release message (best-effort, non-blocking)
        async def send_release():
            if self._transport:
                release_msg = WireRelease(import_id, refcount)
                batch = serialize_wire_batch([release_msg])

                with suppress(Exception):
                    await self._transport.send_and_receive(batch.encode("utf-8"))

        # Schedule the release to run in the background
        asyncio.create_task(send_release())

    def register_capability(self, export_id: int, target: RpcTarget) -> None:
        """Register a local capability that can be called by the server.

        Args:
            export_id: The export ID (should be negative for local exports)
            target: The RPC target implementation
        """
        self.register_target(export_id, target)

    def create_stub(
        self, target: RpcTarget
    ) -> Any:  # Returns RpcStub but avoiding circular import
        """Create a stub for a local capability that can be passed to the server.

        This allocates an export ID and registers the target, returning an RpcStub
        that can be passed as an argument to server methods.

        Args:
            target: The local RpcTarget to export

        Returns:
            RpcStub that references the exported capability
        """
        # Create hook for the target
        hook = TargetStubHook(target)

        # Allocate next export ID
        export_id = len(self._exports)
        while export_id in self._exports:
            export_id += 1

        # Store the hook in exports so the serializer can find it
        self._exports[export_id] = hook

        # Return stub wrapping the hook
        return RpcStub(hook)

    def get_remote_stub(self, remote_export_id: int) -> RpcStub:
        """Get a stub for a remote capability exported by the server.

        This creates an import for the server's capability and returns a stub
        that can be used to call methods on it.

        Args:
            remote_export_id: The export ID of the capability on the server

        Returns:
            RpcStub that references the remote capability
        """
        # Use the import_capability method from RpcSession to create the import
        hook = self.import_capability(remote_export_id)

        # Return stub with session for map() support
        return RpcStub(hook, session=self)

    def validate_resume_token(self, token: ResumeToken) -> bool:
        """Validate a resume token.

        Args:
            token: Token to validate

        Returns:
            True if token is valid (not expired, properly formed), False otherwise

        Note:
            This only validates the token structure and expiration.
            Server-side validation is required to ensure the session still exists.
        """
        return token.is_valid()

    def get_resume_token_info(self, token: ResumeToken) -> dict[str, Any]:
        """Get information about a resume token without restoring it.

        Args:
            token: Token to inspect

        Returns:
            Dictionary with token information (session_id, expires_at, etc.)
        """
        return {
            "session_id": token.session_id,
            "created_at": token.created_at,
            "expires_at": token.expires_at,
            "is_expired": token.is_expired(),
            "is_valid": token.is_valid(),
            "capability_count": len(token.capabilities),
            "metadata": token.metadata,
        }

    def pipeline(self) -> PipelineBatch:
        """Create a pipeline batch for batching multiple RPC calls.

        Returns:
            A PipelineBatch that can be used to make multiple calls
            that will be sent in a single HTTP batch

        Example:
            ```python
            batch = client.pipeline()
            user = batch.call(0, "authenticate", ["token-123"])
            profile = batch.call(0, "getUserProfile", [user.id])
            notifications = batch.call(0, "getNotifications", [user.id])

            # All three calls sent in one HTTP request
            u, p, n = await asyncio.gather(user, profile, notifications)
            ```
        """
        return PipelineBatch(self)

    def call_pipelined(
        self,
        batch: PipelineBatch,
        cap_id: int,
        method: str,
        args: list[Any],
        property_path: list[str] | None = None,
    ) -> PipelinePromise:
        """Make a pipelined RPC call as part of a batch.

        Args:
            batch: The pipeline batch this call belongs to
            cap_id: The capability ID (use 0 for main capability)
            method: The method name
            args: List of arguments (can include PipelinePromise objects)
            property_path: Optional property path to navigate before calling method

        Returns:
            A PipelinePromise that can be awaited or used in other pipelined calls

        Note:
            This method is typically called through PipelineBatch.call() rather than directly.
        """
        import_id = batch._allocate_import_id()
        pending_call = PendingCall(
            import_id=import_id,
            cap_id=cap_id,
            method=method,
            args=args,
            property_path=property_path,
        )
        batch._add_call(pending_call)
        return PipelinePromise(self, batch, import_id)

    # RpcSession abstract method implementations

    def send_pipeline_call(
        self,
        import_id: int,
        path: list[str | int],
        args: RpcPayload,
        result_import_id: int,
    ) -> None:
        """Send a pipelined call message.

        This is called by RpcImportHook when a call is made on a remote
        capability. Since HTTP batch transport can't pipeline, we need to
        send immediately and block.

        Args:
            import_id: The import ID to call on
            path: Property path + method name
            args: Arguments for the call
            result_import_id: Import ID for the result
        """
        # For HTTP batch, we can't truly pipeline - we have to send immediately
        # Create a task that will be resolved when the result comes back

        # Build property path for the wire message
        path_keys = [PropertyKey(p) for p in path]

        # Serialize arguments
        serialized_args = self.serializer.serialize_payload(args)

        # Look up the remote export_id from our import_id
        # The server needs to see its own export_id, not our import_id
        remote_export_id = self._import_to_remote_export.get(import_id, import_id)

        # Create pipeline expression using the remote's export_id
        # Note: field is named import_id but holds the remote export_id
        pipeline_expr = WirePipeline(
            import_id=remote_export_id,
            property_path=path_keys,
            args=serialized_args,
        )

        # Create push and pull messages
        # Server auto-assigns sequential IDs starting from 1, so request ID 1
        push_msg = WirePush(pipeline_expr)
        server_import_id = 1  # Server will assign this ID to the WirePush
        pull_msg = WirePull(server_import_id)

        # Send immediately in a background task
        async def send_and_handle():
            if not self._transport:
                return

            batch = serialize_wire_batch([push_msg, pull_msg])
            response_bytes = await self._transport.send_and_receive(
                batch.encode("utf-8")
            )
            response_text = response_bytes.decode("utf-8")

            # Parse response and resolve the pending import
            # Server responds with positive export_id per Cap'n Web protocol
            messages = parse_wire_batch(response_text)
            for msg in messages:
                if isinstance(msg, WireResolve) and msg.export_id == server_import_id:
                    result_payload = self.parser.parse(msg.value)
                    # Get the future for this import
                    if result_import_id in self._pending_promises:
                        future = self._pending_promises.pop(result_import_id)
                        if not future.done():
                            # Create hook from the payload
                            result_hook = PayloadStubHook(result_payload)
                            future.set_result(result_hook)
                elif isinstance(msg, WireReject) and msg.export_id == server_import_id:
                    error = self._parse_error(msg.error)
                    if result_import_id in self._pending_promises:
                        future = self._pending_promises.pop(result_import_id)
                        if not future.done():
                            error_hook = ErrorStubHook(error)
                            future.set_result(error_hook)

        # Schedule the task
        asyncio.create_task(send_and_handle())

    def send_pipeline_get(
        self,
        import_id: int,
        path: list[str | int],
        result_import_id: int,
    ) -> None:
        """Send a pipelined property get message.

        This is called by RpcImportHook when a property is accessed on a
        remote capability.

        Args:
            import_id: The import ID to get from
            path: Property path
            result_import_id: Import ID for the result
        """
        # Similar to send_pipeline_call but with no args
        self.send_pipeline_call(import_id, path, RpcPayload.owned([]), result_import_id)

    def send_pipeline_map(
        self,
        import_id: int,
        path: list[str | int],
        captures: list,
        instructions: list[Any],
        result_import_id: int,
    ) -> None:
        """Send a pipelined map message.

        This is called by RpcImportHook/ChainedImportHook when .map() is called
        on a remote capability.

        Args:
            import_id: The import ID to map over
            path: Property path to the iterable
            captures: External capabilities (StubHooks) used in the map function
            instructions: Operations to perform for each element
            result_import_id: Import ID for the result
        """
        # Import here to avoid circular import at module level
        from capnweb.core.hooks import StubHook  # noqa: PLC0415

        # Convert captures to wire format
        wire_captures: list[WireCapture] = []
        for cap_hook in captures:
            if not isinstance(cap_hook, StubHook):
                continue

            # Try to find this hook in our imports
            cap_import_id = self._get_import_id_for_hook(cap_hook)
            if cap_import_id is not None:
                # It's a remote capability we already imported
                remote_export_id = self._import_to_remote_export.get(
                    cap_import_id, cap_import_id
                )
                wire_captures.append(WireCapture("import", remote_export_id))
            else:
                # It's a local capability we need to export
                # Wrap hook in RpcStub for export_capability
                cap_export_id = self.export_capability(RpcStub(cap_hook))
                wire_captures.append(WireCapture("export", cap_export_id))

        # Build property path
        path_keys = [PropertyKey(p) for p in path] if path else None

        # Look up the remote export_id from our import_id
        remote_export_id = self._import_to_remote_export.get(import_id, import_id)

        # Create WireRemap expression
        remap_expr = WireRemap(
            import_id=remote_export_id,
            property_path=path_keys,
            captures=wire_captures,
            instructions=instructions,
        )

        # Create push and pull messages
        # Server auto-assigns sequential IDs starting from 1, so request ID 1
        push_msg = WirePush(remap_expr)
        server_import_id = 1  # Server will assign this ID to the WirePush
        pull_msg = WirePull(server_import_id)

        # Send in a background task
        async def send_and_handle():
            if not self._transport:
                return

            batch = serialize_wire_batch([push_msg, pull_msg])
            try:
                response_bytes = await self._transport.send_and_receive(
                    batch.encode("utf-8")
                )
                response_text = response_bytes.decode("utf-8")

                # Parse response and resolve the pending import
                # Server responds with positive export_id per Cap'n Web protocol
                messages = parse_wire_batch(response_text)
                for msg in messages:
                    if (
                        isinstance(msg, WireResolve)
                        and msg.export_id == server_import_id
                    ):
                        result_payload = self.parser.parse(msg.value)
                        # Get the future for this import
                        if result_import_id in self._pending_promises:
                            future = self._pending_promises.pop(result_import_id)
                            if not future.done():
                                # Create hook from the payload
                                result_hook = PayloadStubHook(result_payload)
                                future.set_result(result_hook)
                    elif (
                        isinstance(msg, WireReject)
                        and msg.export_id == server_import_id
                    ):
                        error = self._parse_error(msg.error)
                        if result_import_id in self._pending_promises:
                            future = self._pending_promises.pop(result_import_id)
                            if not future.done():
                                error_hook = ErrorStubHook(error)
                                future.set_result(error_hook)
            except Exception as e:
                # Handle transport errors
                if result_import_id in self._pending_promises:
                    future = self._pending_promises.pop(result_import_id)
                    if not future.done():
                        error = RpcError.internal(f"Transport error: {e}")
                        error_hook = ErrorStubHook(error)
                        future.set_result(error_hook)

        # Schedule the task
        asyncio.create_task(send_and_handle())

    def _get_import_id_for_hook(self, hook: StubHook) -> int | None:
        """Look up the import ID for a hook in our imports table.

        Args:
            hook: The StubHook to look up

        Returns:
            The import ID if found, None otherwise
        """

        for import_id, stored_hook in self._imports.items():
            if stored_hook is hook:
                return import_id
        return None

    async def pull_import(self, import_id: int) -> RpcPayload:
        """Pull the value from a remote capability.

        This is called by RpcImportHook when awaiting a remote capability.

        Args:
            import_id: The import ID to pull

        Returns:
            The pulled value as RpcPayload
        """
        # Send a pull message and await the response
        if not self._transport:
            msg = "No transport available"
            raise RpcError.internal(msg)

        pull_msg = WirePull(import_id)
        batch = serialize_wire_batch([pull_msg])

        response_bytes = await self._transport.send_and_receive(batch.encode("utf-8"))
        response_text = response_bytes.decode("utf-8")

        if not response_text:
            msg = "Empty response from pull"
            raise RpcError.internal(msg)

        # Parse responses
        messages = parse_wire_batch(response_text)

        for msg in messages:
            if isinstance(msg, WireResolve) and msg.export_id == import_id:
                # Parse and return the value
                return self.parser.parse(msg.value)
            if isinstance(msg, WireReject) and msg.export_id == import_id:
                error = self._parse_error(msg.error)
                raise error

        msg = "No response for pull"
        raise RpcError.internal(msg)

    async def _ws_listen_loop(self) -> None:
        """Background task to listen for incoming WebSocket messages.

        This enables bidirectional RPC where the server can call methods on
        client-side capabilities.
        """
        if not isinstance(self._transport, WebSocketTransport):
            return

        logger = logging.getLogger(__name__)
        logger.debug("WebSocket listener started")

        try:
            # Signal that listener is ready
            self._ws_listener_ready.set()
            logger.debug("WebSocket listener ready")

            while True:
                # Receive messages from server
                try:
                    logger.debug("Waiting for message from server...")
                    message_bytes = await self._transport.receive()
                    message_text = message_bytes.decode("utf-8")
                    logger.debug("Received message: %s", message_text[:200])

                    # Parse and process messages
                    messages = parse_wire_batch(message_text)
                    responses = await self._process_ws_messages(messages)

                    # Send responses back to server
                    if responses:
                        response_batch = serialize_wire_batch(responses)
                        await self._transport.send(response_batch.encode("utf-8"))

                except asyncio.CancelledError:
                    break
                except Exception:
                    # Log and continue - don't break the listener on errors
                    logger = logging.getLogger(__name__)
                    logger.exception("Error in WebSocket listener")
                    # Small delay to prevent tight loop on persistent errors
                    await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            pass

    async def _process_ws_messages(
        self, messages: list[WireMessage]
    ) -> list[WireMessage]:
        """Process a batch of WebSocket messages.

        Args:
            messages: List of wire messages to process

        Returns:
            List of response messages to send back
        """
        responses: list[WireMessage] = []

        for msg in messages:
            match msg:
                case WirePush():
                    # Server→Client call
                    response = await self._handle_server_push(msg.expression)
                    if response:
                        responses.append(response)

                case WirePull():
                    # Server requesting resolution
                    response = await self._handle_server_pull(msg.import_id)
                    if response:
                        responses.append(response)

                case WireResolve():
                    # Response to a client→server call
                    self._handle_wire_resolve(msg)

                case WireReject():
                    # Error response to a client→server call
                    self._handle_wire_reject(msg)

        return responses

    def _handle_wire_resolve(self, msg: WireResolve) -> None:
        """Handle a WireResolve message (response to client call).

        Args:
            msg: The WireResolve message
        """
        logger = logging.getLogger(__name__)
        import_id = msg.export_id
        logger.debug(
            "WireResolve: export_id=%s, pending=%s",
            import_id,
            list(self._ws_pending_client_calls.keys()),
        )

        if import_id in self._ws_pending_client_calls:
            future = self._ws_pending_client_calls.pop(import_id)
            if not future.done():
                logger.debug("Completing future for import_id=%s", import_id)
                future.set_result([msg])
        else:
            logger.warning("Received WireResolve for unknown import_id=%s", import_id)

    def _handle_wire_reject(self, msg: WireReject) -> None:
        """Handle a WireReject message (error response to client call).

        Args:
            msg: The WireReject message
        """
        logger = logging.getLogger(__name__)
        import_id = msg.export_id
        logger.debug(
            "WireReject: export_id=%s, pending=%s",
            import_id,
            list(self._ws_pending_client_calls.keys()),
        )

        if import_id in self._ws_pending_client_calls:
            future = self._ws_pending_client_calls.pop(import_id)
            if not future.done():
                logger.debug("Completing future with error for import_id=%s", import_id)
                future.set_result([msg])
        else:
            logger.warning("Received WireReject for unknown import_id=%s", import_id)

    async def _handle_server_push(self, expression: Any) -> WireMessage | None:
        """Handle a push message from server (server calling client method).

        Args:
            expression: The wire expression (expected to be WirePipeline)

        Returns:
            Response message or None
        """
        # Allocate import ID for this server→client call
        import_id = self._next_server_push_id
        self._next_server_push_id += 1

        try:
            if not isinstance(expression, WirePipeline):
                msg = "Expected WirePipeline expression"
                raise RpcError.bad_request(msg)

            # Get the target from exports (client-side capabilities)
            target_hook = self.get_export_hook(expression.import_id)
            if target_hook is None:
                msg = f"Export {expression.import_id} not found on client"
                raise RpcError.not_found(msg)

            # Parse arguments
            args_payload = (
                self.parser.parse(expression.args)
                if expression.args is not None
                else RpcPayload.owned([])
            )

            # Extract path
            path: list[str | int] = [
                str(pk.value) for pk in (expression.property_path or [])
            ]

            # Execute the call
            result_hook = await target_hook.call(path, args_payload)

            # Store result in imports so server can pull it
            self._imports[import_id] = result_hook

            # No immediate response - server will pull when ready
            return None

        except RpcError as e:
            # Return error
            error_expr = WireError(
                str(e.code.value),
                e.message,
                None,
                e.data if isinstance(e.data, dict) else None,
            )
            return WireReject(-import_id, error_expr)

        except Exception as e:
            # Internal error
            logger = logging.getLogger(__name__)
            logger.exception("Error handling server push: %s", e)
            error_expr = WireError("internal", f"Internal error: {e}", None)
            return WireReject(-import_id, error_expr)

    async def _handle_server_pull(self, import_id: int) -> WireMessage | None:
        """Handle a pull message from server (server requesting result).

        Args:
            import_id: The import ID the server wants to pull

        Returns:
            Response message or None
        """
        try:
            # Get the hook from imports
            hook = self.get_import_hook(import_id)
            if hook is None:
                msg = f"Import {import_id} not found on client"
                raise RpcError.not_found(msg)

            # Pull the final value
            payload = await hook.pull()

            # Serialize the result
            serialized_value = self.serializer.serialize_payload(payload)

            # Send resolution
            return WireResolve(import_id, serialized_value)

        except RpcError as e:
            error_expr = WireError(
                str(e.code.value),
                e.message,
                None,
                e.data if isinstance(e.data, dict) else None,
            )
            return WireReject(import_id, error_expr)

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.exception("Error handling server pull: %s", e)
            error_expr = WireError("internal", f"Internal error: {e}", None)
            return WireReject(import_id, error_expr)

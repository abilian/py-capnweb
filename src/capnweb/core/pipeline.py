"""Promise pipelining support for Cap'n Web client.

This module provides support for batching multiple dependent RPC calls
into a single round trip using promise pipelining.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from capnweb.error import RpcError
from capnweb.protocol.ids import ImportId
from capnweb.protocol.wire import (
    PropertyKey,
    WireMessage,
    WirePipeline,
    WirePull,
    WirePush,
    WireReject,
    WireResolve,
    parse_wire_batch,
    serialize_wire_batch,
)
from capnweb.transport.transports import create_transport

if TYPE_CHECKING:
    from capnweb.client import Client


@dataclass
class PendingCall:
    """A pending RPC call in a pipeline batch."""

    import_id: ImportId
    cap_id: int
    method: str
    args: list[Any]
    property_path: list[str] | None = None


class PipelinePromise:
    """A promise that can be used in pipelined calls.

    This class wraps a future import ID and allows property access
    to create pipelined references without awaiting the result.
    """

    def __init__(
        self,
        client: Client,
        batch: PipelineBatch,
        import_id: ImportId,
    ) -> None:
        """Initialize a pipeline promise.

        Args:
            client: The client instance
            batch: The batch this promise belongs to
            import_id: The import ID for this promise
        """
        self._client = client
        self._batch = batch
        self._import_id = import_id
        self._result: Any = None
        self._resolved = False

    def __getattr__(self, name: str) -> PipelinePromise:
        """Access a property on the promised value, creating a pipelined reference.

        Args:
            name: Property name to access

        Returns:
            A new PipelinePromise representing the property access
        """
        # Create a new import ID for the pipelined property access
        next_import_id = self._batch._allocate_import_id()

        # Create a WirePipeline expression for this property access
        pipeline_expr = WirePipeline(
            import_id=self._import_id.value,
            property_path=[PropertyKey(name)],
            args=None,
        )

        # Register this as a pending call in the batch
        self._batch._add_pipeline_expr(next_import_id, pipeline_expr)

        # Return a new promise for the property value
        return PipelinePromise(self._client, self._batch, next_import_id)

    def __await__(self):
        """Make this promise awaitable.

        This triggers the batch execution if not already executed.
        """
        return self._batch._execute_and_get_result(self._import_id).__await__()


class PipelineBatch:
    """A batch of pipelined RPC calls.

    This class manages a collection of RPC calls that should be sent
    together in a single HTTP batch, with promise references between them.
    """

    def __init__(self, client: Client) -> None:
        """Initialize a pipeline batch.

        Args:
            client: The client instance
        """
        self._client = client
        self._pending_calls: dict[ImportId, PendingCall | WirePipeline] = {}
        self._results: dict[ImportId, Any] = {}
        self._executed = False
        self._executing_lock = asyncio.Lock()
        self._next_import_id = 1

    def _allocate_import_id(self) -> ImportId:
        """Allocate a new import ID for this batch.

        Returns:
            A new import ID
        """
        import_id = ImportId(self._next_import_id)
        self._next_import_id += 1
        return import_id

    def _add_call(self, pending_call: PendingCall) -> None:
        """Add a pending call to the batch.

        Args:
            pending_call: The call to add
        """
        self._pending_calls[pending_call.import_id] = pending_call

    def _add_pipeline_expr(
        self, import_id: ImportId, pipeline_expr: WirePipeline
    ) -> None:
        """Add a pipeline expression to the batch.

        Args:
            import_id: Import ID for this expression
            pipeline_expr: The pipeline expression
        """
        self._pending_calls[import_id] = pipeline_expr

    async def _execute_and_get_result(self, import_id: ImportId) -> Any:
        """Execute the batch and get the result for a specific import ID.

        Args:
            import_id: The import ID to get the result for

        Returns:
            The result value

        Raises:
            RpcError: If the call fails
        """
        # Execute if not already executed (with proper locking)
        await self._execute()

        if import_id in self._results:
            result = self._results[import_id]
            # If the result is an exception, raise it
            if isinstance(result, Exception):
                raise result
            return result

        msg = f"No result for import ID {import_id}"
        raise RuntimeError(msg)

    def call(
        self,
        cap_id: int,
        method: str,
        args: list[Any],
        property_path: list[str] | None = None,
    ) -> PipelinePromise:
        """Make a pipelined RPC call.

        Args:
            cap_id: The capability ID (use 0 for main capability)
            method: The method name
            args: List of arguments (can include PipelinePromise objects)
            property_path: Optional property path to navigate before calling method

        Returns:
            A PipelinePromise that can be awaited or used in other pipelined calls
        """
        return self._client.call_pipelined(self, cap_id, method, args, property_path)

    async def _ensure_transport(self) -> None:
        """Ensure the transport is available and connected."""
        if not self._client._transport:
            self._client._transport = create_transport(
                self._client.config.url, timeout=self._client.config.timeout
            )
        transport = self._client._transport
        assert transport is not None
        await transport.__aenter__()  # noqa: PLC2801

    def _build_batch_messages(self) -> tuple[list[WireMessage], dict[int, ImportId]]:
        """Build the batch of push and pull messages.

        Returns:
            A tuple containing:
            - List of wire messages to send.
            - A map from server-side export IDs to client-side import IDs.
        """
        messages: list[WireMessage] = []
        server_to_client_id_map: dict[int, ImportId] = {}

        # Add push messages and build ID map
        for server_export_id, (client_import_id, call_or_expr) in enumerate(
            self._pending_calls.items(), start=1
        ):
            if isinstance(call_or_expr, PendingCall):
                pending_call = call_or_expr
                # Build property path including method name
                full_path = (pending_call.property_path or []) + [pending_call.method]
                path_keys = [PropertyKey(p) for p in full_path]

                # Create pipeline expression
                pipeline_expr = WirePipeline(
                    import_id=pending_call.cap_id,
                    property_path=path_keys,
                    args=pending_call.args,
                )
                messages.append(WirePush(pipeline_expr))

            elif isinstance(call_or_expr, WirePipeline):
                # Direct pipeline expression
                messages.append(WirePush(call_or_expr))

            # Map server's implicit export ID to client-side import ID
            server_to_client_id_map[server_export_id] = client_import_id

        # Add pull messages for all export IDs (using server-side sequential IDs)
        messages.extend(WirePull(server_id) for server_id in server_to_client_id_map)

        return messages, server_to_client_id_map

    def _process_response_messages(
        self,
        response_messages: list[WireMessage],
        server_to_client_id_map: dict[int, ImportId],
    ) -> None:
        """Process response messages and store results using the ID map.

        Args:
            response_messages: List of response messages from the server
            server_to_client_id_map: Map from server-side export IDs to client-side import IDs
        """
        for msg in response_messages:
            if isinstance(msg, WireResolve):
                # Server responds with negative export_id, so take absolute value
                server_export_id = abs(msg.export_id)
                client_import_id = server_to_client_id_map.get(server_export_id)

                if client_import_id is not None:
                    # Parse the value using the client's parser
                    parsed_payload = self._client.parser.parse(msg.value)
                    self._results[client_import_id] = parsed_payload.value

            elif isinstance(msg, WireReject):
                # Server responds with negative export_id, so take absolute value
                server_export_id = abs(msg.export_id)
                client_import_id = server_to_client_id_map.get(server_export_id)

                if client_import_id is not None:
                    # Parse error and store it as an exception
                    error = self._client._parse_error(msg.error)
                    # Store the error - will be raised when awaited
                    self._results[client_import_id] = error

    async def _execute(self) -> None:
        """Execute all pending calls in a single batch.

        This method sends all pending calls to the server in a single
        HTTP batch request and stores the results.
        """
        async with self._executing_lock:
            if self._executed:
                return

            self._executed = True

            # Ensure transport is available
            await self._ensure_transport()

            # Build batch of push and pull messages, getting ID mapping
            messages, server_to_client_id_map = self._build_batch_messages()

            # Send the entire batch in one request
            batch = serialize_wire_batch(messages)

            # Verify transport is available (should always be true after _ensure_transport)
            if not self._client._transport:
                msg = "Transport not available after initialization"
                raise RpcError.internal(msg)

            try:
                response_bytes = await self._client._transport.send_and_receive(
                    batch.encode("utf-8")
                )
                response_text = response_bytes.decode("utf-8")

                if not response_text:
                    return

                # Parse and process responses with ID mapping
                response_messages = parse_wire_batch(response_text)
                self._process_response_messages(
                    response_messages, server_to_client_id_map
                )

            except Exception as e:
                # Store the error for all pending calls
                error = RpcError.internal(f"Batch execution failed: {e}")
                for import_id in self._pending_calls:
                    self._results[import_id] = error

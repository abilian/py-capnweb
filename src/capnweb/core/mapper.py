"""Implementation of the .map() operation for server-side transformations.

This module provides the client-side MapBuilder for creating ["remap", ...]
expressions and the server-side MapApplicator for executing them.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from capnweb.core.payload import RpcPayload
from capnweb.error import RpcError

if TYPE_CHECKING:
    from collections.abc import Callable

    from capnweb.core.session import RpcSession
    from capnweb.core.stubs import RpcPromise

# Import StubHook outside TYPE_CHECKING since it's used at runtime
from capnweb.core.hooks import StubHook


class MapBuilder:
    """Builds a ["remap", ...] expression from a Python lambda."""

    def __init__(
        self, session: RpcSession, subject_hook: StubHook, path: list[str | int]
    ):
        self.session = session
        self.subject_hook = subject_hook
        self.path = path
        self.instructions: list[Any] = []
        self.captures: list[StubHook] = []
        self.capture_map: dict[StubHook, int] = {}

    def make_input_promise(self) -> RpcPromise:
        """Create the placeholder promise to pass to the user's map function."""
        from capnweb.core.hooks import MapVariableHook  # noqa: PLC0415
        from capnweb.core.stubs import RpcPromise  # noqa: PLC0415

        hook = MapVariableHook(self, 0)
        return RpcPromise(hook, session=self.session)

    def add_instruction(
        self, hook: StubHook, path: list[str | int], args: RpcPayload | None
    ) -> StubHook:
        """Record a pipeline instruction."""
        from capnweb.core.hooks import MapVariableHook  # noqa: PLC0415

        subject_idx = self._capture_hook(hook)
        path_keys = [str(p) for p in path]

        devalued_args = None
        if args:
            # The Exporter protocol is used here to devaluate arguments,
            # converting any RpcStubs within them into capture indices.
            devalued_args = self.session.serializer.serialize(args.value)

        instruction = ["pipeline", subject_idx, path_keys, devalued_args]
        self.instructions.append(instruction)
        return MapVariableHook(self, len(self.instructions))

    def _capture_hook(self, hook: StubHook) -> int:
        """Add a hook to captures if it's external, or return its variable index."""
        from capnweb.core.hooks import MapVariableHook  # noqa: PLC0415

        if isinstance(hook, MapVariableHook) and hook.builder is self:
            return hook.idx  # It's one of our internal variables.

        if hook in self.capture_map:
            return self.capture_map[hook]

        # It's an external capability, so add it to captures.
        capture_idx = -len(self.captures) - 1
        self.captures.append(hook)
        self.capture_map[hook] = capture_idx
        return capture_idx

    def build_and_send(self, final_hook: StubHook) -> StubHook:
        """Finalize the instructions, build the WireRemap, and send it."""
        # The final operation's result is the return value of the map function.
        final_idx = self._capture_hook(final_hook)
        self.instructions.append(final_idx)

        return self.subject_hook.map(self.path, self.captures, self.instructions)


def send_map(
    session: RpcSession,
    subject_hook: StubHook,
    path: list[str | int],
    func: Callable,
) -> RpcPromise:
    """Orchestrate the creation of a .map() call."""
    from capnweb.core.hooks import PayloadStubHook  # noqa: PLC0415
    from capnweb.core.stubs import RpcPromise  # noqa: PLC0415

    builder = MapBuilder(session, subject_hook, path)
    placeholder = builder.make_input_promise()

    try:
        # Execute the user's lambda. Calls on the placeholder will be
        # intercepted by the MapBuilder and recorded as instructions.
        result = func(placeholder)
    except Exception as e:
        # Errors during map construction are fatal.
        msg = f"Error building map function: {e}"
        raise RpcError.internal(msg) from e

    if asyncio.iscoroutine(result) or isinstance(result, RpcPromise):
        # The final result must be an RpcStub/RpcPromise from the builder.
        if hasattr(result, "_hook") and isinstance(result._hook, StubHook):
            final_hook = result._hook
        else:
            msg = "Map function must not be async and must return a value derived from the input."
            raise RpcError.bad_request(msg)
    else:
        # The user returned a plain value. Wrap it in a hook.
        final_hook = PayloadStubHook(RpcPayload.owned(result))

    result_hook = builder.build_and_send(final_hook)
    return RpcPromise(result_hook, session=session)


class MapApplicator:
    """Executes a `remap` instruction on the server."""

    def __init__(
        self,
        session: RpcSession,
        captures_json: list[list[Any]],
        instructions: list[Any],
    ):
        self.session = session
        self.instructions = instructions
        self.captures: list[StubHook] = []
        for cap_expr in captures_json:
            cap_hook = self._resolve_capture(cap_expr)
            self.captures.append(cap_hook)

    def _resolve_capture(self, cap_expr: list[Any]) -> StubHook:
        """Resolve a capture from a wire expression like ["import", id]."""
        cap_type, cap_id = cap_expr
        if cap_type == "import":
            # The capture is a capability the client already knows about
            # and is passing back to us.
            hook = self.session.get_import_hook(cap_id)
            if hook:
                return hook.dup()
        elif cap_type == "export":
            # The capture is a new capability the client is exporting to us.
            return self.session.import_capability(cap_id)

        msg = f"Invalid capture in remap: {cap_expr}"
        raise RpcError.internal(msg)

    async def execute(self, input_value: Any) -> Any:
        """Execute the map instructions for a single input value."""
        from capnweb.core.hooks import PayloadStubHook  # noqa: PLC0415

        variables: list[StubHook] = [PayloadStubHook(RpcPayload.owned(input_value))]

        try:
            for instruction in self.instructions:
                if isinstance(instruction, int):
                    # Final instruction is just an index.
                    final_hook = self._resolve_variable(instruction, variables)
                    result_payload = await final_hook.pull()
                    return result_payload.value

                # It's a pipeline instruction
                # Handle both 3-element (property access) and 4-element (method call) formats
                # from WirePipeline.to_json()
                op = instruction[0]
                subject_idx = instruction[1]
                path = instruction[2]
                args_json = instruction[3] if len(instruction) > 3 else None

                if op != "pipeline":
                    msg = "Only pipeline instructions are supported in remap"
                    raise RpcError.internal(msg)

                subject_hook = self._resolve_variable(subject_idx, variables)

                # Distinguish between property access (args_json is None) and method call
                if args_json is None:
                    # Property access - use .get()
                    result_hook = subject_hook.get(path)
                else:
                    # Method call - deserialize arguments and use .call()
                    args_list = self.session.parser.parse(args_json)
                    args_payload = RpcPayload.owned(args_list)
                    result_hook = await subject_hook.call(path, args_payload)
                variables.append(result_hook)

            msg = "Map instructions ended without a final result"
            raise RpcError.internal(msg)
        finally:
            # Dispose all intermediate variables and captures.
            for hook in variables:
                hook.dispose()
            for hook in self.captures:
                hook.dispose()

    def _resolve_variable(self, index: int, variables: list[StubHook]) -> StubHook:
        """Resolve a variable index to a hook."""
        if index == 0:
            return variables[0]  # The input value
        if index > 0:
            return variables[index]  # Result of a previous instruction
        if index < 0:
            return self.captures[-index - 1]  # An external capture

        msg = f"Invalid variable index in remap: {index}"
        raise RpcError.internal(msg)

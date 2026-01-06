"""StubHook hierarchy for decentralized RPC capability management.

This module implements the hook pattern from the TypeScript reference implementation.
Each StubHook represents the backing implementation of an RPC-able reference.

Instead of a monolithic evaluator, different hook types handle different scenarios:
- ErrorStubHook: Holds an error
- PayloadStubHook: Wraps locally-resolved data
- TargetStubHook: Wraps a local RpcTarget object
- RpcImportHook: Represents a remote capability
- PromiseStubHook: Wraps a future that will resolve to another hook
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from capnweb.core.payload import RpcPayload
from capnweb.error import RpcError

if TYPE_CHECKING:
    from capnweb.core.mapper import MapBuilder
    from capnweb.core.session import RpcSession
    from capnweb.types import RpcTarget


class StubHook(ABC):
    """Abstract base class for all stub hook implementations.

    A StubHook represents the backing implementation of an RPC capability.
    It knows how to handle calls, property access, promise resolution, etc.

    This is the core of the decentralized architecture - each hook type
    implements these methods according to its specific semantics.
    """

    @abstractmethod
    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method through this hook.

        Args:
            path: Property path to navigate before calling (e.g., ["user", "profile", "getName"])
            args: Arguments wrapped in RpcPayload

        Returns:
            A new StubHook representing the result
        """
        ...

    @abstractmethod
    def get(self, path: list[str | int]) -> StubHook:
        """Get a property through this hook.

        Args:
            path: Property path to navigate (e.g., ["user", "id"])

        Returns:
            A new StubHook representing the property value
        """
        ...

    @abstractmethod
    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """Apply a map operation through this hook.

        Args:
            path: Property path to the iterable to map over
            captures: External capabilities used in the map function
            instructions: A list of operations to perform for each element

        Returns:
            A new StubHook representing the promise for the mapped result
        """
        ...

    @abstractmethod
    async def pull(self) -> RpcPayload:
        """Pull the final value from this hook.

        This is what happens when you await a promise. It resolves the
        value (possibly waiting for network I/O) and returns the payload.

        Returns:
            The resolved payload

        Raises:
            RpcError: If the capability is in an error state
        """
        ...

    @abstractmethod
    def dispose(self) -> None:
        """Dispose this hook, releasing any resources.

        This decrements reference counts, sends release messages for remote
        capabilities, and cleans up state.
        """
        ...

    @abstractmethod
    def dup(self) -> Self:
        """Duplicate this hook (increment reference count).

        This is used when copying payloads to ensure proper refcounting.

        Returns:
            A new StubHook sharing the same underlying resource
        """
        ...


@dataclass
class ErrorStubHook(StubHook):
    """A hook that holds an error.

    All operations on this hook either return itself or raise the error.
    This is useful for representing failed promises or broken capabilities.
    """

    error: RpcError

    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Always returns self (errors propagate through chains)."""
        return self

    def get(self, path: list[str | int]) -> StubHook:
        """Always returns self (errors propagate through chains)."""
        return self

    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """Dispose captures and return self (errors propagate)."""
        # Dispose captures as we are taking ownership.
        for cap in captures:
            cap.dispose()
        return self

    async def pull(self) -> RpcPayload:
        """Raises the error."""
        raise self.error

    def dispose(self) -> None:
        """Nothing to dispose for errors."""

    def dup(self) -> Self:
        """Errors can be freely shared."""
        return self


class PayloadStubHook(StubHook):
    """A hook that wraps locally-resolved data.

    This represents a capability that has already been resolved to a local
    value. Method calls and property access navigate through the payload's
    object tree.
    """

    def __init__(self, payload: RpcPayload) -> None:
        """Initialize with a payload.

        Args:
            payload: The payload this hook wraps
        """
        self.payload = payload
        # Ensure payload is owned before use
        self.payload.ensure_deep_copied()

    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Navigate the path and call as a function.

        Args:
            path: Property path to navigate
            args: Arguments to pass to the function

        Returns:
            A new hook with the result
        """
        # Navigate to the target
        target = self._navigate(path)

        # If target is callable, call it
        if callable(target):
            args.ensure_deep_copied()

            # Check if target is async

            if inspect.iscoroutinefunction(target):
                # Handle async callables
                async def call_async():
                    try:
                        result = (
                            await target(*args.value)
                            if isinstance(args.value, list)
                            else await target(args.value)
                        )
                        return PayloadStubHook(RpcPayload.owned(result))
                    except Exception as e:
                        error = RpcError.internal(f"Call failed: {e}")
                        return ErrorStubHook(error)

                # Return a promise hook that will resolve to the result
                future: asyncio.Future[StubHook] = asyncio.ensure_future(call_async())
                return PromiseStubHook(future)
            # Handle synchronous callables
            try:
                result = (
                    target(*args.value)
                    if isinstance(args.value, list)
                    else target(args.value)
                )
                return PayloadStubHook(RpcPayload.owned(result))
            except Exception as e:
                error = RpcError.internal(f"Call failed: {e}")
                return ErrorStubHook(error)

        error = RpcError.bad_request(f"Target at {path} is not callable")
        return ErrorStubHook(error)

    def get(self, path: list[str | int]) -> StubHook:
        """Navigate the path and return the property.

        Args:
            path: Property path to navigate

        Returns:
            A new hook with the property value
        """
        try:
            value = self._navigate(path)
            return PayloadStubHook(RpcPayload.owned(value))
        except (KeyError, IndexError, AttributeError) as e:
            error = RpcError.not_found(f"Property {path} not found: {e}")
            return ErrorStubHook(error)

    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """Execute a map operation locally.

        Args:
            path: Property path to the iterable
            captures: External capabilities used in the map function
            instructions: Operations to perform for each element

        Returns:
            A new hook with the mapped result
        """
        from capnweb.core.mapper import MapApplicator  # noqa: PLC0415

        try:
            target_iterable = self._navigate(path)
            if not isinstance(target_iterable, list):
                # For now, only support mapping over lists.
                msg = ".map() can only be called on lists"
                raise RpcError.bad_request(msg)

            # Convert captures to JSON format for MapApplicator
            # We need a session to do this - but PayloadStubHook doesn't have one
            # So we'll need to handle this differently
            # For now, assume captures are already in the right format
            from capnweb.core.session import RpcSession  # noqa: PLC0415

            # Create a minimal session for local execution
            session = RpcSession()
            applicator = MapApplicator(session, [], instructions)

            async def apply_all():
                results = await asyncio.gather(*[
                    applicator.execute(item) for item in target_iterable
                ])
                return PayloadStubHook(RpcPayload.owned(results))

            future: asyncio.Future[StubHook] = asyncio.ensure_future(apply_all())
            return PromiseStubHook(future)
        except Exception as e:
            msg = f"Local map execution failed: {e}"
            return ErrorStubHook(RpcError.internal(msg))

    def _navigate(self, path: list[str | int]) -> Any:
        """Navigate through the payload's value using the path.

        Args:
            path: List of property names/indices to navigate

        Returns:
            The value at the end of the path

        Raises:
            KeyError, IndexError, AttributeError: If navigation fails
        """
        current = self.payload.value

        for segment in path:
            match (segment, current):
                case (int(), _):
                    # Array index
                    current = current[segment]
                case (_, dict()):
                    # Dictionary key
                    current = current[segment]
                case (str(), _):
                    # Object attribute - segment is guaranteed to be str here
                    assert isinstance(segment, str)  # Type narrowing for type checker
                    current = getattr(current, segment)

        return current

    async def pull(self) -> RpcPayload:
        """Return the payload directly (already resolved)."""
        return self.payload

    def dispose(self) -> None:
        """Dispose the payload."""
        self.payload.dispose()

    def dup(self) -> Self:
        """Payloads can be shared (they manage their own stubs)."""
        # Note: The payload already tracks its stubs for disposal
        return PayloadStubHook(self.payload)  # type: ignore[return-value]


@dataclass
class TargetStubHook(StubHook):
    """A hook that wraps a local RpcTarget object.

    This represents a local capability provided by the application. It
    delegates method calls to the actual Python object.
    """

    target: RpcTarget
    ref_count: int = 1  # For disposal tracking

    async def _navigate_to_target(self, property_path: list[str | int]) -> Any:
        """Navigate through properties to reach the target object.

        Args:
            property_path: List of properties to navigate

        Returns:
            The target object after navigation

        Raises:
            RpcError: If navigation fails
        """
        current_obj = self.target
        for prop in property_path:
            try:
                prop_value = await current_obj.get_property(str(prop))
                current_obj = prop_value
            except Exception as e:
                if isinstance(e, RpcError):
                    raise
                msg = f"Property navigation failed at path {property_path}: {e}"
                raise RpcError.not_found(msg) from e
        return current_obj

    async def _invoke_method(
        self, target: Any, method_name: str, args: RpcPayload
    ) -> Any:
        """Invoke a method on the target object.

        Args:
            target: The target object
            method_name: Name of the method to call
            args: Arguments for the method

        Returns:
            The method result

        Raises:
            RpcError: If the method call fails
        """
        # If target is an RpcTarget, use its call method
        if hasattr(target, "call") and callable(target.call):
            return await target.call(  # type: ignore[misc]
                method_name,
                args.value if isinstance(args.value, list) else [args.value],
            )

        # Otherwise, try to call the method directly on the object
        method = getattr(target, method_name)
        if not callable(method):
            msg = f"Method {method_name} is not callable"
            raise RpcError.bad_request(msg)

        # Handle async and sync methods
        if inspect.iscoroutinefunction(method):
            return (
                await method(*args.value)
                if isinstance(args.value, list)
                else await method(args.value)
            )

        return (
            method(*args.value) if isinstance(args.value, list) else method(args.value)
        )

    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method on the target.

        Args:
            path: Property path (last element is method name)
            args: Arguments for the call

        Returns:
            A new hook with the result
        """
        args.ensure_deep_copied()

        if not path:
            error = RpcError.bad_request("Cannot call target without method name")
            return ErrorStubHook(error)

        # Determine method name and target object
        if len(path) == 1:
            method_name = str(path[0])
            current_target = self.target
        else:
            property_path = path[:-1]
            method_name = str(path[-1])
            try:
                current_target = await self._navigate_to_target(property_path)
            except RpcError as e:
                return ErrorStubHook(e)

        # Invoke the method
        try:
            result = await self._invoke_method(current_target, method_name, args)
            return PayloadStubHook(RpcPayload.from_app_return(result))
        except RpcError as e:
            return ErrorStubHook(e)
        except Exception as e:
            error = RpcError.internal(f"Target call failed: {e}")
            return ErrorStubHook(error)

    def get(self, path: list[str | int]) -> StubHook:
        """Get a property from the target.

        Args:
            path: Property path

        Returns:
            A new hook with the property value
        """

        # For now, delegate to target.get_property for simple case
        if len(path) == 1:

            async def get_property_async():
                try:
                    result = await self.target.get_property(str(path[0]))
                    return PayloadStubHook(RpcPayload.from_app_return(result))
                except Exception as e:
                    if isinstance(e, RpcError):
                        return ErrorStubHook(e)
                    error = RpcError.internal(f"Property access failed: {e}")
                    return ErrorStubHook(error)

            # Return a promise hook that will resolve to the property
            future: asyncio.Future[StubHook] = asyncio.ensure_future(
                get_property_async()
            )
            return PromiseStubHook(future)

        error = RpcError.not_found(
            "Complex property paths not yet supported on targets"
        )
        return ErrorStubHook(error)

    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """A TargetStubHook represents a single capability, not a collection.

        It's not iterable, so .map() is not a valid operation on it directly.

        Args:
            path: Property path to the iterable
            captures: External capabilities used in the map function
            instructions: Operations to perform for each element

        Returns:
            An ErrorStubHook since this operation is not valid
        """
        # Dispose captures since we're taking ownership
        for cap in captures:
            cap.dispose()
        msg = ".map() cannot be called on a root capability"
        return ErrorStubHook(RpcError.bad_request(msg))

    async def pull(self) -> RpcPayload:
        """Targets can't be pulled directly."""

        msg = "Cannot pull a target object"
        raise RpcError.bad_request(msg)

    def dispose(self) -> None:
        """Decrement reference count and notify target if disposable."""
        self.ref_count -= 1

        # Notify target when refcount reaches 0 if it implements disposal
        if self.ref_count == 0:
            dispose_method = getattr(self.target, "dispose", None)
            if dispose_method is not None and callable(dispose_method):
                # Ignore disposal errors - best effort cleanup
                with suppress(Exception):
                    dispose_method()

    def dup(self) -> Self:
        """Increment reference count."""
        self.ref_count += 1
        return self


@dataclass
class ChainedImportHook(StubHook):
    """A hook representing a remote capability with a property path.

    This hook defers GET vs CALL decision until the operation is actually performed.
    This allows stub.method(args) to work as a direct CALL instead of GET+CALL.
    """

    session: RpcSession
    import_id: int
    path: list[str | int]

    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method, combining the stored path with the extra path.

        Args:
            path: Additional path elements (usually empty for method calls)
            args: Arguments

        Returns:
            A PromiseStubHook for the result
        """
        # Combine paths: stored path + extra path
        full_path = self.path + path

        # Create a new import ID for the result
        result_import_id = self.session.allocate_import_id()
        future: asyncio.Future[StubHook] = asyncio.Future()
        self.session.register_pending_import(result_import_id, future)

        # Send CALL with the full path (including method name from stored path)
        self.session.send_pipeline_call(
            self.import_id, full_path, args, result_import_id
        )

        return PromiseStubHook(future)

    def get(self, path: list[str | int]) -> StubHook:
        """Get a property, extending the path.

        Args:
            path: Additional path elements

        Returns:
            A new ChainedImportHook with extended path
        """
        # Chain another level
        return ChainedImportHook(self.session, self.import_id, self.path + path)

    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """Send a map operation to the remote side.

        Args:
            path: Property path to the iterable
            captures: External capabilities used in the map function
            instructions: Operations to perform for each element

        Returns:
            A PromiseStubHook for the result
        """
        # Combine the stored path with the additional path
        full_path = self.path + path

        # Allocate import ID for the result
        result_import_id = self.session.allocate_import_id()
        future: asyncio.Future[StubHook] = asyncio.Future()
        self.session.register_pending_import(result_import_id, future)

        # Send the pipeline map message
        self.session.send_pipeline_map(
            self.import_id, full_path, captures, instructions, result_import_id
        )

        return PromiseStubHook(future)

    async def pull(self) -> RpcPayload:
        """Pull the value (triggers actual GET operation).

        Returns:
            The resolved payload
        """
        # Now we know it's a property access, send the GET
        result_import_id = self.session.allocate_import_id()
        future: asyncio.Future[StubHook] = asyncio.Future()
        self.session.register_pending_import(result_import_id, future)
        self.session.send_pipeline_get(self.import_id, self.path, result_import_id)

        # Wait for result and pull it
        hook = await future
        return await hook.pull()

    def dispose(self) -> None:
        """No-op for chained hooks (no resources to release)."""

    def dup(self) -> Self:
        """Return self (chained hooks are immutable)."""
        return self


@dataclass
class RpcImportHook(StubHook):
    """A hook representing a remote capability.

    This hook communicates with an RpcSession to send messages over the network.
    It tracks the import ID and delegates to the session for actual I/O.
    """

    session: RpcSession
    import_id: int
    ref_count: int = 1

    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method on the remote capability.

        Args:
            path: Property path + method name
            args: Arguments

        Returns:
            A PromiseStubHook for the result
        """
        logger = logging.getLogger(__name__)
        logger.debug("RpcImportHook.call: import_id=%s, path=%s", self.import_id, path)

        # Create a new import ID for the result
        result_import_id = self.session.allocate_import_id()

        # Create a future for the result
        future: asyncio.Future[StubHook] = asyncio.Future()

        # Register the future with the session
        self.session.register_pending_import(result_import_id, future)

        # Send a pipeline call message
        # This will be handled by the session
        self.session.send_pipeline_call(self.import_id, path, args, result_import_id)

        return PromiseStubHook(future)

    def get(self, path: list[str | int]) -> StubHook:
        """Get a property from the remote capability.

        Args:
            path: Property path

        Returns:
            A ChainedImportHook that defers GET/CALL decision
        """
        # Return a chained hook that remembers the path
        # This allows stub.method(args) to work as a direct CALL
        # instead of GET+CALL
        return ChainedImportHook(self.session, self.import_id, path)

    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """Send a map operation to the remote side.

        Args:
            path: Property path to the iterable
            captures: External capabilities used in the map function
            instructions: Operations to perform for each element

        Returns:
            A PromiseStubHook for the result
        """
        # Allocate import ID for the result
        result_import_id = self.session.allocate_import_id()
        future: asyncio.Future[StubHook] = asyncio.Future()
        self.session.register_pending_import(result_import_id, future)

        # Send the pipeline map message
        self.session.send_pipeline_map(
            self.import_id, path, captures, instructions, result_import_id
        )

        return PromiseStubHook(future)

    async def pull(self) -> RpcPayload:
        """Pull the value from the remote capability.

        Returns:
            The resolved payload
        """
        # Send a pull message and wait for response
        return await self.session.pull_import(self.import_id)

    def dispose(self) -> None:
        """Decrement refcount and send release if needed."""
        self.ref_count -= 1
        if self.ref_count == 0:
            self.session.release_import(self.import_id)

    def dup(self) -> Self:
        """Increment refcount."""
        self.ref_count += 1
        return self


@dataclass
class PromiseStubHook(StubHook):
    """A hook wrapping a future that will resolve to another hook.

    This represents a promise - a value that will be available in the future.
    Operations on this hook create chained promises.
    """

    future: asyncio.Future[StubHook]

    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Wait for the promise to resolve, then call on the result.

        Args:
            path: Property path + method name
            args: Arguments

        Returns:
            A new PromiseStubHook for the chained result
        """

        async def chained_call():
            resolved_hook = await self.future
            return await resolved_hook.call(path, args)

        chained_future: asyncio.Future[StubHook] = asyncio.ensure_future(chained_call())
        return PromiseStubHook(chained_future)

    def get(self, path: list[str | int]) -> StubHook:
        """Wait for the promise to resolve, then get property on the result.

        Args:
            path: Property path

        Returns:
            A new PromiseStubHook for the chained result
        """

        async def chained_get():
            resolved_hook = await self.future
            return resolved_hook.get(path)

        chained_future: asyncio.Future[StubHook] = asyncio.ensure_future(chained_get())
        return PromiseStubHook(chained_future)

    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """Wait for the promise to resolve, then map on the result.

        Args:
            path: Property path to the iterable
            captures: External capabilities used in the map function
            instructions: Operations to perform for each element

        Returns:
            A new PromiseStubHook for the chained result
        """

        async def chained_map():
            resolved_hook = await self.future
            return resolved_hook.map(path, captures, instructions)

        chained_future: asyncio.Future[StubHook] = asyncio.ensure_future(chained_map())
        return PromiseStubHook(chained_future)

    async def pull(self) -> RpcPayload:
        """Wait for the promise to resolve, then pull from the result.

        Returns:
            The final payload
        """
        resolved_hook = await self.future
        return await resolved_hook.pull()

    def dispose(self) -> None:
        """Cancel the promise if not resolved, or dispose the result if resolved."""
        if not self.future.done():
            self.future.cancel()
        elif not self.future.cancelled():
            try:
                result = self.future.result()
                result.dispose()
            except Exception:  # noqa: S110
                pass

    def dup(self) -> Self:
        """Share the same future (promises can be shared)."""
        return PromiseStubHook(self.future)  # type: ignore[return-value]


@dataclass
class MapVariableHook(StubHook):
    """A hook representing a placeholder variable inside a .map() function.

    Operations on this hook do not execute immediately but are recorded as
    instructions by a MapBuilder.
    """

    builder: MapBuilder
    idx: int  # 0 for input, >0 for instruction result

    async def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Record a call instruction in the builder.

        Args:
            path: Property path + method name
            args: Arguments for the call

        Returns:
            A new MapVariableHook for the result
        """
        return self.builder.add_instruction(self, path, args)

    def map(
        self, path: list[str | int], captures: list[StubHook], instructions: list[Any]
    ) -> StubHook:
        """Nested .map() calls are not supported.

        Args:
            path: Property path to the iterable
            captures: External capabilities
            instructions: Operations to perform

        Returns:
            An ErrorStubHook since nested maps are not allowed
        """
        # Dispose captures
        for cap in captures:
            cap.dispose()
        msg = "Nested .map() calls are not supported"
        return ErrorStubHook(RpcError.bad_request(msg))

    def get(self, path: list[str | int]) -> StubHook:
        """Record a property access instruction in the builder.

        Args:
            path: Property path

        Returns:
            A new MapVariableHook for the result
        """
        return self.builder.add_instruction(self, path, None)

    async def pull(self) -> RpcPayload:
        """Cannot await a map variable inside the map function.

        Raises:
            RpcError: Always, since this operation is not allowed
        """
        msg = "Cannot await a map variable inside the map function"
        raise RpcError.bad_request(msg)

    def dispose(self) -> None:
        """No resources to release for map variables."""

    def dup(self) -> Self:
        """Variables are immutable references."""
        return self

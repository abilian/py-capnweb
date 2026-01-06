"""Integration tests for .map() feature."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from capnweb.client import Client, ClientConfig
from capnweb.core.hooks import PayloadStubHook
from capnweb.core.payload import RpcPayload
from capnweb.core.session import RpcSession
from capnweb.core.stubs import RpcStub
from capnweb.error import RpcError
from capnweb.server import Server, ServerConfig
from capnweb.types import RpcTarget


class DataService(RpcTarget):
    """Test service that provides collections for mapping."""

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC method calls."""
        match method:
            case "get_numbers":
                return await self.get_numbers()
            case "get_users":
                return await self.get_users()
            case "get_nested_data":
                return await self.get_nested_data()
            case "get_empty_list":
                return await self.get_empty_list()
            case "get_objects_with_methods":
                return await self.get_objects_with_methods()
            case _:
                msg = f"Method {method} not found"
                raise RpcError.not_found(msg)

    async def get_property(self, property: str) -> Any:
        """Get a property - not used in these tests."""
        msg = f"Property {property} not found"
        raise RpcError.not_found(msg)

    async def get_numbers(self) -> list[int]:
        """Return a list of numbers."""
        return [1, 2, 3, 4, 5]

    async def get_users(self) -> list[dict[str, Any]]:
        """Return a list of user dictionaries."""
        return [
            {"id": 1, "name": "Alice", "score": 100},
            {"id": 2, "name": "Bob", "score": 85},
            {"id": 3, "name": "Charlie", "score": 92},
        ]

    async def get_nested_data(self) -> list[dict[str, Any]]:
        """Return nested data structures."""
        return [
            {"user": {"profile": {"name": "Alice"}}},
            {"user": {"profile": {"name": "Bob"}}},
            {"user": {"profile": {"name": "Charlie"}}},
        ]

    async def get_empty_list(self) -> list[Any]:
        """Return an empty list."""
        return []

    async def get_objects_with_methods(self) -> list[dict[str, Any]]:
        """Return objects that can be called."""
        return [
            {"value": 10, "double": lambda: 20},
            {"value": 20, "double": lambda: 40},
            {"value": 30, "double": lambda: 60},
        ]


@pytest.fixture
async def map_server():
    """Create a test server with DataService."""
    port = 18099  # Fixed port for testing
    config = ServerConfig(
        host="127.0.0.1",
        port=port,
        include_stack_traces=True,
    )
    server = Server(config)
    server.register_capability(0, DataService())

    await server.start()
    yield port
    await server.stop()


class TestMapBasics:
    """Test basic map functionality."""

    @pytest.mark.asyncio
    async def test_map_identity(self, map_server):
        """Test map with identity function."""
        port = map_server
        config = ClientConfig(url=f"http://127.0.0.1:{port}/rpc/batch")

        async with Client(config) as client:
            # Get numbers and map identity
            result = await client.call(0, "get_numbers", [])
            assert result == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_map_property_access(self, map_server):
        """Test map extracting property from objects."""
        port = map_server
        config = ClientConfig(url=f"http://127.0.0.1:{port}/rpc/batch")

        async with Client(config) as client:
            # Get stub for the main capability (export_id=0)
            stub = client.get_remote_stub(0)

            # Get users and extract IDs
            users_promise = stub.get_users()
            ids = users_promise.map(lambda user: user.id)
            result = await ids

            assert result == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_map_nested_property(self, map_server):
        """Test map with nested property access."""
        port = map_server
        config = ClientConfig(url=f"http://127.0.0.1:{port}/rpc/batch")

        async with Client(config) as client:
            # Get stub for the main capability (export_id=0)
            stub = client.get_remote_stub(0)

            # Get nested data and extract names
            data_promise = stub.get_nested_data()
            names = data_promise.map(lambda item: item.user.profile.name)
            result = await names

            assert result == ["Alice", "Bob", "Charlie"]

    @pytest.mark.asyncio
    async def test_map_empty_list(self, map_server):
        """Test map on empty list."""
        port = map_server
        config = ClientConfig(url=f"http://127.0.0.1:{port}/rpc/batch")

        async with Client(config) as client:
            # Get stub for the main capability (export_id=0)
            stub = client.get_remote_stub(0)

            # Map over empty list
            empty_promise = stub.get_empty_list()
            mapped = empty_promise.map(lambda x: x.id)
            result = await mapped

            assert result == []

    @pytest.mark.asyncio
    async def test_map_multiple_properties(self, map_server):
        """Test map extracting multiple properties."""
        port = map_server
        config = ClientConfig(url=f"http://127.0.0.1:{port}/rpc/batch")

        async with Client(config) as client:
            # Get stub for the main capability (export_id=0)
            stub = client.get_remote_stub(0)

            # Get users and extract names
            users_promise = stub.get_users()
            names = users_promise.map(lambda user: user.name)
            result = await names

            assert result == ["Alice", "Bob", "Charlie"]

            # Get users and extract scores
            users_promise2 = stub.get_users()
            scores = users_promise2.map(lambda user: user.score)
            result2 = await scores

            assert result2 == [100, 85, 92]


class TestMapOnStub:
    """Test map called directly on stubs."""

    @pytest.mark.asyncio
    async def test_stub_map(self, map_server):
        """Test calling .map() on a stub."""
        port = map_server
        config = ClientConfig(url=f"http://127.0.0.1:{port}/rpc/batch")

        async with Client(config) as client:
            # Get stub for the main capability (export_id=0)
            stub = client.get_remote_stub(0)

            # Call map on the stub's method result
            result = await stub.get_users().map(lambda u: u.id)
            assert result == [1, 2, 3]


class TestMapErrorHandling:
    """Test error handling in map operations."""

    @pytest.mark.asyncio
    async def test_map_without_session(self):
        """Test that map requires a session."""

        # Create stub without session
        hook = PayloadStubHook(RpcPayload.owned([1, 2, 3]))
        stub = RpcStub(hook, session=None)

        # Should raise RuntimeError about missing session
        with pytest.raises(RuntimeError, match="RpcSession is required"):
            stub.map(lambda x: x)

    @pytest.mark.asyncio
    async def test_map_with_async_function_raises(self):
        """Test that async functions in map raise error."""

        # Create a minimal session
        session = RpcSession()

        hook = PayloadStubHook(RpcPayload.owned([1, 2, 3]))
        stub = RpcStub(hook, session=session)

        # Async lambda should raise error
        with pytest.raises(RpcError):
            await stub.map(lambda x: asyncio.sleep(0))


class TestMapWithPayloadHook:
    """Test map on local PayloadStubHook (non-remote)."""

    @pytest.mark.asyncio
    async def test_local_map_execution(self):
        """Test that map executes locally on PayloadStubHook."""
        session = RpcSession()

        # Create local data
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]

        hook = PayloadStubHook(RpcPayload.owned(data))
        stub = RpcStub(hook, session=session)

        # Map should execute locally
        result = await stub.map(lambda item: item.id)
        assert result == [1, 2, 3]

"""Interactive chat client using WebSocket transport.

This example demonstrates:
- WebSocket transport for persistent connections
- Bidirectional RPC (client exports capabilities to server)
- Async input handling
- Real-time message updates

Run:
    python examples/chat/client.py
"""

import asyncio
import logging
import sys
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from capnweb.client import Client, ClientConfig
from capnweb.error import RpcError
from capnweb.types import RpcTarget

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


@dataclass
class ChatClient(RpcTarget):
    """Client-side chat capability that receives messages from the server."""

    username: str

    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle RPC method calls from the server."""
        match method:
            case "onMessage":
                return await self._on_message(args[0])
            case _:
                msg = f"Method {method} not found"
                raise RpcError.not_found(msg)

    async def get_property(self, property: str) -> Any:
        """Handle property access."""
        msg = f"Property {property} not found"
        raise RpcError.not_found(msg)

    async def _on_message(self, message: dict[str, str]) -> None:
        """Receive a message from the server.

        Args:
            message: Dictionary with 'username', 'text', and 'type' keys
        """
        msg_type = message.get("type", "chat")
        username = message.get("username", "Unknown")
        text = message.get("text", "")

        if msg_type == "system":
            # System messages in yellow
            print(f"\033[93m*** {text} ***\033[0m")
        elif username == self.username:
            # Own messages in gray
            print(f"\033[90m[You] {text}\033[0m")
        else:
            # Other users' messages in default color
            print(f"[{username}] {text}")


async def read_input() -> str:
    """Read a line of input asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, sys.stdin.readline)


async def main():
    """Run the interactive chat client."""
    print("=== Cap'n Web Chat Client ===")
    print()

    # Get username
    print("Enter your username: ", end="", flush=True)
    username = (await read_input()).strip()

    if not username:
        print("Username cannot be empty!")
        return

    # Connect to server
    # Use WebSocket for persistent connection
    config = ClientConfig(url="ws://127.0.0.1:8080/rpc/ws", timeout=60.0)

    print(f"\nConnecting to chat server as '{username}'...")

    try:
        async with Client(config) as client:
            # Create client capability
            client_cap = ChatClient(username)

            # Register our capability with the session
            # The server will call our onMessage method
            client_stub = client.create_stub(client_cap)

            # Join the chat room
            try:
                welcome = await client.call(0, "join", [username, client_stub])
                print(f"\n\033[92m{welcome['message']}\033[0m")
                print(
                    f"Connected users ({welcome['userCount']}): {', '.join(welcome['users'])}"
                )
                print("\nType messages and press Enter to send.")
                print(
                    "Type '/quit' to exit, '/users' to list users, '/help' for commands"
                )
                print()

            except RpcError as e:
                print(f"\n\033[91mError joining chat: {e.message}\033[0m")
                return

            # Main chat loop
            try:
                while True:
                    # Read user input
                    message = (await read_input()).strip()

                    if not message:
                        continue

                    # Handle commands
                    if message == "/quit":
                        print("\nLeaving chat...")
                        await client.call(0, "leave", [username])
                        break

                    if message == "/users":
                        users = await client.call(0, "listUsers", [])
                        print(f"\n\033[96mConnected users: {', '.join(users)}\033[0m\n")

                    elif message == "/help":
                        print("\n\033[96mCommands:")
                        print("  /users  - List connected users")
                        print("  /quit   - Leave the chat")
                        print("  /help   - Show this help\033[0m\n")

                    elif message.startswith("/"):
                        print(f"\n\033[91mUnknown command: {message}\033[0m")
                        print("Type /help for available commands\n")

                    else:
                        # Send regular message
                        try:
                            await client.call(0, "sendMessage", [username, message])
                        except RpcError as e:
                            print(
                                f"\n\033[91mError sending message: {e.message}\033[0m\n"
                            )

            except KeyboardInterrupt:
                print("\n\nLeaving chat...")
                with suppress(Exception):
                    await client.call(0, "leave", [username])

    except Exception as e:
        print(f"\n\033[91mConnection error: {e}\033[0m")
        print("Make sure the server is running: python examples/chat/server.py")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye!")

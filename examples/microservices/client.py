"""Microservices demo client - Interacts with the API gateway.

This example demonstrates:
- Multi-service architecture with API gateway
- Authentication flow across services
- Capability-based authorization
- Service mesh communication

Run (after starting all services):
    python examples/microservices/client.py
"""

import asyncio
import logging

from capnweb.client import Client, ClientConfig
from capnweb.error import RpcError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Run the microservices demo client."""
    # Connect to API Gateway
    config = ClientConfig(url="http://127.0.0.1:8080/rpc/batch")

    print("=== Microservices Demo ===\n")

    async with Client(config) as client:
        # ============================================================
        # 1. Login as regular user (Bob)
        # ============================================================
        print("1. Login as Bob (regular user)")
        try:
            login_result = await client.call(0, "login", ["bob", "password"])
            bob_token = login_result["token"]
            print(
                f"   ✓ Logged in as {login_result['username']} (role: {login_result['role']})"
            )
            print(f"   Token: {bob_token}\n")
        except RpcError as e:
            print(f"   ✗ Login failed: {e.message}\n")
            return

        # ============================================================
        # 2. Get user profile
        # ============================================================
        print("2. Get Bob's profile")
        try:
            profile = await client.call(0, "getUserProfile", [bob_token])
            print(f"   User ID: {profile['userId']}")
            print(f"   Username: {profile['username']}")
            print(f"   Email: {profile['email']}")
            print(f"   Role: {profile['role']}\n")
        except RpcError as e:
            print(f"   ✗ Get profile failed: {e.message}\n")

        # ============================================================
        # 3. List all users
        # ============================================================
        print("3. List all users")
        try:
            users = await client.call(0, "listUsers", [bob_token])
            print(f"   Found {len(users)} users:")
            for user in users:
                print(
                    f"     - {user['username']} (id: {user['id']}, role: {user['role']})"
                )
            print()
        except RpcError as e:
            print(f"   ✗ List users failed: {e.message}\n")

        # ============================================================
        # 4. Create an order as Bob
        # ============================================================
        print("4. Create an order as Bob")
        items = [
            {"name": "Laptop", "price": 999.99, "quantity": 1},
            {"name": "Mouse", "price": 29.99, "quantity": 2},
        ]
        try:
            order_result = await client.call(0, "createOrder", [bob_token, items])
            bob_order_id = order_result["orderId"]
            print(f"   ✓ Order created: {bob_order_id}")
            print(f"   Total: ${order_result['total']:.2f}")
            print(f"   Status: {order_result['status']}")
            print(f"   Items: {len(order_result['items'])}\n")
        except RpcError as e:
            print(f"   ✗ Create order failed: {e.message}\n")
            return

        # ============================================================
        # 5. List Bob's orders
        # ============================================================
        print("5. List Bob's orders")
        try:
            orders = await client.call(0, "listOrders", [bob_token])
            print(f"   Found {len(orders)} order(s):")
            for order in orders:
                print(
                    f"     - {order['orderId']}: ${order['total']:.2f} ({order['status']})"
                )
            print()
        except RpcError as e:
            print(f"   ✗ List orders failed: {e.message}\n")

        # ============================================================
        # 6. Try to cancel order as Bob (should fail - no permission)
        # ============================================================
        print("6. Try to cancel order as Bob (should fail)")
        try:
            await client.call(0, "cancelOrder", [bob_token, bob_order_id])
            print("   ✗ Unexpected success - Bob shouldn't be able to cancel orders!\n")
        except RpcError as e:
            print(f"   ✓ Expected failure: {e.message}\n")

        # ============================================================
        # 7. Login as admin (Alice)
        # ============================================================
        print("7. Login as Alice (admin)")
        try:
            login_result = await client.call(0, "login", ["alice", "password"])
            alice_token = login_result["token"]
            print(
                f"   ✓ Logged in as {login_result['username']} (role: {login_result['role']})"
            )
            print(f"   Token: {alice_token}\n")
        except RpcError as e:
            print(f"   ✗ Login failed: {e.message}\n")
            return

        # ============================================================
        # 8. Alice creates an order
        # ============================================================
        print("8. Create an order as Alice")
        items = [
            {"name": "Server", "price": 4999.99, "quantity": 1},
        ]
        try:
            order_result = await client.call(0, "createOrder", [alice_token, items])
            alice_order_id = order_result["orderId"]
            print(f"   ✓ Order created: {alice_order_id}")
            print(f"   Total: ${order_result['total']:.2f}\n")
        except RpcError as e:
            print(f"   ✗ Create order failed: {e.message}\n")

        # ============================================================
        # 9. Alice cancels Bob's order (should succeed - admin permission)
        # ============================================================
        print("9. Alice cancels Bob's order (should succeed)")
        try:
            result = await client.call(0, "cancelOrder", [alice_token, bob_order_id])
            print(f"   ✓ Order cancelled: {result['orderId']}")
            print(f"   Status: {result['status']}\n")
        except RpcError as e:
            print(f"   ✗ Cancel failed: {e.message}\n")

        # ============================================================
        # 10. Verify order was cancelled
        # ============================================================
        print("10. Verify Bob's order was cancelled")
        try:
            orders = await client.call(0, "listOrders", [bob_token])
            for order in orders:
                if order["orderId"] == bob_order_id:
                    print(f"   Order {bob_order_id} status: {order['status']}")
                    if order["status"] == "cancelled":
                        print("   ✓ Order successfully cancelled\n")
                    else:
                        print(f"   ✗ Expected cancelled, got {order['status']}\n")
                    break
        except RpcError as e:
            print(f"   ✗ List orders failed: {e.message}\n")

        print("=== Demo Complete ===")
        print("\nThis demo showed:")
        print("  • API Gateway routing requests to backend services")
        print("  • User authentication via User Service")
        print("  • Cross-service capability passing (User → Order Service)")
        print("  • Permission-based access control (admin can cancel, user cannot)")
        print("  • Service mesh architecture with Cap'n Web")


if __name__ == "__main__":
    asyncio.run(main())

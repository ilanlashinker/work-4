#include <iostream>
#include "EventBus.h"
#include "Publisher.h"
#include "Subscriber.h"
#include "Tests.h"

int main() {
    std::cout << "====================================================\n";
    std::cout << "   Event-Driven Architecture – Pub/Sub Demo\n";
    std::cout << "====================================================\n\n";

    // -------------------------------------------------------
    // 1. Setup: bus, partitions, publishers, subscribers
    // -------------------------------------------------------
    EventBus bus;

    // Two partitions (capacity=6 so we can demo circular-buffer overwrite during demo)
    bus.createPartition("orders",    6);
    bus.createPartition("inventory", 10);

    // Two publishers
    Publisher orderPub("OrderService",     bus);
    Publisher inventoryPub("InventoryService", bus);

    // Three subscribers
    Subscriber subCreated ("OrderCreatedHandler");
    Subscriber subApproved("OrderApprovedHandler");
    Subscriber subInventory("InventoryHandler");

    bus.registerSubscriber(&subCreated);
    bus.registerSubscriber(&subApproved);
    bus.registerSubscriber(&subInventory);

    // Subscriptions: each subscriber registers to one partition + one event type
    bus.subscribe("OrderCreatedHandler",  "orders",    "ORDER_CREATED");
    bus.subscribe("OrderApprovedHandler", "orders",    "ORDER_APPROVED");
    bus.subscribe("InventoryHandler",     "inventory", "STOCK_UPDATE");

    // -------------------------------------------------------
    // 2. Publish events (>= 10 total)
    // -------------------------------------------------------
    std::cout << "\n--- Publishing Events ---\n";

    // 7 events to 'orders' (capacity=6 → event #0 gets overwritten by #6)
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1001 – Alice");
    orderPub.publish("ORDER_APPROVED", "orders", "Order #1001 approved");
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1002 – Bob");
    orderPub.publish("ORDER_APPROVED", "orders", "Order #1002 approved");
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1003 – Carol");
    orderPub.publish("ORDER_APPROVED", "orders", "Order #1003 approved");
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1004 – Dave"); // overwrites #0

    // 4 events to 'inventory'
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product A: 100 units");
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product B:  50 units");
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product C: 200 units");
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product A:  80 units");

    // -------------------------------------------------------
    // 3. Consume events
    // -------------------------------------------------------
    std::cout << "\n--- Consuming Events ---\n";

    // OrderCreatedHandler: subscribes to ORDER_CREATED.
    // Event #0 ("Order #1001") was overwritten – bus will warn and skip to oldest=#1.
    std::cout << "\n[OrderCreatedHandler consuming from 'orders']:\n";
    for (int i = 0; i < 4; i++) bus.consume("OrderCreatedHandler", "orders");

    std::cout << "\n[OrderApprovedHandler consuming from 'orders']:\n";
    for (int i = 0; i < 3; i++) bus.consume("OrderApprovedHandler", "orders");

    std::cout << "\n[InventoryHandler consuming from 'inventory']:\n";
    for (int i = 0; i < 4; i++) bus.consume("InventoryHandler", "inventory");

    // -------------------------------------------------------
    // 4. Rewind demo
    // -------------------------------------------------------
    std::cout << "\n--- Rewind Demo ---\n";

    // Rewind OrderCreatedHandler by 1 step and re-consume
    std::string result = bus.rewind("OrderCreatedHandler", "orders", 1);
    if (result.empty()) {
        std::cout << "[OrderCreatedHandler re-consuming after rewind]:\n";
        bus.consume("OrderCreatedHandler", "orders");
    } else {
        std::cout << result << "\n";
    }

    // Attempt an illegal rewind (too many steps back – events overwritten)
    std::cout << "\n[Attempting illegal rewind of 10 steps]:\n";
    result = bus.rewind("OrderCreatedHandler", "orders", 10);
    if (!result.empty())
        std::cout << result << "\n";

    // Rewind InventoryHandler by 2 steps, then replay
    std::cout << "\n[InventoryHandler rewinding 2 steps]:\n";
    result = bus.rewind("InventoryHandler", "inventory", 2);
    if (result.empty()) {
        bus.consume("InventoryHandler", "inventory");
        bus.consume("InventoryHandler", "inventory");
    }

    // -------------------------------------------------------
    // 5. Stats & tests
    // -------------------------------------------------------
    bus.printStats();

    Tests::runAll();

    return 0;
}

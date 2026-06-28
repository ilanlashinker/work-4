#include <iostream>
#include "EventBus.h"
#include "Publisher.h"
#include "Subscriber.h"
#include "Tests.h"

int main() {
    std::cout << "====================================================\n";
    std::cout << "   Event-Driven Architecture – Pub/Sub Demo\n";
    std::cout << "====================================================\n\n";

    // step 1: create everything
    EventBus bus;

    // orders is small on purpose so we can see old events get replaced
    bus.createPartition("orders",    6);
    bus.createPartition("inventory", 10);

    // two publishers
    Publisher orderPub("OrderService",     bus);
    Publisher inventoryPub("InventoryService", bus);

    // three subscribers
    Subscriber subCreated ("OrderCreatedHandler");
    Subscriber subApproved("OrderApprovedHandler");
    Subscriber subInventory("InventoryHandler");

    bus.registerSubscriber(&subCreated);
    bus.registerSubscriber(&subApproved);
    bus.registerSubscriber(&subInventory);

    // connect each subscriber to the right partition and event type
    bus.subscribe("OrderCreatedHandler",  "orders",    "ORDER_CREATED");
    bus.subscribe("OrderApprovedHandler", "orders",    "ORDER_APPROVED");
    bus.subscribe("InventoryHandler",     "inventory", "STOCK_UPDATE");

    // step 2: publish events
    std::cout << "\n--- Publishing Events ---\n";

    // 7 events to orders, but it only holds 6 so the first one gets replaced
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1001 – Alice");
    orderPub.publish("ORDER_APPROVED", "orders", "Order #1001 approved");
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1002 – Bob");
    orderPub.publish("ORDER_APPROVED", "orders", "Order #1002 approved");
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1003 – Carol");
    orderPub.publish("ORDER_APPROVED", "orders", "Order #1003 approved");
    orderPub.publish("ORDER_CREATED",  "orders", "Order #1004 – Dave"); // this one replaces event #0

    // 4 events to inventory
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product A: 100 units");
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product B:  50 units");
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product C: 200 units");
    inventoryPub.publish("STOCK_UPDATE", "inventory", "Product A:  80 units");

    // step 3: read events
    std::cout << "\n--- Consuming Events ---\n";

    // event 0 was replaced so the bus will skip it and start from the next one
    std::cout << "\n[OrderCreatedHandler consuming from 'orders']:\n";
    for (int i = 0; i < 4; i++) bus.consume("OrderCreatedHandler", "orders");

    std::cout << "\n[OrderApprovedHandler consuming from 'orders']:\n";
    for (int i = 0; i < 3; i++) bus.consume("OrderApprovedHandler", "orders");

    std::cout << "\n[InventoryHandler consuming from 'inventory']:\n";
    for (int i = 0; i < 4; i++) bus.consume("InventoryHandler", "inventory");

    // step 4: try the rewind feature
    std::cout << "\n--- Rewind Demo ---\n";

    // go back 1 step and read the last event again
    std::string result = bus.rewind("OrderCreatedHandler", "orders", 1);
    if (result.empty()) {
        std::cout << "[OrderCreatedHandler re-consuming after rewind]:\n";
        bus.consume("OrderCreatedHandler", "orders");
    } else {
        std::cout << result << "\n";
    }

    // this should fail because we're going too far back
    std::cout << "\n[Attempting illegal rewind of 10 steps]:\n";
    result = bus.rewind("OrderCreatedHandler", "orders", 10);
    if (!result.empty())
        std::cout << result << "\n";

    // go back 2 steps and read those events again
    std::cout << "\n[InventoryHandler rewinding 2 steps]:\n";
    result = bus.rewind("InventoryHandler", "inventory", 2);
    if (result.empty()) {
        bus.consume("InventoryHandler", "inventory");
        bus.consume("InventoryHandler", "inventory");
    }

    // step 5: show stats and run the tests
    bus.printStats();

    Tests::runAll();

    return 0;
}

#include "Tests.h"
#include "EventBus.h"
#include "Subscriber.h"
#include <iostream>

static void sep(int n, const std::string& title) {
    std::cout << "\n--- Test " << n << ": " << title << " ---\n";
}

void Tests::report(int num, const std::string& desc, bool passed) {
    std::cout << (passed ? "[PASS]" : "[FAIL]")
              << " Test " << num << ": " << desc << "\n";
}

// -------------------------------------------------------------------
// Test 1 – subscriber receives only events from its subscribed partition
// -------------------------------------------------------------------
void Tests::test1_partitionIsolation() {
    sep(1, "Partition isolation");

    EventBus bus;
    bus.createPartition("P1", 10);
    bus.createPartition("P2", 10);

    Subscriber subA("SubA");
    bus.registerSubscriber(&subA);
    bus.subscribe("SubA", "P1", "ORDER"); // subscribed ONLY to P1

    // Publish one event to each partition
    bus.publish("ORDER", "P1", "from-P1");
    bus.publish("ORDER", "P2", "from-P2");

    bus.consume("SubA", "P1"); // SubA pulls from P1

    bool passed = subA.getReceivedEvents().size() == 1 &&
                  subA.getReceivedEvents()[0].partitionName == "P1";
    report(1, "Subscriber receives only events from subscribed partition", passed);
}

// -------------------------------------------------------------------
// Test 2 – subscriber receives only the event type it registered for
// -------------------------------------------------------------------
void Tests::test2_eventTypeFilter() {
    sep(2, "Event-type filter");

    EventBus bus;
    bus.createPartition("orders", 10);

    Subscriber subB("SubB");
    bus.registerSubscriber(&subB);
    bus.subscribe("SubB", "orders", "ORDER_CREATED");

    bus.publish("ORDER_CREATED",  "orders", "c1");
    bus.publish("ORDER_APPROVED", "orders", "a1");
    bus.publish("ORDER_CREATED",  "orders", "c2");
    bus.publish("ORDER_APPROVED", "orders", "a2");

    // Pull 4 times – should deliver only 2 (the ORDER_CREATED events)
    for (int i = 0; i < 4; i++) bus.consume("SubB", "orders");

    bool passed = subB.getReceivedEvents().size() == 2;
    for (const auto& e : subB.getReceivedEvents())
        if (e.type != "ORDER_CREATED") { passed = false; break; }
    report(2, "Subscriber receives only subscribed event type", passed);
}

// -------------------------------------------------------------------
// Test 3 – consumption pointer advances after each consume
// -------------------------------------------------------------------
void Tests::test3_pointerAdvances() {
    sep(3, "Pointer advances");

    EventBus bus;
    bus.createPartition("t3", 10);

    Subscriber sub("Sub3");
    bus.registerSubscriber(&sub);
    bus.subscribe("Sub3", "t3", ""); // all types

    bus.publish("EV", "t3", "a");
    bus.publish("EV", "t3", "b");

    auto* info   = bus.getSubscriptionInfo("Sub3", "t3");
    int   before = info->consumptionPointer;

    bus.consume("Sub3", "t3");

    int after = info->consumptionPointer;
    report(3, "Consumption pointer advances after consuming", after > before);
}

// -------------------------------------------------------------------
// Test 4 – circular buffer overwrites oldest when full
// -------------------------------------------------------------------
void Tests::test4_circularBufferOverwrite() {
    sep(4, "Circular buffer overwrite");

    EventBus bus;
    bus.createPartition("small", 3); // capacity = 3

    // Publish 5 events: IDs 0-4 → only IDs 2,3,4 remain
    for (int i = 0; i < 5; i++)
        bus.publish("EV", "small", "ev" + std::to_string(i));

    Partition* p = bus.getPartition("small");

    bool passed = p->getOldestAvailableId() == 2 &&
                  p->getNewestAvailableId() == 4 &&
                  !p->isAvailable(0) &&
                  !p->isAvailable(1) &&
                   p->isAvailable(2);
    report(4, "Circular buffer overwrites old events when full", passed);
}

// -------------------------------------------------------------------
// Test 5 – rewind moves pointer back; subscriber re-consumes the event
// -------------------------------------------------------------------
void Tests::test5_rewindWorks() {
    sep(5, "Rewind works");

    EventBus bus;
    bus.createPartition("rw", 10);

    Subscriber sub("SubRw");
    bus.registerSubscriber(&sub);
    bus.subscribe("SubRw", "rw", "EV");

    bus.publish("EV", "rw", "first");   // id=0
    bus.publish("EV", "rw", "second");  // id=1

    bus.consume("SubRw", "rw"); // gets "first",  pointer→1
    bus.consume("SubRw", "rw"); // gets "second", pointer→2

    std::string err = bus.rewind("SubRw", "rw", 1); // pointer→1
    bus.consume("SubRw", "rw");                       // gets "second" again

    bool passed = err.empty() &&
                  sub.getReceivedEvents().size() == 3 &&
                  sub.getReceivedEvents().back().data == "second";
    report(5, "Rewind returns subscriber to previous event", passed);
}

// -------------------------------------------------------------------
// Test 6 – rewinding to an overwritten event returns an error
// -------------------------------------------------------------------
void Tests::test6_rewindToOverwritten() {
    sep(6, "Rewind to overwritten event");

    EventBus bus;
    bus.createPartition("tiny", 3); // capacity = 3

    Subscriber sub("SubOvw");
    bus.registerSubscriber(&sub);
    bus.subscribe("SubOvw", "tiny", "EV"); // pointer starts at 0

    // Publish 5 events: IDs 0-4.  IDs 0 & 1 are now overwritten.
    for (int i = 0; i < 5; i++)
        bus.publish("EV", "tiny", "ev" + std::to_string(i));

    // Consume the 3 available events (auto-advances past #0 and #1 internally).
    bus.consume("SubOvw", "tiny"); // gets #2, pointer→3
    bus.consume("SubOvw", "tiny"); // gets #3, pointer→4
    bus.consume("SubOvw", "tiny"); // gets #4, pointer→5

    // Request rewind 4 steps → target #1, which is overwritten.
    std::string err = bus.rewind("SubOvw", "tiny", 4);

    bool passed = !err.empty(); // must return a non-empty error string
    std::cout << "  Error message: " << err << "\n";
    report(6, "Rewind to overwritten event returns error", passed);
}

// -------------------------------------------------------------------
// Test 7 – multiple subscribers on the same partition are independent
// -------------------------------------------------------------------
void Tests::test7_multipleSubscribersIndependent() {
    sep(7, "Multiple subscribers independent");

    EventBus bus;
    bus.createPartition("shared", 10);

    Subscriber subX("SubX"), subY("SubY");
    bus.registerSubscriber(&subX);
    bus.registerSubscriber(&subY);
    bus.subscribe("SubX", "shared", "MSG");
    bus.subscribe("SubY", "shared", "MSG");

    bus.publish("MSG", "shared", "hello"); // id=0
    bus.publish("MSG", "shared", "world"); // id=1

    // SubX consumes both; SubY consumes only one.
    bus.consume("SubX", "shared");
    bus.consume("SubX", "shared");
    bus.consume("SubY", "shared");

    auto* infoX = bus.getSubscriptionInfo("SubX", "shared");
    auto* infoY = bus.getSubscriptionInfo("SubY", "shared");

    bool passed = subX.getReceivedEvents().size() == 2 &&
                  subY.getReceivedEvents().size() == 1 &&
                  infoX->consumptionPointer != infoY->consumptionPointer;
    report(7, "Multiple subscribers consume from same partition independently", passed);
}

// -------------------------------------------------------------------

void Tests::runAll() {
    std::cout << "\n\n========== RUNNING ALL TESTS ==========\n";
    test1_partitionIsolation();
    test2_eventTypeFilter();
    test3_pointerAdvances();
    test4_circularBufferOverwrite();
    test5_rewindWorks();
    test6_rewindToOverwritten();
    test7_multipleSubscribersIndependent();
    std::cout << "========================================\n\n";
}

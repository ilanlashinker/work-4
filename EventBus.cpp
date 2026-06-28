#include "EventBus.h"
#include <iostream>
#include <algorithm>

// setup functions

void EventBus::createPartition(const std::string& name, int capacity) {
    if (partitions_.count(name)) {
        std::cerr << "[BUS][ERROR] Partition '" << name << "' already exists.\n";
        return;
    }
    partitions_[name] = std::make_unique<Partition>(name, capacity);
    std::cout << "[BUS] Created partition '" << name
              << "' (capacity=" << capacity << ")\n";
}

void EventBus::registerSubscriber(ISubscriber* subscriber) {
    subscribers_[subscriber->getName()] = subscriber;
    std::cout << "[BUS] Registered subscriber '" << subscriber->getName() << "'\n";
}

bool EventBus::subscribe(const std::string& subscriberName,
                          const std::string& partitionName,
                          const std::string& eventType) {
    if (!partitions_.count(partitionName)) {
        std::cerr << "[BUS][ERROR] Partition '" << partitionName << "' does not exist.\n";
        return false;
    }
    if (!subscribers_.count(subscriberName)) {
        std::cerr << "[BUS][ERROR] Subscriber '" << subscriberName << "' not registered.\n";
        return false;
    }

    SubscriptionInfo info;
    info.subscriberName      = subscriberName;
    info.partitionName       = partitionName;
    info.eventType           = eventType;
    // start reading from the next event that gets published so we don't miss anything
    info.consumptionPointer  = partitions_[partitionName]->getNextGlobalId();

    subscriptions_[partitionName][subscriberName] = info;

    std::cout << "[BUS] '" << subscriberName << "' subscribed to ["
              << (eventType.empty() ? "*" : eventType)
              << "] in partition '" << partitionName
              << "' (initial pointer=" << info.consumptionPointer << ")\n";
    return true;
}

// main functions

int EventBus::publish(const std::string& eventType,
                       const std::string& partitionName,
                       const std::string& data) {
    auto it = partitions_.find(partitionName);
    if (it == partitions_.end()) {
        std::cerr << "[BUS][ERROR] Partition '" << partitionName << "' does not exist.\n";
        return -1;
    }

    int id = it->second->publish(eventType, data);
    std::cout << "[PUBLISH] #" << id << " [" << eventType << "] -> '"
              << partitionName << "': \"" << data << "\"\n";
    return id;
}

bool EventBus::consume(const std::string& subscriberName,
                        const std::string& partitionName) {
    auto pit = partitions_.find(partitionName);
    if (pit == partitions_.end()) {
        std::cerr << "[BUS][ERROR] Partition '" << partitionName << "' does not exist.\n";
        return false;
    }

    auto& partSubs = subscriptions_[partitionName];
    auto  sit      = partSubs.find(subscriberName);
    if (sit == partSubs.end()) {
        std::cerr << "[BUS][ERROR] '" << subscriberName
                  << "' is not subscribed to partition '" << partitionName << "'.\n";
        return false;
    }

    SubscriptionInfo& info      = sit->second;
    Partition*        partition = pit->second.get();

    while (info.consumptionPointer <= partition->getNewestAvailableId()) {
        int ptr = info.consumptionPointer;

        // this event got replaced by a newer one before the subscriber could read it
        if (!partition->isAvailable(ptr)) {
            int newPtr = partition->getOldestAvailableId();
            std::cerr << "[BUS][WARN] Event #" << ptr << " was overwritten in '"
                      << partitionName << "'. Advancing pointer from "
                      << ptr << " to oldest available #" << newPtr << ".\n";
            info.consumptionPointer = newPtr;
            continue;
        }

        Event evt = partition->getEvent(ptr);
        info.consumptionPointer++; // always move forward, even if we end up skipping this event

        if (info.eventType.empty() || evt.type == info.eventType) {
            subscribers_[subscriberName]->onEvent(evt);
            std::cout << "  [POINTER] '" << subscriberName << "' in '"
                      << partitionName << "' -> " << info.consumptionPointer << "\n";
            return true;
        }
        // wrong type, just skip it
    }

    std::cout << "[BUS] No new matching events for '" << subscriberName
              << "' in partition '" << partitionName << "'\n";
    return false;
}

std::string EventBus::rewind(const std::string& subscriberName,
                               const std::string& partitionName,
                               int steps) {
    auto pit = partitions_.find(partitionName);
    if (pit == partitions_.end())
        return "ERROR: Partition '" + partitionName + "' does not exist.";

    auto& partSubs = subscriptions_[partitionName];
    auto  sit      = partSubs.find(subscriberName);
    if (sit == partSubs.end())
        return "ERROR: '" + subscriberName + "' is not subscribed to '"
               + partitionName + "'.";

    if (steps <= 0)
        return "ERROR: Rewind steps must be a positive integer.";

    SubscriptionInfo& info      = sit->second;
    Partition*        partition = pit->second.get();
    int               newPtr    = info.consumptionPointer - steps;

    if (newPtr < 0)
        return "ERROR: Rewind would go to a negative event ID (requested #"
               + std::to_string(newPtr) + ").";

    int oldest = partition->getOldestAvailableId();
    if (newPtr < oldest)
        return "ERROR: Cannot rewind " + std::to_string(steps) + " step(s) for '"
               + subscriberName + "' in '" + partitionName
               + "'. Requested event #" + std::to_string(newPtr)
               + " has been overwritten. Oldest available: #"
               + std::to_string(oldest) + ".";

    std::cout << "[REWIND] '" << subscriberName << "' in '" << partitionName
              << "': pointer " << info.consumptionPointer
              << " -> " << newPtr
              << " (rewound " << steps << " step(s))\n";

    info.consumptionPointer = newPtr;
    return ""; // all good
}

// helper functions

Partition* EventBus::getPartition(const std::string& name) {
    auto it = partitions_.find(name);
    return it != partitions_.end() ? it->second.get() : nullptr;
}

SubscriptionInfo* EventBus::getSubscriptionInfo(const std::string& subscriberName,
                                                  const std::string& partitionName) {
    auto pit = subscriptions_.find(partitionName);
    if (pit == subscriptions_.end()) return nullptr;
    auto sit = pit->second.find(subscriberName);
    return sit != pit->second.end() ? &sit->second : nullptr;
}

void EventBus::printStats() const {
    std::cout << "\n========== Partition Stats ==========\n";
    for (const auto& [pName, part] : partitions_) {
        int total  = part->getNextGlobalId();
        int oldest = part->getOldestAvailableId();
        int newest = part->getNewestAvailableId();
        std::cout << "  Partition '" << pName
                  << "' capacity=" << part->getCapacity()
                  << " total-published=" << total;
        if (total > 0)
            std::cout << " available=[#" << oldest << "..#" << newest << "]";
        else
            std::cout << " (empty)";
        std::cout << "\n";

        auto pit = subscriptions_.find(pName);
        if (pit != subscriptions_.end()) {
            for (const auto& [sName, info] : pit->second) {
                std::cout << "    subscriber='" << sName
                          << "' type=[" << (info.eventType.empty() ? "*" : info.eventType)
                          << "] pointer=" << info.consumptionPointer << "\n";
            }
        }
    }
    std::cout << "=====================================\n\n";
}

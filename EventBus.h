#pragma once
#include <map>
#include <string>
#include <memory>
#include "Partition.h"
#include "ISubscriber.h"

// Per-subscription state: which partition/type the subscriber tracks
// and where its consumption pointer currently sits.
struct SubscriptionInfo {
    std::string subscriberName;
    std::string partitionName;
    std::string eventType;        // empty = accept all types
    int         consumptionPointer; // next globalId to try consuming
};

// Central event bus: owns partitions, routes publish calls,
// tracks per-subscriber consumption pointers, and handles rewind.
class EventBus {
public:
    // --- Setup ---
    void createPartition(const std::string& name, int capacity);
    void registerSubscriber(ISubscriber* subscriber);

    // Subscribe a registered subscriber to a specific event type within a partition.
    // Pass empty string for eventType to receive all types.
    bool subscribe(const std::string& subscriberName,
                   const std::string& partitionName,
                   const std::string& eventType);

    // --- Operations ---

    // Publish an event; returns the globalId assigned (-1 on error).
    int publish(const std::string& eventType,
                const std::string& partitionName,
                const std::string& data);

    // Pull the next matching event for the subscriber and deliver it via onEvent().
    // Skips events of other types and auto-advances past overwritten events.
    // Returns true if an event was delivered.
    bool consume(const std::string& subscriberName,
                 const std::string& partitionName);

    // Move the subscriber's pointer back by `steps` positions.
    // Returns an empty string on success, or a descriptive error message on failure.
    std::string rewind(const std::string& subscriberName,
                       const std::string& partitionName,
                       int steps);

    // --- Inspection (used by tests) ---
    Partition*       getPartition(const std::string& name);
    SubscriptionInfo* getSubscriptionInfo(const std::string& subscriberName,
                                          const std::string& partitionName);

    void printStats() const;

private:
    std::map<std::string, std::unique_ptr<Partition>> partitions_;
    std::map<std::string, ISubscriber*>               subscribers_;
    // subscriptions_[partitionName][subscriberName]
    std::map<std::string, std::map<std::string, SubscriptionInfo>> subscriptions_;
};

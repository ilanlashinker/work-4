#pragma once
#include <map>
#include <string>
#include <memory>
#include "Partition.h"
#include "ISubscriber.h"

// saves info about one subscriber in one partition - what type they want and where they stopped reading
struct SubscriptionInfo {
    std::string subscriberName;
    std::string partitionName;
    std::string eventType;        // if empty, the subscriber gets all event types
    int         consumptionPointer; // the next event number this subscriber should read
};

// the main class that connects publishers and subscribers and manages everything
class EventBus {
public:
    // setup functions
    void createPartition(const std::string& name, int capacity);
    void registerSubscriber(ISubscriber* subscriber);

    // sign up a subscriber to a partition and event type. use "" to get all types
    bool subscribe(const std::string& subscriberName,
                   const std::string& partitionName,
                   const std::string& eventType);

    // main actions

    // sends an event to a partition and returns the event number (-1 if something went wrong)
    int publish(const std::string& eventType,
                const std::string& partitionName,
                const std::string& data);

    // reads the next event for this subscriber, skips wrong types and replaced events
    // returns true if we found and delivered an event
    bool consume(const std::string& subscriberName,
                 const std::string& partitionName);

    // go back N steps. returns "" if it worked, or an error message if we can't
    std::string rewind(const std::string& subscriberName,
                       const std::string& partitionName,
                       int steps);

    // helper functions used by the tests
    Partition*       getPartition(const std::string& name);
    SubscriptionInfo* getSubscriptionInfo(const std::string& subscriberName,
                                          const std::string& partitionName);

    void printStats() const;

private:
    std::map<std::string, std::unique_ptr<Partition>> partitions_;
    std::map<std::string, ISubscriber*>               subscribers_;
    // first key is partition name, second key is subscriber name
    std::map<std::string, std::map<std::string, SubscriptionInfo>> subscriptions_;
};

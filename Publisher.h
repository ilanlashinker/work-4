#pragma once
#include <string>

class EventBus; // we need this so we can use EventBus without including the whole file

class Publisher {
public:
    Publisher(std::string name, EventBus& bus);

    const std::string& getName() const { return name_; }

    // sends an event to the bus, returns the event number or -1 if something went wrong
    int publish(const std::string& eventType,
                const std::string& partitionName,
                const std::string& data);

private:
    std::string name_;
    EventBus&   bus_;
};

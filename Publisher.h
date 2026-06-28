#pragma once
#include <string>

class EventBus; // forward declaration

class Publisher {
public:
    Publisher(std::string name, EventBus& bus);

    const std::string& getName() const { return name_; }

    // Publish an event to the given partition.
    // Returns the globalId assigned by the bus, or -1 on error.
    int publish(const std::string& eventType,
                const std::string& partitionName,
                const std::string& data);

private:
    std::string name_;
    EventBus&   bus_;
};

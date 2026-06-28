#pragma once
#include <vector>
#include <string>
#include "Event.h"

// Stores events in a fixed-capacity circular buffer.
// Each event gets a monotonically increasing globalId.
// slot in buffer = globalId % capacity
class Partition {
public:
    Partition(std::string name, int capacity);

    const std::string& getName() const { return name_; }
    int getCapacity()      const { return capacity_; }
    int getNextGlobalId()  const { return nextGlobalId_; }

    // Assigns the next globalId to the event and writes it into the buffer.
    // Returns the assigned globalId.
    int publish(const std::string& eventType, const std::string& data);

    Event getEvent(int globalId) const;
    bool  isAvailable(int globalId) const;

    // Oldest globalId still in the buffer (newer events may have overwritten older ones).
    int getOldestAvailableId() const;
    // Newest globalId currently stored (-1 if empty).
    int getNewestAvailableId() const;

private:
    std::string        name_;
    int                capacity_;
    std::vector<Event> buffer_;
    int                nextGlobalId_; // next id to assign
};

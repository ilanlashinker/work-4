#pragma once
#include <vector>
#include <string>
#include "Event.h"

// saves events in a circular array - when it's full, old ones get replaced
// each event gets a number starting from 0 going up
// to find where an event sits: event number % array size
class Partition {
public:
    Partition(std::string name, int capacity);

    const std::string& getName() const { return name_; }
    int getCapacity()      const { return capacity_; }
    int getNextGlobalId()  const { return nextGlobalId_; }

    // saves the event and returns the number we gave it
    int publish(const std::string& eventType, const std::string& data);

    Event getEvent(int globalId) const;
    bool  isAvailable(int globalId) const;

    // the oldest event number that's still saved (not replaced yet)
    int getOldestAvailableId() const;
    // the newest event number saved (-1 if nothing was published yet)
    int getNewestAvailableId() const;

private:
    std::string        name_;
    int                capacity_;
    std::vector<Event> buffer_;
    int                nextGlobalId_; // the next number to give to the next event
};

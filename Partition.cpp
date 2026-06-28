#include "Partition.h"
#include <algorithm>
#include <stdexcept>

Partition::Partition(std::string name, int capacity)
    : name_(std::move(name)), capacity_(capacity),
      buffer_(capacity), nextGlobalId_(0) {
    if (capacity <= 0)
        throw std::invalid_argument("Partition capacity must be positive");
}

int Partition::publish(const std::string& eventType, const std::string& data) {
    int id = nextGlobalId_++;
    buffer_[id % capacity_] = Event(id, eventType, name_, data);
    return id;
}

Event Partition::getEvent(int globalId) const {
    if (!isAvailable(globalId)) return Event(); // invalid sentinel
    return buffer_[globalId % capacity_];
}

bool Partition::isAvailable(int globalId) const {
    if (nextGlobalId_ == 0) return false;
    return globalId >= getOldestAvailableId() && globalId < nextGlobalId_;
}

int Partition::getOldestAvailableId() const {
    return std::max(0, nextGlobalId_ - capacity_);
}

int Partition::getNewestAvailableId() const {
    return nextGlobalId_ - 1;
}

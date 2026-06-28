#include "Publisher.h"
#include "EventBus.h"
#include <iostream>

Publisher::Publisher(std::string name, EventBus& bus)
    : name_(std::move(name)), bus_(bus) {}

int Publisher::publish(const std::string& eventType,
                        const std::string& partitionName,
                        const std::string& data) {
    std::cout << "[PUBLISHER:" << name_ << "] ";
    return bus_.publish(eventType, partitionName, data);
}

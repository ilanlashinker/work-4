#include "Subscriber.h"
#include <iostream>

Subscriber::Subscriber(std::string name) : name_(std::move(name)) {}

void Subscriber::onEvent(const Event& event) {
    received_.push_back(event);
    std::cout << "  [CONSUME] '" << name_
              << "' got event #" << event.globalId
              << " [" << event.type << "] partition='"
              << event.partitionName << "' data=\"" << event.data << "\"\n";
}

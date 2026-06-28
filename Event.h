#pragma once
#include <string>

struct Event {
    int globalId;
    std::string type;
    std::string partitionName;
    std::string data;

    Event() : globalId(-1) {}
    Event(int id, std::string type, std::string partition, std::string data)
        : globalId(id), type(std::move(type)),
          partitionName(std::move(partition)), data(std::move(data)) {}

    bool isValid() const { return globalId >= 0; }
};

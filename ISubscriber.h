#pragma once
#include <string>
#include "Event.h"


class ISubscriber {
public:
    virtual ~ISubscriber() = default;
    virtual const std::string& getName() const = 0;
    virtual void onEvent(const Event& event) = 0;
};

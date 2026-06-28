#pragma once
#include "ISubscriber.h"
#include <vector>

class Subscriber : public ISubscriber {
public:
    explicit Subscriber(std::string name);

    const std::string& getName() const override { return name_; }
    void onEvent(const Event& event) override;

    const std::vector<Event>& getReceivedEvents() const { return received_; }
    void clearReceived() { received_.clear(); }

private:
    std::string        name_;
    std::vector<Event> received_;
};

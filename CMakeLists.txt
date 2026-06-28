cmake_minimum_required(VERSION 3.14)
project(EventDriven CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(EventDriven
    main.cpp
    Partition.cpp
    Subscriber.cpp
    Publisher.cpp
    EventBus.cpp
    Tests.cpp
)

target_include_directories(EventDriven PRIVATE ${CMAKE_SOURCE_DIR})

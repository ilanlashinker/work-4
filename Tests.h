#pragma once
#include <string>

// Runs all 7 mandatory tests against freshly created EventBus instances.
class Tests {
public:
    static void runAll();

private:
    static void test1_partitionIsolation();
    static void test2_eventTypeFilter();
    static void test3_pointerAdvances();
    static void test4_circularBufferOverwrite();
    static void test5_rewindWorks();
    static void test6_rewindToOverwritten();
    static void test7_multipleSubscribersIndependent();

    static void report(int num, const std::string& desc, bool passed);
};

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <numeric>

#include "data_structures/bsl/bsl.hpp"

using namespace data_structures::bsl;
using namespace std;

TEST_CASE("init"){
    BSL bsl(0.25, 8, 4);
};

TEST_CASE("basic insert") {
    BSL bsl(0.25, 16, 16);

    // generate list of values
    std::vector<int64_t> values(1024);
    std::iota(values.begin(), values.end(), 1);
    std::random_shuffle(values.begin(), values.end());

    // insert and
    for (auto v : values) {
        bsl.insert(v, v);
        //std::cout << "DUMPING BSL" << std::endl;
        //bsl.dump();
        //std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    bsl.dump();

    // check if we can still find them
    for (auto v : values) {
        int64_t x = bsl.find(v);
        REQUIRE(x == v);
    }

}
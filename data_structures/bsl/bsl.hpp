#pragma once

#include <atomic>
#include <vector>
#include "tbb/spin_mutex.h"

#include "interface.hpp"
#include "iterator.hpp"
#include "parallel.hpp"
#include "common/spin_lock.hpp"
#include <mutex>
#include <limits>
#include <iostream>
#include <thread>
#include <cassert>

using namespace std;

namespace data_structures::bsl {
//#define DEBUG
#if defined(DEBUG)
    static mutex _local_mutex;
    #define ASSERT(expr, msg) { lock_guard<mutex> _lock(_local_mutex); if (!(expr)) { std::cerr << msg << std::endl; std::cerr.flush(); }}
    #define COUT_DEBUG(msg) { lock_guard<mutex> _lock(_local_mutex); \
        std::cout << "[BSL::" << __FUNCTION__ << "] [thread: " << pthread_self() << "] " << msg << std::endl; }
#else
    #define ASSERT(expr, msg) { assert(expr); }
    #define COUT_DEBUG(msg)
#endif
#define REMOVE_DEBUG(msg) { COUT_DEBUG("[Remove] " << msg); }
#define INSERT_DEBUG(msg) { COUT_DEBUG("[Insert] " << msg); }

struct BSLNode {
int64_t key, value;
};

struct BSLBlock {
atomic_int64_t version;
atomic_int64_t length;

tbb::spin_mutex mu;
std::vector<BSLBlock*> forward;
BSLBlock(int64_t anchor, int64_t level, int64_t maxblksize);

int64_t anchor;
std::vector<BSLNode> values;
bool insert(int64_t key, int64_t value);
int64_t find(int64_t key);
int64_t remove(int64_t key);
void navigate(std::vector<int64_t>& previousVersions, std::vector<int64_t>& currentVersions, std::vector<BSLBlock*>& previousBlocks, std::vector<BSLBlock*>& currentBlocks);
size_t size();
bool full();
};

class BSL: public data_structures::Interface {
private:
    float p;
    int64_t maxlevel, maxblksize;
    std::atomic<size_t> cardinality;
    BSLBlock* head;
    int64_t randLevel() const;
public:
    BSL(float p = 0.1, int64_t maxlevel = 16, int64_t maxblksize = 512);

    void insert(int64_t key, int64_t value) override;
    int64_t find(int64_t key) const override;
    int64_t remove(int64_t key) override;
    std::size_t size() const override;
    bool empty() const;
    ::data_structures::Interface::SumResult sum(int64_t min, int64_t max) const override;
    std::unique_ptr<::data_structures::Iterator> iterator() const override;
    void dump() const override;
};

}

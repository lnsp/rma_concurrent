#include "bsl.hpp"
#include <algorithm>
#include <thread>


namespace data_structures::bsl {

using defer = shared_ptr<void>;    
BSLBlock::BSLBlock(int64_t anchor, int64_t level) {
    this->anchor = anchor;
    this->forward = std::vector<BSLBlock*>(level);
}

bool BSLBlock::insert(int64_t key, int64_t value) {
    bool found = false;
    for (auto i = 0; i < values.size(); i++) {
        if (values[i].key == key) {
            values[i].value = value;
            found = true;
        }
    }
    if (!found) values.push_back({ key, value });
    return !found;
}

int64_t BSLBlock::find(int64_t key) {
    for (auto i = 0; i < values.size(); i++) {
        if (values[i].key == key) {
            return values[i].value;
        }
    }
    return -1;
}

size_t BSLBlock::size() {
    return values.size();
}

BSL::BSL(float p, int64_t maxlevel, int64_t maxblksize) {
    this->p = p;
    this->maxlevel = maxlevel;
    this->maxblksize = maxblksize;

    // Construct empty BSL
    int64_t anchorBot0 = std::numeric_limits<int64_t>::min();
    int64_t anchorBot1 = std::numeric_limits<int64_t>::min()+1;
    int64_t anchorTop0 = std::numeric_limits<int64_t>::max();

    head = new BSLBlock(anchorBot0, maxlevel+1);
    auto base = new BSLBlock(anchorBot1, maxlevel+1);
    auto tail = new BSLBlock(anchorTop0, maxlevel+1);

    for (auto i = 0; i <= maxlevel; i++) {
        head->forward[i] = base;
        base->forward[i] = tail;
    }

    COUT_DEBUG("Initialized with maxlevel=" << maxlevel << " and p=" << p);
}

int64_t BSL::randLevel() const {
    float r = (float)rand()/RAND_MAX;
    int l = 0;
    while (r < p && l < maxlevel-1) {
        l++;
        r = (float)rand()/RAND_MAX;
    }
    return l;
}

void BSL::insert(int64_t key, int64_t value) {
    restart:

    // Previous -> Current, where previous is the node that
    // should contain the value and current is the node where it may point.
    std::vector<int64_t> previous_versions(maxlevel+1);
    std::vector<BSLBlock*> previous_blocks(maxlevel+1);

    std::vector<int64_t> current_versions(maxlevel+1);
    std::vector<BSLBlock*> current_blocks(maxlevel+1);

    // Set current to head node, track version
    current_versions[maxlevel] = head->version;
    current_blocks[maxlevel] = head;

    // Search in skip list
    for (auto i = maxlevel; i >= 0; i--) {
        // Make sure that versions match up
        if ((current_versions[i] != current_blocks[i]->version)) goto restart;

        COUT_DEBUG("Checking node=" << std::hex << current_blocks[i] << " with anchor=" << std::dec << current_blocks[i]->anchor << " at level=" << std::dec << i);

        // Move forwards as long as the current node may contain the key
        while (current_blocks[i]->anchor <= key) {
            if ((current_versions[i] != current_blocks[i]->version)
             || (previous_blocks[i] && previous_versions[i] != previous_blocks[i]->version)) goto restart;

            previous_versions[i] = current_versions[i];
            previous_blocks[i] = current_blocks[i];

            current_versions[i] = current_blocks[i]->forward[i]->version;
            current_blocks[i] = current_blocks[i]->forward[i];

            COUT_DEBUG("Skipping forward to node=" << std::hex << current_blocks[i] << " with anchor=" << std::dec << current_blocks[i]->anchor);
        }

        COUT_DEBUG("Determined prev anchor=" << previous_blocks[i]->anchor << ", current anchor=" <<  current_blocks[i]->anchor << " at level " << i);

        // :: current_blocks[i]->anchor > key => current does not contain key
        // :: previous_blocks[i]->anchor <= key => current may contain key

        // Copy down starting point for next iteration
        if (i > 0) {
            current_versions[i-1] = previous_versions[i];
            current_blocks[i-1] = previous_blocks[i];
        }

        // std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    COUT_DEBUG("Inserting on anchor=" << std::dec << previous_blocks[0]->anchor);

    BSLBlock *lockedPrevBlock = nullptr, *lockedCurrBlock = nullptr;
    // Validate versions and lock nodes
    for (auto lockLevel = maxlevel; lockLevel >= 0; lockLevel--) {
        COUT_DEBUG("Locking level " << lockLevel << " with previous=" << std::hex << previous_blocks[lockLevel] << " and current=" << current_blocks[lockLevel] << std::dec);
        // Lock nodes
        if (lockedPrevBlock != previous_blocks[lockLevel]) {
            COUT_DEBUG("Locking previous=" << previous_blocks[lockLevel]->anchor << " on level=" << lockLevel);
            previous_blocks[lockLevel]->mu.lock();
            lockedPrevBlock = previous_blocks[lockLevel];
        }
        if (lockedCurrBlock != current_blocks[lockLevel] && current_blocks[lockLevel]) {
            COUT_DEBUG("Locking current=" << current_blocks[lockLevel]->anchor << " on level=" << lockLevel);
            current_blocks[lockLevel]->mu.lock();
            lockedCurrBlock = current_blocks[lockLevel];
        }
        // Check versions and pointers
        if ((current_versions[lockLevel] != current_blocks[lockLevel]->version)
         || (previous_blocks[lockLevel] && previous_versions[lockLevel] != previous_blocks[lockLevel]->version)
         || (previous_blocks[lockLevel]->forward[lockLevel] != current_blocks[lockLevel])) {
            // Unlock all nodes with higher lock level
            lockedPrevBlock = lockedCurrBlock = nullptr;
            for (auto i = lockLevel; i < maxlevel; i++) {
                // Unlock nodes
                if (lockedCurrBlock != current_blocks[i]) {
                    current_blocks[i]->mu.unlock();
                    lockedCurrBlock = current_blocks[i];
                }
                if (lockedPrevBlock != previous_blocks[i]) {
                    previous_blocks[i]->mu.unlock();
                    lockedPrevBlock = previous_blocks[i];
                }
            }
            goto restart;
        }
    }

    // Insert into block
    if (previous_blocks[0]->insert(key, value)) cardinality++;

    // Check if rebalance is required
    if (previous_blocks[0]->size() > maxblksize) {
        // Do rebalance here
        COUT_DEBUG("Node " << previous_blocks[0]->anchor << " reached size of " << previous_blocks[0]->size() << ", need to rebalance");
        // We determine the next anchor by sorting
        std::sort(previous_blocks[0]->values.begin(), previous_blocks[0]->values.end(), [](auto x, auto y) { return x.key < y.key; });
        // and picking the value in the middle
        auto anchorIt = previous_blocks[0]->values.begin() + (previous_blocks[0]->values.size()/2);
        // Then we allocate a new BSL block
        BSLBlock* next = new BSLBlock(anchorIt->key, maxlevel);
        next->values.reserve(previous_blocks[0]->values.end() - anchorIt);
        next->values.insert(next->values.begin(), anchorIt, previous_blocks[0]->values.end());
        // And drop the values from the old block
        previous_blocks[0]->values.resize(anchorIt - previous_blocks[0]->values.begin());

        auto rlevel = randLevel();
        for (auto i = 0; i <= rlevel; i++) {
          next->forward[i] = current_blocks[i];
          current_blocks[i]->version++;
          previous_blocks[i]->forward[i] = next;
          previous_blocks[i]->version++;
        }

        COUT_DEBUG("Inserted key=" << key);
    }

    // Unlock nodes from the bottom up
    for (auto i = 0; i <= maxlevel; i++) {
        // Unlock nodes
        previous_blocks[i]->mu.unlock();
        current_blocks[i]->mu.unlock();
    }
}

int64_t BSL::find(int64_t key) const {
    // Walk down BSL
    BSLBlock* current = head;
    for (auto i = maxlevel; i >= 0; i--) {
        // walk forward until next anchor is larger than key
        // this guarantees that the next node can not contain our key
        while (current->forward[i] != nullptr
               && current->forward[i]->anchor <= key) {
          current = current->forward[i];
        }
    }

    COUT_DEBUG("searching in anchor=" << current->anchor << " for key=" << key);

    // Do linear search in current node
    return current->find(key);
}

int64_t BSL::remove(int64_t key) {
    return 0;
}

std::size_t BSL::size() const {
    return static_cast<std::size_t>(cardinality);
}

bool BSL::empty() const {
    return cardinality == 0;
}

::data_structures::Interface::SumResult BSL::sum(int64_t min, int64_t max) const {
    return ::data_structures::Interface::SumResult();
}

std::unique_ptr<::data_structures::Iterator> BSL::iterator() const {
    return nullptr;
}

void BSL::dump() const {
    // Dump out nodes at level 0
    BSLBlock* node = head;
    while (node != nullptr) {
        // depth is number of forwards != nullptr
        std::cout << "[anchor="
                  << node->anchor
                  << " forward={ ";
        for (auto i = 0; i <= maxlevel; i++) {
            if (node->forward[i] != nullptr) {
                std::cout << node->forward[i]->anchor << " ";
            }
            else break;
        }
        // print out anchor, number of values and depth
        std::cout << "} values={ ";
        // dump out values
        for (auto v : node->values) {
          std::cout << v.key << " ";
        }
        std::cout << "}]" << std::endl;
        node = node->forward[0];
    }
}

}

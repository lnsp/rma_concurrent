#include "bsl.hpp"
#include <algorithm>
#include <thread>


namespace data_structures::bsl {

using defer = shared_ptr<void>;    
BSLBlock::BSLBlock(int64_t anchor, int64_t level) {
    this->anchor = anchor;
    this->forward = std::vector<BSLBlock*>(level, nullptr);
}

bool BSLBlock::insert(int64_t key, int64_t value) {
    for (auto i = 0; i < values.size(); i++) {
        if (values[i].key == key) {
            values[i].value = value;
            return false;
        }
    }
    values.push_back({ key, value });
    return true;
}

int64_t BSLBlock::find(int64_t key) {
    for (auto i = 0; i < values.size(); i++) {
        if (values[i].key == key) {
            auto v = values[i].value;
            return v;
        }
    }
    return -1;
}

int64_t BSLBlock::remove(int64_t key) {
    for (auto i = 0; i < values.size(); i++ ) {
        if (values[i].key == key) {
            int64_t v = values[i].value;
            // swap with last
            values[i] = values[values.size()-1];
            values.resize(values.size()-1);
            return v;
        }
    }
    return -1;
}

size_t BSLBlock::size() {
    auto s = values.size();
    return s;
}

BSL::BSL(float p, int64_t maxlevel, int64_t maxblksize) {
    this->p = p;
    this->maxlevel = maxlevel;
    this->maxblksize = maxblksize;
    this->cardinality = 0;

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
    //bool has_inserted = false;
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

        INSERT_DEBUG("Checking node=" << std::hex << current_blocks[i] << " with anchor=" << std::dec << current_blocks[i]->anchor << " at level=" << std::dec << i);

        // Move forwards as long as the current node may contain the key
        while (current_blocks[i]->anchor <= key) {
            if ((current_versions[i] != current_blocks[i]->version)
             || (previous_blocks[i] && previous_versions[i] != previous_blocks[i]->version)) goto restart;

            previous_versions[i] = current_versions[i];
            previous_blocks[i] = current_blocks[i];

            current_versions[i] = current_blocks[i]->forward[i]->version;
            current_blocks[i] = current_blocks[i]->forward[i];

            INSERT_DEBUG("Skipping forward to node=" << std::hex << current_blocks[i] << " with anchor=" << std::dec << current_blocks[i]->anchor);
        }

        INSERT_DEBUG("Determined prev anchor=" << previous_blocks[i]->anchor << ", current anchor=" <<  current_blocks[i]->anchor << " at level " << i);

        // :: current_blocks[i]->anchor > key => current does not contain key
        // :: previous_blocks[i]->anchor <= key => current may contain key

        // Copy down starting point for next iteration
        if (i > 0) {
            current_versions[i-1] = previous_versions[i];
            current_blocks[i-1] = previous_blocks[i];
        }

        // std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    INSERT_DEBUG("Inserting on anchor=" << std::dec << previous_blocks[0]->anchor);


    // If we only insert into the block, we do not need to lock the entire tree
    //if (!has_inserted) {
    //    previous_blocks[0]->mu.lock();
    //    if (previous_blocks[0]->version != previous_versions[0])  {
    //        previous_blocks[0]->mu.unlock();
    //        goto restart;
    //    }
    //    // Insert into block
    //    if (previous_blocks[0]->insert(key, value)) {
    //        cardinality++;
    //    }
    //    has_inserted = true;
    //    bool need_rebalance = previous_blocks[0]->size() > maxblksize;
    //    previous_blocks[0]->mu.unlock();
    //    // Check for rebalancing
    //    if (!need_rebalance) return;
    //}
    INSERT_DEBUG("Inserting key=" << key << " into anchor=" << previous_blocks[0]->anchor);

    // Lock up to rlevel
    auto rlevel = randLevel();

validate_versions:
    BSLBlock *lockedPrevBlock = nullptr, *lockedCurrBlock = nullptr;
    // Validate versions and lock nodes
    for (auto lockLevel = rlevel; lockLevel >= 0; lockLevel--) {
        INSERT_DEBUG("Locking level " << lockLevel << " with previous=" << std::hex << previous_blocks[lockLevel] << " and current=" << current_blocks[lockLevel] << std::dec);
        // Attempt to obtain level lock
        bool failedPrevLock = false, failedCurrentLock = false;
        if (lockedPrevBlock != previous_blocks[lockLevel]) {
            if (previous_blocks[lockLevel]->mu.try_lock()) {
                lockedPrevBlock = previous_blocks[lockLevel];
            } else failedPrevLock = true;
        }
        if (lockedCurrBlock != current_blocks[lockLevel] && current_blocks[lockLevel]) {
            if (current_blocks[lockLevel]->mu.try_lock()) {
                lockedCurrBlock = current_blocks[lockLevel];
            } else failedCurrentLock = true;
        }
        
        // Check versions and pointers
        if ((current_versions[lockLevel] != current_blocks[lockLevel]->version)
         || (previous_blocks[lockLevel] && previous_versions[lockLevel] != previous_blocks[lockLevel]->version)
         || (previous_blocks[lockLevel]->forward[lockLevel] != current_blocks[lockLevel])
         || failedPrevLock || failedCurrentLock) {
            INSERT_DEBUG("[Insert] Detected conflict on level=" << lockLevel << ", unlocking now");

            // Detect incomplete level lock
            if (failedPrevLock || failedCurrentLock) {
                if (!failedPrevLock) previous_blocks[lockLevel]->mu.unlock();
                if (!failedCurrentLock) current_blocks[lockLevel]->mu.unlock();
                lockLevel++;
            }

            // Unlock all nodes with higher lock level
            lockedPrevBlock = lockedCurrBlock = nullptr;
            for (auto i = rlevel; i >= lockLevel; i--) {
                INSERT_DEBUG("Unlocking level " << lockLevel << " with previous=" << std::hex << previous_blocks[i] << " and current=" << current_blocks[i] << std::dec);
                if (lockedPrevBlock != previous_blocks[i]) {
                    previous_blocks[i]->mu.unlock();
                    lockedPrevBlock = previous_blocks[i];
                }
                // Unlock nodes
                if (lockedCurrBlock != current_blocks[i]) {
                    current_blocks[i]->mu.unlock();
                    lockedCurrBlock = current_blocks[i];
                }
            }

            if (failedPrevLock || failedCurrentLock) goto validate_versions;
            goto restart;
        }
    }

    if (previous_blocks[0]->insert(key, value)) cardinality++;

    if (previous_blocks[0]->size() > maxblksize) {
        // Do rebalance here
        INSERT_DEBUG("Node " << previous_blocks[0]->anchor << " reached size of " << previous_blocks[0]->size() << ", need to rebalance");
        // We determine the next anchor by sorting
        // Just pick mid value as split
        std::sort(previous_blocks[0]->values.begin(), previous_blocks[0]->values.end(), [](auto x, auto y) { return x.key < y.key; });
        // and picking the value in the middle
        auto anchorIt = previous_blocks[0]->values.begin() + (previous_blocks[0]->values.size()/2);
        // Then we allocate a new BSL block
        // std::vector<BSLNode> prevValues, currValues;
        // for (auto i = 0; i < previous_blocks[0]->values.size(); i++) {
        //     if (previous_blocks[0]->values[i].key < anchorIt->key) {
        //         prevValues.push_back(previous_blocks[0]->values[i]);
        //     } else {
        //         currValues.push_back(previous_blocks[0]->values[i]);
        //     }
        // }
        BSLBlock* next = new BSLBlock(anchorIt->key, maxlevel+1);
        // next->values = currValues;
        // previous_blocks[0]->values = prevValues;
        next->values.reserve(previous_blocks[0]->values.end() - anchorIt);
        next->values.insert(next->values.begin(), anchorIt, previous_blocks[0]->values.end());
        // And drop the values from the old block
        previous_blocks[0]->values.resize(anchorIt - previous_blocks[0]->values.begin());

        for (auto i = rlevel; i >= 0; i--) {
            INSERT_DEBUG("Rebalance with rlevel=" << i << " and maxlevel=" << maxlevel << " pointing to " << current_blocks[i]->anchor);
            next->forward[i] = current_blocks[i];
            current_blocks[i]->version++;
            previous_blocks[i]->forward[i] = next;
            previous_blocks[i]->version++;
        }

        INSERT_DEBUG("Inserted key=" << key);
    }

    // Unlock nodes from the bottom up
    lockedCurrBlock = lockedPrevBlock = nullptr;
    for (auto i = rlevel; i >= 0; i--) {
        if (lockedPrevBlock != previous_blocks[i]) {
            INSERT_DEBUG("Unlocking previous=" << previous_blocks[i]->anchor << " on level=" << i);
            previous_blocks[i]->mu.unlock();
            lockedPrevBlock = previous_blocks[i];
        }
        // Unlock nodes
        if (lockedCurrBlock != current_blocks[i]) {
            INSERT_DEBUG("Unlocking current=" << current_blocks[i]->anchor << " on level=" << i);
            current_blocks[i]->mu.unlock();
            lockedCurrBlock = current_blocks[i];
        }
    }
}

int64_t BSL::find(int64_t key) const {
    restart:
    // Walk down BSL
    int64_t current_version = head->version, prev_version = 0;
    BSLBlock* current = head, *prev = nullptr;
    for (auto i = maxlevel; i >= 0; i--) {
        // walk forward until next anchor is larger than key
        // this guarantees that the next node can not contain our key
        while (current->forward[i] != nullptr
               && current->forward[i]->anchor <= key) {
            if ((current_version != current->version)
             || (prev && prev_version != prev->version)) goto restart;

            prev_version = current_version;
            prev = current;
            current_version = current->forward[i]->version;
            current = current->forward[i];

        }
            if (i > 0) {
            current_version = prev_version;
            current = prev;
        }
    }

    COUT_DEBUG("searching in anchor=" << current->anchor << " for key=" << key);

    // Do linear search in current node
    return current->find(key);
}

int64_t BSL::remove(int64_t key) {
    restart:
    //std::this_thread::sleep_for(std::chrono::seconds(1));

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

        REMOVE_DEBUG("Checking node=" << std::hex << current_blocks[i] << " with anchor=" << std::dec << current_blocks[i]->anchor << " at level=" << std::dec << i);

        // Move forwards as long as the next node may contain the key
        while (current_blocks[i]->forward[i]->anchor <= key) {
            if ((current_versions[i] != current_blocks[i]->version)
             || (previous_blocks[i] && previous_versions[i] != previous_blocks[i]->version)) goto restart;

            previous_versions[i] = current_versions[i];
            previous_blocks[i] = current_blocks[i];

            current_versions[i] = current_blocks[i]->forward[i]->version;
            current_blocks[i] = current_blocks[i]->forward[i];

            REMOVE_DEBUG("Skipping forward to node=" << std::hex << current_blocks[i] << " with anchor=" << std::dec << current_blocks[i]->anchor);
        }

        REMOVE_DEBUG("Determined prev anchor=" << previous_blocks[i]->anchor << ", current anchor=" <<  current_blocks[i]->anchor << " at level " << i);

        // :: current_blocks[i]->anchor <= key => current contains the key
        // :: previous_blocks[i]->anchor << key => previous does not contain the key

        // Copy down starting point for next iteration
        if (i > 0) {
            current_versions[i-1] = previous_versions[i];
            current_blocks[i-1] = previous_blocks[i];
        }
    }

    // Determine first level where we are pointed to out current block
    int64_t rlevel = 0;
    while (rlevel < maxlevel && current_blocks[rlevel+1] == current_blocks[rlevel]) rlevel++;

    BSLBlock *lockedPrevBlock = nullptr, *lockedCurrBlock = nullptr;
    // Validate versions and lock nodes
    for (auto lockLevel = rlevel; lockLevel >= 0; lockLevel--) {
        REMOVE_DEBUG("Locking level " << lockLevel << " with prev=" << previous_blocks[lockLevel] << " current=" << current_blocks[lockLevel] << std::dec);
        // Attempt to obtain level lock
        bool failed_curr_lock = false, failed_prev_lock = false;
        if (lockedPrevBlock != previous_blocks[lockLevel]) {
            if (previous_blocks[lockLevel]->mu.try_lock()) {
                lockedPrevBlock = previous_blocks[lockLevel];
            } else failed_prev_lock = true;
        }
        if (lockedCurrBlock != current_blocks[lockLevel]) {
            if (current_blocks[lockLevel]->mu.try_lock()) {
                lockedCurrBlock = current_blocks[lockLevel];
            } else failed_curr_lock = true;
        }

        // Check versions and pointers
        if ((previous_versions[lockLevel] != previous_blocks[lockLevel]->version)
         || (current_versions[lockLevel] != current_blocks[lockLevel]->version)
         || (previous_blocks[lockLevel]->forward[lockLevel] != current_blocks[lockLevel])
         || failed_curr_lock || failed_prev_lock) {
            REMOVE_DEBUG("Detected conflict on level=" << lockLevel << ", unlocking now");

            // Detect incomplete level lock
            if (failed_prev_lock || failed_curr_lock) {
                REMOVE_DEBUG("Failed level lock, resetting lock level");
                if (!failed_curr_lock) current_blocks[lockLevel]->mu.unlock();
                if (!failed_prev_lock) previous_blocks[lockLevel]->mu.unlock();
                lockLevel++;
            }

            // Unlock all nodes with higher lock level
            lockedCurrBlock = lockedPrevBlock = nullptr;
            for (auto i = rlevel; i >= lockLevel; i--) {
                REMOVE_DEBUG("Unlocking level " << i << " with prev=" << std::hex << previous_blocks[i] << " and current=" << current_blocks[i] << std::dec);
                if (lockedCurrBlock != current_blocks[i]) {
                    current_blocks[i]->mu.unlock();
                    lockedCurrBlock = current_blocks[i];
                }
                // Unlock nodes
                if (lockedPrevBlock != previous_blocks[i]) {
                    previous_blocks[i]->mu.unlock();
                    lockedPrevBlock = previous_blocks[i];
                }
            }
            goto restart;
        }
    }

    REMOVE_DEBUG("Deleting key=" << key << " in anchor=" << current_blocks[0]->anchor);
    int64_t value = current_blocks[0]->remove(key);
    if (value != -1) cardinality--;

    /*
    if (previous_blocks[0] != head) {
        // Merge back together if required
        if (current_blocks[0]->size() == 0) {
            // Just remove the current block, make all pointers from previous[0] that point to
            // current, point to current->forward
            for (auto i = 0; i <= rlevel; i++) {
                previous_blocks[i]->forward[i] = current_blocks[i]->forward[i];
                previous_versions[i]++;
            }
            current_versions[0]++;
        } else if (previous_blocks[0]->size() + current_blocks[0]->size() < 0.5 * maxblksize) {
            // Merge them back together
            previous_blocks[0]->values.insert(previous_blocks[0]->values.end(), current_blocks[0]->values.begin(), current_blocks[0]->values.end());
            for (auto i = 0; i <= rlevel; i++) {
                previous_blocks[i]->forward[i] = current_blocks[i]->forward[i];
                previous_versions[i]++;
            }
            current_versions[0]++;
        }
    }
    */

    // Unlock nodes from the bottom up
    lockedPrevBlock = lockedCurrBlock = nullptr;
    for (auto i = rlevel; i >= 0; i--) {
        REMOVE_DEBUG("Unlocking level " << i << " with prev=" << std::hex << previous_blocks[i] << " and current=" << current_blocks[i] << std::dec);
        if (lockedCurrBlock != current_blocks[i]) {
            current_blocks[i]->mu.unlock();
            lockedCurrBlock = current_blocks[i];
        }
        // Unlock nodes
        if (lockedPrevBlock != previous_blocks[i]) {
            previous_blocks[i]->mu.unlock();
            lockedPrevBlock = previous_blocks[i];
        }
    }
    return value;
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

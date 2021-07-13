#include "bsl.hpp"
#include <algorithm>
#include <thread>


namespace data_structures::bsl {

using defer = shared_ptr<void>;    
BSLBlock::BSLBlock(int64_t anchor, int64_t level, int64_t maxblksize) {
    this->anchor = anchor;
    this->length = 0;
    this->forward = std::vector<BSLBlock*>(level, nullptr);
    this->values = std::vector<BSLNode>(maxblksize);
}

bool BSLBlock::full() {
    ASSERT(length <= this->values.size(), "length must be smaller equal than value size");
    return length == this->values.size();
}

bool BSLBlock::insert(int64_t key, int64_t value) {
    for (auto i = 0; i < length; i++) {
        if (values[i].key == key) {
            values[i].value = value;
            return false;
        }
    }
    ASSERT(length < values.size(), "length must be smaller than value size on insert");
    values[length] = { key, value };
    ++length;
    return true;
}

int64_t BSLBlock::find(int64_t key) {
    for (auto i = 0; i < length; i++) {
        if (values[i].key == key) {
            auto v = values[i].value;
            return v;
        }
    }
    return -1;
}

int64_t BSLBlock::remove(int64_t key) {
    for (auto i = 0; i < length; i++ ) {
        if (values[i].key == key) {
            int64_t v = values[i].value;
            // swap with last
            values[i] = values[length-1];
            // decrease len
            --length;
            return v;
        }
    }
    return -1;
}

size_t BSLBlock::size() {
    return length;
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

    head = new BSLBlock(anchorBot0, maxlevel+1, 0);
    auto base = new BSLBlock(anchorBot1, maxlevel+1, maxblksize);
    auto tail = new BSLBlock(anchorTop0, maxlevel+1, 0);

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
    std::vector<int64_t> previousVersions(maxlevel+1);
    std::vector<BSLBlock*> previousBlocks(maxlevel+1);

    std::vector<int64_t> currentVersions(maxlevel+1);
    std::vector<BSLBlock*> currentBlocks(maxlevel+1);

restart:
    INSERT_DEBUG("Starting insert operation for key=" << key);
    // Set current to head node, track version
    previousVersions[maxlevel] = head->version;
    previousBlocks[maxlevel] = head;
    currentVersions[maxlevel] = head->forward[maxlevel]->version;
    currentBlocks[maxlevel] = head->forward[maxlevel];

    // Search in skip list
    for (auto i = maxlevel; i >= 0; i--) {
        // Make sure that versions match up
        if ((currentVersions[i] != currentBlocks[i]->version
          || previousBlocks[i]->forward[i] != currentBlocks[i]
          || previousVersions[i] != previousBlocks[i]->version)) goto restart;

        INSERT_DEBUG("Checking node=" << std::hex << currentBlocks[i] << " with anchor=" << std::dec << currentBlocks[i]->anchor << " at level=" << std::dec << i);

        // Move forwards as long as the current node may contain the key
        while (currentBlocks[i]->anchor <= key) {
            if ((currentVersions[i] != currentBlocks[i]->version
              || previousBlocks[i]->forward[i] != currentBlocks[i]
              || previousVersions[i] != previousBlocks[i]->version)) goto restart;

            previousVersions[i] = currentVersions[i];
            previousBlocks[i] = currentBlocks[i];

            currentVersions[i] = currentBlocks[i]->forward[i]->version;
            currentBlocks[i] = currentBlocks[i]->forward[i];

            INSERT_DEBUG("Skipping forward to node=" << std::hex << currentBlocks[i] << " with anchor=" << std::dec << currentBlocks[i]->anchor);
        }

        INSERT_DEBUG("Determined prev anchor=" << previousBlocks[i]->anchor << ", current anchor=" <<  currentBlocks[i]->anchor << " at level " << i);

        // :: current_blocks[i]->anchor > key => current does not contain key
        // :: previous_blocks[i]->anchor <= key => current may contain key

        // Copy down starting point for next iteration
        if (i > 0) {
            previousVersions[i-1] = previousVersions[i];
            previousBlocks[i-1] = previousBlocks[i];
            currentVersions[i-1] = previousBlocks[i-1]->forward[i-1]->version;
            currentBlocks[i-1] = previousBlocks[i-1]->forward[i-1];
        }

        // std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Lock up to rlevel
    auto rlevel = randLevel();

    INSERT_DEBUG("Inserting on anchor=" << std::dec << previousBlocks[0]->anchor << " with rlevel=" << rlevel);

    BSLBlock *lockedPrevBlock = nullptr, *lockedCurrBlock = nullptr;
    bool failedPrevLock = false, failedCurrLock = false;

validateVersions:
    // Validate versions and lock nodes
    for (auto lockLevel = rlevel; lockLevel >= 0; lockLevel--) {
        failedPrevLock = failedCurrLock = false;
        INSERT_DEBUG("Locking level " << lockLevel << " with previous=" << previousBlocks[lockLevel] << " and current=" << currentBlocks[lockLevel] << " while avoiding (" << lockedPrevBlock << ", " << lockedCurrBlock << ")");

        if (lockedPrevBlock != previousBlocks[lockLevel]) {
            lockedPrevBlock = previousBlocks[lockLevel];
            if (!previousBlocks[lockLevel]->mu.try_lock()) {
                failedPrevLock = true;
            }
        }

        if (lockedCurrBlock != currentBlocks[lockLevel]) {
            lockedCurrBlock = currentBlocks[lockLevel];
            if (!currentBlocks[lockLevel]->mu.try_lock()) {
                failedCurrLock = true;
            }
        }

        // Check versions and pointers
        if ((previousVersions[lockLevel] != previousBlocks[lockLevel]->version)
         || (previousBlocks[lockLevel]->forward[lockLevel] != currentBlocks[lockLevel])
         || (currentVersions[lockLevel] != currentBlocks[lockLevel]->version)
         || failedCurrLock || failedPrevLock) {
             if (failedCurrLock || failedPrevLock) {
                INSERT_DEBUG("Detected lock conflict on level=" << lockLevel << ", unlocking now");
             } else {
                INSERT_DEBUG("Detected version conflict on level=" << lockLevel << ", unlocking now");
             }

            lockedCurrBlock = lockedPrevBlock = nullptr;

            if (failedPrevLock) {
                lockedPrevBlock = previousBlocks[lockLevel];
            }
            if (failedCurrLock) {
                lockedCurrBlock = currentBlocks[lockLevel];
            }

            // Unlock all nodes with lower lock level
            for (auto i = lockLevel; i <= rlevel; i++) {
                INSERT_DEBUG("Unlocking level " << i << " with previous=" << std::dec << previousBlocks[i]->anchor << " and current=" << currentBlocks[i]->anchor);
                if (lockedPrevBlock != previousBlocks[i]) {
                    previousBlocks[i]->mu.unlock();
                    lockedPrevBlock = previousBlocks[i];
                }
                if (lockedCurrBlock != currentBlocks[i]) {
                    currentBlocks[i]->mu.unlock();
                    lockedCurrBlock = currentBlocks[i];
                }
            }

            if (failedPrevLock || failedCurrLock) {
                lockedPrevBlock = lockedCurrBlock = nullptr;
                goto validateVersions;
            }

            // Restart and try again
            goto restart;
        }
    }

    if (previousBlocks[0]->insert(key, value)) {
        cardinality++;
    }

    INSERT_DEBUG("Inserted key=" << key);
    if (previousBlocks[0]->full()) {
        // Do rebalance here
        INSERT_DEBUG("Node " << previousBlocks[0]->anchor << " reached size of " << previousBlocks[0]->size() << ", need to rebalance");

        // Sort block
        std::sort(previousBlocks[0]->values.begin(), previousBlocks[0]->values.begin() + previousBlocks[0]->length, [](const auto& a, const auto& b) { return a.key < b.key; });
        // Pick pivot element
        int64_t pivotIndex = previousBlocks[0]->length / 2;
        // Copy over content
        BSLBlock* next = new BSLBlock(previousBlocks[0]->values[pivotIndex].key, maxlevel+1, maxblksize);
        std::copy(previousBlocks[0]->values.begin() + pivotIndex, previousBlocks[0]->values.begin() + previousBlocks[0]->length, next->values.begin());
        next->length = previousBlocks[0]->length - pivotIndex;
        next->version = currentBlocks[0]->version.load();
        previousBlocks[0]->length = pivotIndex;

        // Add pointers
        for (auto i = 0; i <= rlevel; i++) {
            INSERT_DEBUG("Rebalance with i=" << i << " and next=" << next << " pointing to " << currentBlocks[i]->anchor);
            next->forward[i] = currentBlocks[i];
            currentBlocks[i]->version++;
            previousBlocks[i]->forward[i] = next;
            previousBlocks[i]->version++;
        }

        if (next->length + previousBlocks[0]->length != maxblksize) {
            INSERT_DEBUG("next and previous dont add up, because " << next->length << " + " << previousBlocks[0]->length << " != " << maxblksize);
        }

        if (next->full()) {
            INSERT_DEBUG("next is full after split");
        }

        if (previousBlocks[0]->full()) {
            INSERT_DEBUG("previous is full after split");
        }

        INSERT_DEBUG("Successfully split nodes, continuing with unlocking");
    }


    // Unlock nodes from the bottom up
    lockedPrevBlock = lockedCurrBlock = nullptr;
    for (auto i = 0; i <= rlevel; i++) {
        INSERT_DEBUG("Unlocking i=" << i << " with previous=" << std::dec << previousBlocks[i]->anchor << " and current=" << currentBlocks[i]->anchor);
        if (lockedPrevBlock != previousBlocks[i]) {
            previousBlocks[i]->mu.unlock();
            lockedPrevBlock = previousBlocks[i];
        }
        if (lockedCurrBlock != currentBlocks[i]) {
            currentBlocks[i]->mu.unlock();
            lockedCurrBlock = currentBlocks[i];
        }
    }
}

int64_t BSL::find(int64_t key) const {
    restart:
    // Walk down BSL
    int64_t currentVersion = head->forward[maxlevel]->version, previousVersion = head->version;
    BSLBlock* currentBlock = head->forward[maxlevel], *previousBlock = head;
    for (auto i = maxlevel; i >= 0; i--) {
        if (currentVersion != currentBlock->version
         || previousVersion != previousBlock->version
         || previousBlock->forward[i] != currentBlock) goto restart;

        // walk forward until next anchor is larger than key
        // this guarantees that the next node can not contain our key
        while (currentBlock->anchor <= key) {
            if (currentVersion != currentBlock->version
             || previousVersion != previousBlock->version
             || previousBlock->forward[i] != currentBlock) goto restart;

            previousVersion = currentVersion;
            previousBlock = currentBlock;
            currentVersion = currentBlock->forward[i]->version;
            currentBlock = currentBlock->forward[i];

        }
        if (i > 0) {
            currentVersion = previousBlock->forward[i-1]->version;
            currentBlock = previousBlock->forward[i-1];
        }
    }

    COUT_DEBUG("searching in anchor=" << currentBlock->anchor << " for key=" << key);

    // Do linear search in current node
    previousBlock->mu.lock();
    int64_t result = previousBlock->find(key);
    if (currentVersion != currentBlock->version || previousVersion != previousBlock->version || previousBlock->forward[0] != currentBlock) {
        previousBlock->mu.unlock();
        goto restart;
    }
    previousBlock->mu.unlock();
    // Make sure to validate result
    return result;
}

int64_t BSL::remove(int64_t key) {
    restart:
    //std::this_thread::sleep_for(std::chrono::seconds(1));

    std::vector<int64_t> previousVersions(maxlevel+1);
    std::vector<BSLBlock*> previousBlocks(maxlevel+1);

    std::vector<int64_t> currentVersions(maxlevel+1);
    std::vector<BSLBlock*> currentBlocks(maxlevel+1);

    previousVersions[maxlevel] = head->version;
    previousBlocks[maxlevel] = head;
    currentVersions[maxlevel] = head->forward[maxlevel]->version;
    currentBlocks[maxlevel] = head->forward[maxlevel];

    // Search in skip list
    for (auto i = maxlevel; i >= 0; i--) {
        // Make sure that versions match up
        if ((currentVersions[i] != currentBlocks[i]->version
          || previousBlocks[i]->forward[i] != currentBlocks[i]
          || previousVersions[i] != previousBlocks[i]->version)) goto restart;

        INSERT_DEBUG("Checking node=" << std::hex << currentBlocks[i] << " with anchor=" << std::dec << currentBlocks[i]->anchor << " at level=" << std::dec << i);

        // Move forwards as long as the current node may contain the key
        while (currentBlocks[i]->forward[i]->anchor <= key) {
            if ((currentVersions[i] != currentBlocks[i]->version
              || previousBlocks[i]->forward[i] != currentBlocks[i]
              || previousVersions[i] != previousBlocks[i]->version)) goto restart;

            previousVersions[i] = currentVersions[i];
            previousBlocks[i] = currentBlocks[i];

            currentVersions[i] = currentBlocks[i]->forward[i]->version;
            currentBlocks[i] = currentBlocks[i]->forward[i];

            INSERT_DEBUG("Skipping forward to node=" << std::hex << currentBlocks[i] << " with anchor=" << std::dec << currentBlocks[i]->anchor);
        }

        INSERT_DEBUG("Determined prev anchor=" << previousBlocks[i]->anchor << ", current anchor=" <<  currentBlocks[i]->anchor << " at level " << i);

        // :: current_blocks[i]->anchor > key => current does not contain key
        // :: previous_blocks[i]->anchor <= key => current may contain key

        // Copy down starting point for next iteration
        if (i > 0) {
            previousVersions[i-1] = previousVersions[i];
            previousBlocks[i-1] = previousBlocks[i];
            currentVersions[i-1] = previousBlocks[i-1]->forward[i-1]->version;
            currentBlocks[i-1] = previousBlocks[i-1]->forward[i-1];
        }

        // std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Determine first level where we are pointed to out current block
    int64_t rlevel = 0;
    while (rlevel < maxlevel && currentBlocks[rlevel+1] == currentBlocks[rlevel]) ++rlevel;

    BSLBlock *lockedPrevBlock = nullptr, *lockedCurrBlock = nullptr;
    bool failedPrevLock = false, failedCurrLock = false;

validateVersions:
    // Validate versions and lock nodes
    for (auto lockLevel = rlevel; lockLevel >= 0; lockLevel--) {
        failedPrevLock = failedCurrLock = false;
        INSERT_DEBUG("Locking level " << lockLevel << " with previous=" << previousBlocks[lockLevel] << " and current=" << currentBlocks[lockLevel] << " while avoiding (" << lockedPrevBlock << ", " << lockedCurrBlock << ")");

        if (lockedPrevBlock != previousBlocks[lockLevel]) {
            lockedPrevBlock = previousBlocks[lockLevel];
            if (!previousBlocks[lockLevel]->mu.try_lock()) {
                failedPrevLock = true;
            }
        }

        if (lockedCurrBlock != currentBlocks[lockLevel]) {
            lockedCurrBlock = currentBlocks[lockLevel];
            if (!currentBlocks[lockLevel]->mu.try_lock()) {
                failedCurrLock = true;
            }
        }

        // Check versions and pointers
        if ((previousVersions[lockLevel] != previousBlocks[lockLevel]->version)
         || (previousBlocks[lockLevel]->forward[lockLevel] != currentBlocks[lockLevel])
         || (currentVersions[lockLevel] != currentBlocks[lockLevel]->version)
         || failedCurrLock || failedPrevLock) {
             if (failedCurrLock || failedPrevLock) {
                INSERT_DEBUG("Detected lock conflict on level=" << lockLevel << ", unlocking now");
             } else {
                INSERT_DEBUG("Detected version conflict on level=" << lockLevel << ", unlocking now");
             }

            lockedCurrBlock = lockedPrevBlock = nullptr;

            if (failedPrevLock) {
                lockedPrevBlock = previousBlocks[lockLevel];
            }
            if (failedCurrLock) {
                lockedCurrBlock = currentBlocks[lockLevel];
            }

            // Unlock all nodes with lower lock level
            for (auto i = lockLevel; i <= rlevel; i++) {
                INSERT_DEBUG("Unlocking level " << i << " with previous=" << std::dec << previousBlocks[i]->anchor << " and current=" << currentBlocks[i]->anchor);
                if (lockedPrevBlock != previousBlocks[i]) {
                    previousBlocks[i]->mu.unlock();
                    lockedPrevBlock = previousBlocks[i];
                }
                if (lockedCurrBlock != currentBlocks[i]) {
                    currentBlocks[i]->mu.unlock();
                    lockedCurrBlock = currentBlocks[i];
                }
            }

            if (failedCurrLock || failedPrevLock) goto validateVersions;

            // Restart and try again
            goto restart;
        }
    }

    REMOVE_DEBUG("Deleting key=" << key << " in anchor=" << current_blocks[0]->anchor);
    int64_t value = currentBlocks[0]->remove(key);
    if (value != -1) {
        cardinality--;
    }

    //if (previousBlocks[0] != head) {
    //    // Merge back together if required
    //    if (previousBlocks[0]->length + currentBlocks[0]->length < 0.5 * maxblksize) {
    //        // Copy over elements
    //        for (int64_t i = 0; i < currentBlocks[0]->length; i++) {
    //            previousBlocks[0]->values[previousBlocks[0]->length] = currentBlocks[0]->values[i];
    //            ++previousBlocks[0]->length;
    //        }
    //        // Set pointers
    //        for (auto i = 0; i <= rlevel; i++) {
    //            previousBlocks[i]->version++;
    //            currentBlocks[i]->version++;
    //            previousBlocks[i]->forward[i] = currentBlocks[i]->forward[i];
    //            currentBlocks[i]->forward[i] = nullptr;
    //        }
    //    }
    //}

    // Unlock nodes from the bottom up
    lockedPrevBlock = lockedCurrBlock = nullptr;
    for (auto i = 0; i <= rlevel; i++) {
        INSERT_DEBUG("Unlocking i=" << i << " with previous=" << std::dec << previousBlocks[i]->anchor << " and current=" << currentBlocks[i]->anchor);
        if (lockedPrevBlock != previousBlocks[i]) {
            previousBlocks[i]->mu.unlock();
            lockedPrevBlock = previousBlocks[i];
        }
        if (lockedCurrBlock != currentBlocks[i]) {
            currentBlocks[i]->mu.unlock();
            lockedCurrBlock = currentBlocks[i];
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
    restart:
    // Walk down BSL
    int64_t currentVersion = head->forward[maxlevel]->version, previousVersion = head->version;
    BSLBlock* currentBlock = head->forward[maxlevel], *previousBlock = head;
    for (auto i = maxlevel; i >= 0; i--) {
        if (currentVersion != currentBlock->version
         || previousVersion != previousBlock->version
         || previousBlock->forward[i] != currentBlock) goto restart;

        // walk forward until next anchor is larger than key
        // this guarantees that the next node can not contain our key
        while (currentBlock->anchor <= min) {
            if (currentVersion != currentBlock->version
             || previousVersion != previousBlock->version
             || previousBlock->forward[i] != currentBlock) goto restart;

            previousVersion = currentVersion;
            previousBlock = currentBlock;
            currentVersion = currentBlock->forward[i]->version;
            currentBlock = currentBlock->forward[i];

        }
        if (i > 0) {
            currentVersion = previousBlock->forward[i-1]->version;
            currentBlock = previousBlock->forward[i-1];
        }
    }

    // just walk forward
    ::data_structures::Interface::SumResult result;
    while (currentBlock->anchor <= max) {
        if (currentVersion != currentBlock->version
         || previousVersion != previousBlock->version
         || previousBlock->forward[0] != currentBlock) {
             goto restart;
         }
        
        previousVersion = currentVersion;
        previousBlock = currentBlock;
        currentVersion = currentBlock->forward[0]->version;
        currentBlock = currentBlock->forward[0];
    }


    return ::data_structures::Interface::SumResult();
}

std::unique_ptr<::data_structures::Iterator> BSL::iterator() const {
    return nullptr;
}

void BSL::dump() const {
    // Dump out nodes at level 0
    #ifdef DEBUG
    lock_guard<mutex> guard(_local_mutex);
    #endif
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
        for (auto i = 0; i < node->length; i++) {
          std::cout << node->values[i].key << " ";
        }
        std::cout << "}]" << std::endl;
        node = node->forward[0];
    }
}

}

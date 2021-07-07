#include "bsl.hpp"
#include <algorithm>


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

BSL::BSL(float p, int64_t maxlevel, int64_t maxblksize) {
    this->p = p;
    this->maxlevel = maxlevel;
    this->maxblksize = maxblksize;
    this->level = 0;

    // Construct empty BSL
    int64_t anchorBot0 = std::numeric_limits<int64_t>::min();
    int64_t anchorBot1 = std::numeric_limits<int64_t>::min()+1;
    int64_t anchorTop0 = std::numeric_limits<int64_t>::max();

    head = new BSLBlock(anchorBot0, maxlevel);
    auto base = new BSLBlock(anchorBot1, maxlevel);
    auto tail = new BSLBlock(anchorTop0, maxlevel);

    head->forward[0] = base;
    base->forward[0] = tail;

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
    // Remember nodes to update in case of overflow
    std::vector<int64_t> previous_versions(maxlevel);
    std::vector<BSLBlock*> previous_blocks(maxlevel);
    std::vector<int64_t> current_versions(maxlevel);
    std::vector<BSLBlock*> current_blocks(maxlevel);

    current_versions[level] = head->version;
    current_blocks[level] = head;

    COUT_DEBUG("Inserting key=" << key << " on level=" << level);

    // Walk down levels
    for (auto i = level; i >= 0; i--) {
        if (current_versions[i] != current_blocks[i]->version) goto restart;
        if (previous_blocks[i] && previous_versions[i] != previous_blocks[i]->version) goto restart;

        COUT_DEBUG("Checking node=" << std::hex << current->forward[i] << " with level=" << i);
        // walk forward until next anchor is larger than key
        // this guarantees that the next node can not contain our key
        while (current_blocks[i]->forward[i] != nullptr
               && current_blocks[i]->forward[i]->anchor <= key) {
            if (current_versions[i] != current_blocks[i]->version) goto restart;
            if (previous_blocks[i] && previous_versions[i] != previous_blocks[i]->version) goto restart;

            previous_versions[i] = current_versions[i];
            previous_blocks[i] = current_blocks[i];

            current_versions[i] = current_blocks[i]->forward[i]->version;
            current_blocks[i] = current_blocks[i]->forward[i];

            COUT_DEBUG("Skipping forward to node=" << std::hex << current << " with level=" << i);
        }
    }

    // Walk down and lock every preds on the path if they are not the same
    BSLBlock* prevBlock = nullptr;
    for (auto i = level; i >= 0; i--) {
        if (current_blocks[i] != prevBlock) {
        }
    }


    COUT_DEBUG("Inserting on anchor=" << std::dec << current->anchor);

    // do linear search
    if (current->insert(key, value)) cardinality++;

    // check if everything is fine
    if (current->values.size() <= maxblksize) {
        return;
    }

    // rebalance by splitting node
    COUT_DEBUG("Current node reached size of " << current->values.size() << ", need to rebalance");
    // split node here
    // we need to insert another node after the current one
    // first: determine next anchor via sort
    // TODO: is there a better method to do this?
    std::sort(current->values.begin(), current->values.end(), [](auto x, auto y) { return x.key < y.key; });
    // pick the value in the middle
    auto anchorIt = current->values.begin() + (current->values.size()/2);
    BSLBlock* next = new BSLBlock(anchorIt->key, maxlevel);
    next->values.reserve(current->values.end() - anchorIt);
    next->values.insert(next->values.begin(), anchorIt, current->values.end());
    current->values.resize(anchorIt - current->values.begin());

    // else we need to add a new node
    int rlevel = randLevel();
    if (rlevel > level) {
        for (auto i = level+1; i <= rlevel; i++) {
            updates[i] = head;
        }
        level = rlevel;
    }

    for (auto i = 0; i <= rlevel; i++) {
      next->forward[i] = updates[i]->forward[i];
      updates[i]->forward[i] = next;
    }

    COUT_DEBUG("Inserted key=" << key);
}

int64_t BSL::find(int64_t key) const {
    // Walk down BSL
    BSLBlock* current = head;
    for (auto i = level; i >= 0; i--) {
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
        for (auto i = 0; i <= level; i++) {
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

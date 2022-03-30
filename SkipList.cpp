#include <iostream>
#include <cstdlib>

#include "SkipList.h"

int SkipList::MAX_LEVEL = 9;
double SkipList::p = 0.25;

void SkipList::init() {
    length = 0;
    value_size = 0;
    key_min = ULONG_LONG_MAX;
    key_max = 0;
    head = new SKNode(MAX_LEVEL, 0, "", SKNodeType::HEAD);
    tail = new SKNode(1, ULONG_LONG_MAX, "", SKNodeType::NIL);
    level = 1;

    for (int i = 0; i < MAX_LEVEL; i++) {
        head->forwards[i] = tail;
    }
}

SkipList::SkipList() {
    srand((unsigned) time(nullptr));
    update_create();
    init();
}

void SkipList::finish() {
    SKNode *n1 = head;
    SKNode *n2;
    while (n1) {
        n2 = n1->forwards[0];
        delete n1;
        n1 = n2;
    }
    update_clear();
}

SkipList::~SkipList() {
    finish();
}

int SkipList::randomLevel() {
    int result = 1;
    while (result < MAX_LEVEL && rand() / double(RAND_MAX) < p) {
        ++result;
    }
    return result;
}

void SkipList::update_create() {
    for (int i = 0; i < MAX_LEVEL; ++i) {
        update.push_back(nullptr);
    }
}

void SkipList::update_clear() {
    for (int i = 0; i < MAX_LEVEL; ++i) {
        update[i] = nullptr;
    }
}


void SkipList::put(uint64_t key, const std::string &value) {
    key_min = key < key_min ? key : key_min;
    key_max = key > key_max ? key : key_max;
    update_clear();
    SKNode *x = head;
    for (int i = level - 1; i >= 0; i--) {
        while (x->forwards[i]->key < key) {
            x = x->forwards[i];
        }
        update[i] = x;
    }
    x = x->forwards[0];
    value_size += value.length();
    if (x->key == key) {
        value_size -= x->val.length();
        redo_value = x->val;
        x->val = value;
        return;
    }
    redo_value = "";
    length++;
    int v = randomLevel();
    if (v > level) {
        for (int i = level; i < v; i++) {
            update[i] = head;
        }
        level = v;
    }
    x = new SKNode(v, key, value, NORMAL);
    for (int i = 0; i < v; i++) {
        x->forwards[i] = update[i]->forwards[i];
        update[i]->forwards[i] = x;
    }
}


std::string SkipList::get(uint64_t key) {
    SKNode *x = head;
    for (int i = level - 1; i >= 0; i--) {
        while (x->forwards[i]->key < key) {
            x = x->forwards[i];
        }
    }
    x = x->forwards[0];
    if (x->key == key) {
        return x->val;
    } else {
        return NOTFOUND;
    }
}

bool SkipList::del(uint64_t key) {
    update_clear();
    SKNode *x = head;
    for (int i = level - 1; i >= 0; i--) {
        while (x->forwards[i]->key < key) {
            x = x->forwards[i];
        }
        update[i] = x;
    }
    x = x->forwards[0];
    if (x->key != key) {
        return false;
    }
    for (int i = 0; i < level; i++) {
        if (update[i]->forwards[i] != x) {
            break;
        }
        update[i]->forwards[i] = x->forwards[i];
    }
    value_size -= x->val.length();
    length--;
    delete x;
    while (level > 1 && head->forwards[level - 1] == tail) {
        level -= 1;
    }
    return true;
}

void SkipList::scan(uint64_t key1, uint64_t key2, KVheap &heap) {
    SKNode *x = head;
    for (int i = MAX_LEVEL - 1; i >= 0; i--) {
        while (x->forwards[i]->key < key1) {
            x = x->forwards[i];
        }
    }
    x = x->forwards[0];
    while (x->key <= key2) {
        heap.push(std::make_pair(x->key, x->val));
        x = x->forwards[0];
    }
}

void SkipList::reset() {
    finish();
    init();
}

void SkipList::getList(std::list<std::pair<uint64_t, std::string>> &list) {
    SKNode *x = head;
    x = x->forwards[0];
    while (x->type != NIL) {
        list.emplace_back(x->key, x->val);
        x = x->forwards[0];
    }
}

void SkipList::getRange(uint64_t &min, uint64_t &max) const
{
    min = key_min;
    max = key_max;
}

uint64_t SkipList::getLength() const
{
    return length;
}
unsigned int SkipList::getSize() const
{
    return value_size;
}

void SkipList::redo(const key_t key)
{
    if(redo_value.empty()){
        del(key);
        return;
    }
    put(key, redo_value);
}
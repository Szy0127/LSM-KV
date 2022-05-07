//
// Created by Shen on 2022/3/21.
//

#include <iostream>
#include "kvstore.h"

#include <map>
#include <chrono>

#include <algorithm>


std::string path = "../data/";

int main() {
    KVStore kvStore("../data");
    kvStore.reset();
}


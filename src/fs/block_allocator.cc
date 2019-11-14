/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2019 ScyllaDB
 */
#include "fs/block_allocator.hh"
#include <cassert>

namespace seastar {

block_allocator::block_allocator(std::unordered_map<size_t, bool> bp, std::queue<size_t> fb)
        : _block_pool(std::move(bp)), _free_blocks(std::move(fb)) {};

size_t block_allocator::alloc() {
    assert(!_free_blocks.empty());
    size_t ret = _free_blocks.front();
    _free_blocks.pop();
    _block_pool[ret] = false;
    return ret;
}

void block_allocator::free(size_t addr) {
    assert(_block_pool.count(addr) == 1);
    assert(!_block_pool[addr]);
    _free_blocks.push(addr);
    _block_pool[addr] = true;
}

}

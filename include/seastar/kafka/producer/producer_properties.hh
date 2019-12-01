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
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#pragma once

#include <vector>
#include <string>
#include <functional>

#include "../../../../src/kafka/utils/partitioner.hh"

#include <seastar/util/bool_class.hh>
#include <seastar/kafka/utils/defaults.hh>

namespace seastar {

namespace kafka {

enum class ack_policy {
    NONE = 0,
    LEADER = 1,
    ALL = -1,
};

struct enable_idempotence_tag {};
using enable_idempotence = bool_class<enable_idempotence_tag>;

class producer_properties {

public:

    ack_policy _acks = ack_policy::LEADER;
    enable_idempotence _enable_idempotence = enable_idempotence::no;

    uint16_t _linger = 0;
    uint16_t _request_size = 50000;
    uint16_t _max_in_flight = 5;

    uint32_t _buffer_memory = 33554432;
    uint32_t _retries = 10;
    uint32_t _batch_size = 16384;
    uint32_t _request_timeout = 500;
    uint32_t _metadata_refresh = 300000;

    std::string _client_id {};
    std::string _transactional_id {};
    std::vector<std::pair<std::string, uint16_t>> _servers {};

    std::unique_ptr<partitioner> _partitioner = defaults::round_robin_partitioner();
    std::function<future<>(uint32_t)> _retry_backoff_strategy = defaults::exp_retry_backoff(20, 1000);

};

}

}

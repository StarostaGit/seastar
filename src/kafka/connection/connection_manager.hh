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

#include "kafka_connection.hh"
#include "../protocol/metadata_response.hh"
#include "../protocol/metadata_request.hh"

#include <unordered_map>

namespace seastar {

namespace kafka {

class connection_manager {

    std::unordered_map<std::string, lw_shared_ptr<kafka_connection>> _connections;
    std::string _client_id;

public:

    explicit connection_manager(const std::string& client_id)
        : _client_id(client_id) {};

    future<> connect(const std::string& host, uint16_t port);
    std::optional<lw_shared_ptr<kafka_connection>> get_connection(const std::string& host);
    future<> disconnect(const std::string& host);

    future<metadata_response> ask_for_metadata(const metadata_request& request);

};

}

}
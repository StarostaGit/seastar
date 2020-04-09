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

#include "connection_manager.hh"
#include <seastar/core/thread.hh>

namespace seastar {

namespace kafka {

future<lw_shared_ptr<kafka_connection>> connection_manager::connect(const std::string& host, uint16_t port, uint32_t timeout) {
    return with_semaphore(_connect_semaphore, 1, [this, host, port, timeout] {
        auto conn = _connections.find({host, port});
        return conn != _connections.end()
               ? make_ready_future<lw_shared_ptr<kafka_connection>>(conn->second)
               : kafka_connection::connect(host, port, _client_id, timeout)
               .then([this, host, port] (lw_shared_ptr<kafka_connection> conn) {
                    _connections.insert({{host, port}, conn});
                    return conn;
                });
    });
}

future<> connection_manager::init(const std::vector<connection_id>& servers, uint32_t request_timeout) {
    std::vector<future<>> fs;

    fs.reserve(servers.size());

    for (auto& server : servers) {
        fs.push_back(connect(server.first, server.second, request_timeout).discard_result());
    }

    return when_all_succeed(fs.begin(), fs.end()).discard_result();
}

lw_shared_ptr<kafka_connection> connection_manager::get_connection(const connection_id& connection) {
    auto conn = _connections.find(connection);
    return conn != _connections.end() ? conn->second : nullptr;
}

future<> connection_manager::disconnect(const connection_id& connection) {
    auto conn = _connections.find(connection);
    if (conn != _connections.end()) {
        auto conn_ptr = conn->second;
        _connections.erase(conn);
        return conn_ptr->close().finally([conn_ptr]{});
    }
    return make_ready_future();
}

future<lw_shared_ptr<const metadata_response>> connection_manager::ask_for_metadata(seastar::kafka::metadata_request&& request) {
    lw_shared_ptr<const metadata_response> metadata;
    auto conn = _connections.begin();

    return seastar::repeat([this, request = std::move(request), &metadata, &conn] {
            if (conn == _connections.end()) {
                throw metadata_refresh_exception("No brokers responded.");
            }
            return conn->second->send(request).then([this, &metadata, &conn](metadata_response res){
                if (res._error_code == error::kafka_error_code::NONE) {
                    metadata = make_lw_shared<const metadata_response>(std::move(res));
                    return seastar::stop_iteration::yes;
                }
                else {
                    conn = _connections.upper_bound(conn->first);
                    return seastar::stop_iteration::no;
                }
            });
    }).then([&metadata]{
        return metadata;
    });
}

}

}

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

#include <chrono>

#include "../connection/connection_manager.hh"
#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>

namespace seastar {

namespace kafka {

class metadata_manager {

private:
    lw_shared_ptr<connection_manager> _connection_manager;
    metadata_response _metadata;
    bool _keep_refreshing;
    semaphore _refresh_finished;
    semaphore _metadata_sem;
    abort_source _stop_refresh;

public:
    metadata_manager(lw_shared_ptr<connection_manager>& manager)
    : _connection_manager(manager), _refresh_finished(0), _metadata_sem(1) {}

    seastar::future<> refresh_coroutine(std::chrono::seconds dur);
    seastar::future<> refresh_metadata();
    void start_refresh();
    void stop_refresh();
    seastar::future<metadata_response> get_metadata();

};

}

}

/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "hazelcast/util/export.h"

namespace hazelcast {
namespace client {
namespace config {

class HAZELCAST_API client_tpc_config
{
public:

    /**
     * Gets the enabled flag which defines TPC is enabled or not.
     *
     * @return true if TPC is enabled
     */
    bool is_enabled() const;

    /**
     * Sets the enabled flag which defines TPC is enabled or not. Can't
     * return null.
     *
     * @param enabled a boolean to enable or disable TPC
     * @return this
     */
    client_tpc_config& set_enabled(bool enabled);

private:

    bool enabled_;
};

std::ostream HAZELCAST_API & operator<<(std::ostream& os, const client_tpc_config& cfg);

}
}
}
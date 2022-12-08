/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

#include <gtest/gtest.h>

#include "hazelcast/client/hazelcast_client.h"
#include "../HazelcastServerFactory.h"
#include "../HazelcastServer.h"
#include "../remote_controller_client.h"

namespace hazelcast {
namespace client {
namespace test {
namespace compact {

class compact_test_base : public testing::Test
{
public:
    using schema_t = serialization::pimpl::schema;

    compact_test_base()
      : factory_{ "hazelcast/test/resources/compact.xml" }
      , member_{ factory_ }
      , client{ new_client().get() }
    {
        remote_controller_client().ping();
    }

protected:

    HazelcastServerFactory factory_;
    HazelcastServer member_;
    hazelcast_client client;
};

}
}
}
}
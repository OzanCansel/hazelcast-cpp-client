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

#include <memory>
#include <fstream>
#include <sstream>

#include <gtest/gtest.h>

#include "hazelcast/client/hazelcast_client.h"
#include "serializables_for_compact.h"
#include "../HazelcastServerFactory.h"
#include "../HazelcastServer.h"
#include "../remote_controller_client.h"

#if defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
#pragma warning(push)
#pragma warning(disable : 4996) // for unsafe getenv
#endif

namespace hazelcast {
namespace client {
namespace test {
namespace compact {

class CompactSchemaReplicationOnWrite : public ::testing::Test
{
private:

    HazelcastServerFactory factory_;
    HazelcastServer member_;

public:

    using schema_t = serialization::pimpl::schema;

    CompactSchemaReplicationOnWrite()
        :   factory_ {"hazelcast/test/resources/compact.xml"}
        ,   member_ {factory_}
        ,   client {new_client().get()}
    {
        remote_controller_client().ping();
    }

protected:

    template<typename T>
    schema_t get_schema()
    {
        return serialization::pimpl::schema_of<T>::schema_v;
    }

    bool check_schema_on_backend(const schema_t& schema)
    {
        std::ifstream ifs { "script.js" };
        Response response;

        std::stringstream script;

        script << ifs.rdbuf();

        remote_controller_client().executeOnController(
            response,
            factory_.get_cluster_id(),
            script.str(),
            // R"(
            //     var schemas = instance_0.getOriginal().node.getSchemaService().getAllSchemas();

            //     result = schemas;
            // )",
            Lang::JAVASCRIPT
        );

        std::cout << response.result << std::endl;

        return false;
    }

    hazelcast_client client;
};

TEST_F(CompactSchemaReplicationOnWrite, test_write)
{
    auto schema_parent = get_schema<a_type>();
    auto schema_child = get_schema<nested_type>();

    check_schema_on_backend(schema_parent);

    auto map = client.get_map("imap").get();
}

}

}
}
}

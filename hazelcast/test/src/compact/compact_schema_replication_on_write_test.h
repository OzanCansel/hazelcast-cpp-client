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
#include <sstream>
#include <functional>

#include <gtest/gtest.h>

#include "hazelcast/client/hazelcast_client.h"
#include "compact_test_base.h"
#include "serializables_for_compact.h"
#include "../HazelcastServerFactory.h"
#include "../HazelcastServer.h"
#include "../remote_controller_client.h"
#include "../TestHelperFunctions.h"

#if defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
#pragma warning(push)
#pragma warning(disable : 4996) // for unsafe getenv
#endif

namespace hazelcast {
namespace client {
namespace test {
namespace compact {

class CompactSchemaReplicationOnWrite : public compact_test_base
{
protected:
    using schema_t = serialization::pimpl::schema;

    template<typename U>
    schema_t get_schema()
    {
        return serialization::pimpl::schema_of<U>::schema_v;
    }

    bool check_schema_on_backend(const schema_t& schema)
    {
        Response response;

        remote_controller_client().executeOnController(
          response,
          factory_.get_cluster_id(),
          (boost::format(
             R"(
                        var schemas = instance_0.getOriginal().node.getSchemaService().getAllSchemas();
                        var iterator = schemas.iterator();

                        var exist = false;
                        while(iterator.hasNext()){
                            var schema = iterator.next();

                            if (schema.getSchemaId() == "%1%")
                                exist = true;
                        }

                        result = "" + exist;
                    )") %
           schema.schema_id())
            .str(),
          Lang::JAVASCRIPT);

        return response.result == "true";
    }
};

TEST_F(CompactSchemaReplicationOnWrite, imap_put)
{
    auto schema_parent = this->get_schema<a_type>();
    auto schema_child = this->get_schema<nested_type>();

    ASSERT_EQ(this->check_schema_on_backend(schema_parent), false);
    ASSERT_EQ(this->check_schema_on_backend(schema_child), false);

    auto map = client.get_map(random_string()).get();

    map->put(random_string(), a_type{}).get();

    ASSERT_EQ(this->check_schema_on_backend(schema_parent), true);
    ASSERT_EQ(this->check_schema_on_backend(schema_child), true);
}

} // namespace compact
} // namespace test
} // namespace client
} // namespace hazelcast

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

#include "hazelcast/client/serialization/generic_record_builder.h"
#include "compact_test_base.h"

namespace hazelcast {
namespace client {
namespace test {
namespace compact {

struct CompactGenericRecordWrite : compact_test_base
{};

TEST_F(CompactGenericRecordWrite, write)
{
    using namespace serialization;

    auto nested1 = generic_record_builder{"nested1"};
    auto nested2 = generic_record_builder{"nested2"};
    auto builder = generic_record_builder{"gen_rec_type"};

    auto record1 = nested1.set_string("str", "nested1").build();
    auto record2 = nested2.set_string("str", "nested2").build();
    auto record3 = record1.new_builder_with_clone().set_boolean("b", true).build();
    auto record4 = record1.new_builder().set_boolean("b", true).build();
    auto record  = builder.set_boolean("b1", true)
                          .set_boolean("b2", false)
                          .set_string("str" , "Ozan Cansel")
                          .set_array_of_boolean("aob", { true, false } )
                          .set_array_of_string("aos", { "test" , "training" })
                          .set_array_of_generic_record("aog", { record1, record2, record3, record4 })
                          .build();

    auto map = client.get_map(random_string()).get();

    auto key = random_string();
    map->put(key, record).get();
    generic_record value = map->get<std::string, generic_record>(key).get().value();

    bool                                                          b1  = value.get_boolean("b1");
    bool                                                          b2  = value.get_boolean("b2");
    boost::optional<std::string>                                  str = value.get_string("str");
    boost::optional<std::vector<bool>>                            aob = value.get_array_of_boolean("aob");
    boost::optional<std::vector<boost::optional<std::string>>>    aos = value.get_array_of_string("aos");
    boost::optional<std::vector<boost::optional<generic_record>>> aog = value.get_array_of_generic_record("aog");
}

}
}
}
}
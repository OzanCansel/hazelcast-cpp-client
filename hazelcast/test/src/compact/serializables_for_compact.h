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

#include "hazelcast/client/serialization/serialization.h"

#if defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
#pragma warning(push)
#pragma warning(disable : 4996) // for unsafe getenv
#endif

namespace hazelcast {
namespace client {
namespace test {
namespace compact {

struct nested_type
{
    int y;
};

struct a_type
{
    int x;
    nested_type nested;
};

} // namespace compact
} // namespace test

namespace serialization {

template<>
struct hz_serializer<test::compact::nested_type> : compact_serializer
{
    static void write(const test::compact::nested_type& object,
                      compact_writer& writer)
    {
        writer.write_int32("x", object.y);
    }

    static test::compact::nested_type read(compact_reader& reader)
    {
        test::compact::nested_type object;

        object.y = reader.read_int32("y");

        return object;
    }

    static std::string type_name() { return "nested_type"; }
};

template<>
struct hz_serializer<test::compact::a_type> : compact_serializer
{
    static void write(const test::compact::a_type& object,
                      compact_writer& writer)
    {
        writer.write_boolean("x", object.x);
        writer.write_compact<test::compact::nested_type>("nested",
                                                         object.nested);
    }

    static test::compact::a_type read(compact_reader& reader)
    {
        test::compact::a_type object;

        object.x = reader.read_int32("x");
        object.nested =
          *reader.read_compact<test::compact::nested_type>("nested");

        return object;
    }

    static std::string type_name() { return "a_type"; }
};

} // namespace serialization
} // namespace client
} // namespace hazelcast
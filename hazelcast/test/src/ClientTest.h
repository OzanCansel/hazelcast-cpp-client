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

#include <utility>
#include <iterator>
#include <boost/uuid/uuid.hpp>

#include <gtest/gtest.h>

#include <hazelcast/client/client_config.h>
#include <hazelcast/client/imap.h>
#include <hazelcast/logger.h>

namespace hazelcast {
namespace client {

class member;

namespace spi {
class ClientContext;
}

namespace test {

template<typename T, typename = void>
struct generator;
struct key_int_generator;

class HazelcastServerFactory;

class ClientTest : public ::testing::Test
{
public:
    using imap_t = std::shared_ptr<hazelcast::client::imap>;

    ClientTest();

    logger& get_logger();

    static std::string get_test_name();
    static std::string get_ca_file_path();
    static std::string random_map_name();
    static std::string random_string();
    static boost::uuids::uuid generate_key_owned_by(spi::ClientContext& context,
                                                    const member& member);
    static client_config get_config(bool ssl_enabled = false,
                                    bool smart = true);
    static std::string get_ssl_cluster_name();
    static hazelcast_client get_new_client();
    static std::string get_ssl_file_path();
    static HazelcastServerFactory& default_server_factory();

    template<typename Value = int,
             typename Key = int,
             typename Generator = generator<Value>,
             typename KeyGenerator = key_int_generator>
    std::unordered_map<Key, Value> populate_map(
      imap_t map,
      int n_entries = 10,
      Generator&& value_gen = Generator{},
      KeyGenerator&& key_gen = KeyGenerator{})
    {
        std::unordered_map<Key, Value> entries;
        entries.reserve(n_entries);

        generate_n(
          inserter(entries, end(entries)), n_entries, [&value_gen, &key_gen]() {
              return std::make_pair(key_gen(), value_gen());
          });

        for (const auto& p : entries) {
            map->put(p.first, p.second);
        }

        return entries;
    }

private:
    std::shared_ptr<logger> logger_;
};

template<typename T>
struct generator<T, typename std::enable_if<std::is_integral<T>::value>::type>
{
    T operator()() { return T(rand()); }
};

struct key_int_generator
{
    int x{};

    int operator()() { return x++; }
};

template<>
struct generator<std::string>
{
    std::string operator()() { return ClientTest::random_map_name(); }
};

template<>
struct generator<hazelcast_json_value>
{
    hazelcast_json_value operator()()
    {
        generator<std::string> str_gen;
        generator<int> int_gen;

        return hazelcast_json_value{ (boost::format(
                                        R"({
                        "name" : "%1%" ,
                        "value" : %2%
                    })") % str_gen() % int_gen())
                                       .str() };
    }
};

} // namespace test
} // namespace client
} // namespace hazelcast

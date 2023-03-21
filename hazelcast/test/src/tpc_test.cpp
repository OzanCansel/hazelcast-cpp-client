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

#include <gtest/gtest.h>

#include "hazelcast/client/hazelcast_client.h"
#include "hazelcast/client/connection/ClientConnectionManagerImpl.h"
#include "HazelcastServerFactory.h"
#include "HazelcastServer.h"
#include "TestHelperFunctions.h"

namespace hazelcast {
namespace client {
namespace test {

class TpcTest : public ::testing::Test
{
protected:

    static constexpr int NUM_OF_TPC_CHANNELS = 4;

    client_config base_config()
    {
        client_config config;
        config.set_cluster_name("tpc-cluster");

        return config;
    }

    static spi::ClientContext context(hazelcast_client client)
    {
        return spi::ClientContext {client};
    }

    static std::vector<std::shared_ptr<connection::Connection>> connections(hazelcast_client client)
    {
        return spi::ClientContext {client}.get_connection_manager().get_active_connections();
    }

    static void ensure_that_there_are_n_connections(hazelcast_client client, int n, int seconds = 5)
    {
        ASSERT_TRUE_EVENTUALLY_WITH_TIMEOUT((connections(client).size() == n), seconds);
    }

    static void ensure_that_there_are_no_tpc_channels(hazelcast_client client, int seconds = 2)
    {
        ensure_that_there_are_n_tpc_channels(client, 0, seconds);
    }

    static void ensure_that_there_are_n_tpc_channels(hazelcast_client client, std::size_t expected, int seconds = 2)
    {
        auto fn = [expected](hazelcast_client client){
            auto conns = connections(client);

            return all_of(
                begin(conns),
                end(conns),
                [expected](std::shared_ptr<connection::Connection> conn){
                    return conn->get_tpc_channels().size() == expected;
                }
            );
        };

        if (expected != 0) {
            ASSERT_TRUE_EVENTUALLY_WITH_TIMEOUT(fn(client), seconds * 2);
        }
        ASSERT_TRUE_ALL_THE_TIME(fn(client), seconds);
    }
};

TEST_F(TpcTest, connect_to_tpc_disabled_cluster_with_tpc_enabled_client)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-disabled.xml" };
    HazelcastServer member { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(true);

    auto client = new_client(std::move(config)).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_no_tpc_channels(client);

    auto map = client.get_map(random_string()).get();

    map->put(1,2).get();
    auto result = map->get<int,int>(1).get();

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(*result, 2);

    client.shutdown().get();
}

TEST_F(TpcTest, connect_to_tpc_enabled_cluster_with_tpc_disabled_client)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-enabled.xml" };
    HazelcastServer member { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(false);

    auto client = new_client(std::move(config)).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_no_tpc_channels(client);

    auto map = client.get_map(random_string()).get();

    map->put(1,2).get();
    auto result = map->get<int,int>(1).get();

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 2);

    client.shutdown().get();
}

TEST_F(TpcTest, connect_to_tpc_enabled_cluster_with_tpc_enabled_client)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-enabled.xml" };
    HazelcastServer member { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(true);

    auto client = new_client(std::move(config)).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_n_tpc_channels(client, 4);

    auto map = client.get_map(random_string()).get();

    map->put(1,2).get();
    auto result = map->get<int,int>(1).get();

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(*result, 2);

    client.shutdown().get();
}

TEST_F(TpcTest, connect_to_tpc_channels_of_a_newly_joined_member)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-enabled.xml" };
    HazelcastServer member_1 { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(true);

    auto client = new_client(std::move(config)).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_n_tpc_channels(client, 4);

    HazelcastServer member_2 { factory };

    ensure_that_there_are_n_connections(client, 2);
    ensure_that_there_are_n_tpc_channels(client, 4);

    client.shutdown().get();
}

TEST_F(TpcTest, connect_to_tpc_channels_after_cluster_restarts)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-enabled.xml" };
    HazelcastServer member_1 { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(true);

    auto client = new_client(std::move(config)).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_n_tpc_channels(client, 4);

    member_1.shutdown();

    ensure_that_there_are_n_connections(client, 0);

    HazelcastServer member_2 { factory };

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_n_tpc_channels(client, 4);

    client.shutdown().get();
}

TEST_F(TpcTest, route_partition_specific_invocations_to_tpc_channels)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-enabled.xml" };
    HazelcastServer member_1 { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(true);

    auto client = new_client(std::move(config)).get();

    auto map = client.get_map(random_string()).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_n_tpc_channels(client, 4);

    auto connection = connections(client).front();
    for (int channel_idx = 0; channel_idx < NUM_OF_TPC_CHANNELS; ++channel_idx) {
        for (int i = 0; i < 1000; ++i) {
            auto& channels = connection->get_tpc_channels();
            auto last_write_time_before = channels.at(channel_idx)->last_write_time();

            auto request = protocol::codec::client_ping_encode();
            request.set_partition_id(channel_idx);
            auto ctx = context(client);
            std::shared_ptr<spi::impl::ClientInvocation> clientInvocation =
                spi::impl::ClientInvocation::create(ctx, request, "", connection);
            clientInvocation->invoke_urgent().get();
            auto last_write_time_after = channels.at(channel_idx)->last_write_time();

            EXPECT_NE(last_write_time_before, last_write_time_after);
        }
    }

    client.shutdown().get();
}

TEST_F(TpcTest, route_non_partition_specific_invocations_to_legacy_connections)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-enabled.xml" };
    HazelcastServer member_1 { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(true);

    auto client = new_client(std::move(config)).get();

    auto map = client.get_map(random_string()).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_n_tpc_channels(client, 4);

    auto connection = connections(client).front();
    for (int channel_idx = 0; channel_idx < NUM_OF_TPC_CHANNELS; ++channel_idx) {
        for (int i = 0; i < 1000; ++i) {
            auto last_write_time_before = connection->last_write_time();

            auto request = protocol::codec::client_ping_encode();
            auto ctx = context(client);
            std::shared_ptr<spi::impl::ClientInvocation> clientInvocation =
                spi::impl::ClientInvocation::create(ctx, request, "", connection);
            clientInvocation->invoke_urgent().get();
            auto last_write_time_after = connection->last_write_time();

            EXPECT_NE(last_write_time_before, last_write_time_after);
        }
    }

    client.shutdown().get();
}

TEST_F(TpcTest, tpc_enabled_client_shutdowns_cleanly)
{
    HazelcastServerFactory factory { "hazelcast/test/resources/tpc-enabled.xml" };
    HazelcastServer member_1 { factory };

    client_config config = base_config();

    config.get_tpc_config().set_enabled(true);

    auto client = new_client(std::move(config)).get();

    auto map = client.get_map(random_string()).get();

    ensure_that_there_are_n_connections(client, 1);
    ensure_that_there_are_n_tpc_channels(client, 4);

    client.shutdown().get();
}

}
}
}
/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
#include <hazelcast/client/HazelcastClient.h>

int main() {
    hazelcast::client::HazelcastClient hz;

    auto map = hz.getReplicatedMap("map");

    std::string listenerId = map->addEntryListener(
        hazelcast::client::EntryListener {
            /*entryAdded = */ [](const hazelcast::client::EntryEvent &event) {
                std::cout << "[entryAdded] " << event << std::endl;
            },
            /*entryRemoved = */ [](const hazelcast::client::EntryEvent &event) {
                std::cout << "[entryRemoved] " << event << std::endl;
            },
            /*entryUpdated = */ [](const hazelcast::client::EntryEvent &event) {
                std::cout << "[entryAdded] " << event << std::endl;
            },
            /*entryEvicted = */ [](const hazelcast::client::EntryEvent &event) {
                std::cout << "[entryUpdated] " << event << std::endl;
            },
            /*entryExpired = */ [](const hazelcast::client::EntryEvent &event) {
                std::cout << "[entryExpired] " << event << std::endl;
            },
            /*entryMerged = */ [](const hazelcast::client::EntryEvent &event) {
                std::cout << "[entryMerged] " << event << std::endl;
            },
            /*mapEvicted = */ [](const hazelcast::client::MapEvent &event) {
                std::cout << "[mapEvicted] " << event << std::endl;
            },
            /*mapCleared = */ [](const hazelcast::client::MapEvent &event) {
                std::cout << "[mapCleared] " << event << std::endl;
            }
        }
    ).get();

    std::cout << "EntryListener registered with id " << listenerId << std::endl;

    return 0;
}

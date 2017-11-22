/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
#ifndef HAZELCAST_CLIENT_TYPEDDATA_H_
#define HAZELCAST_CLIENT_TYPEDDATA_H_

#include <memory>

#include "hazelcast/util/HazelcastDll.h"
#include "hazelcast/client/serialization/pimpl/SerializationService.h"

#if  defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
#pragma warning(push)
#pragma warning(disable: 4251) //for dll export
#endif

namespace hazelcast {
    namespace client {
        namespace serialization {
            namespace pimpl {
                class Data;
            }
        }
        class HAZELCAST_API TypedData {
        public:
            TypedData();

            TypedData(std::auto_ptr<serialization::pimpl::Data> data,
                      serialization::pimpl::SerializationService &serializationService);

            TypedData(const TypedData &rhs);

            TypedData &operator=(const TypedData& rhs);

            virtual ~TypedData();

            const serialization::pimpl::ObjectType getType() const;

            template <typename T>
            std::auto_ptr<T> get() const {
                return ss->toObject<T>(data.get());
            }

            const serialization::pimpl::Data *getData() const;

        private:
            std::auto_ptr<serialization::pimpl::Data> data;
            serialization::pimpl::SerializationService *ss;
        };

        bool HAZELCAST_API operator<(const TypedData &lhs, const TypedData &rhs);
    }
}

#if  defined(WIN32) || defined(_WIN32) || defined(WIN64) || defined(_WIN64)
#pragma warning(pop)
#endif

#endif //HAZELCAST_CLIENT_TYPEDDATA_H_

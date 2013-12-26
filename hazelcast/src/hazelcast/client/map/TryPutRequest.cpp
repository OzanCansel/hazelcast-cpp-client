//
// Created by sancar koyunlu on 9/4/13.
// Copyright (c) 2013 hazelcast. All rights reserved.


#include "hazelcast/client/map/TryPutRequest.h"
#include "hazelcast/client/serialization/Data.h"
#include "hazelcast/client/map/PortableHook.h"
#include "hazelcast/client/serialization/PortableWriter.h"


namespace hazelcast {
    namespace client {
        namespace map {
            TryPutRequest::TryPutRequest(const std::string& name, serialization::Data& key, serialization::Data& value, int threadId, long timeout)
            :name(name)
            , key(key)
            , value(value)
            , threadId(threadId)
            , ttl(-1)
            , timeout(timeout) {

            };

            int TryPutRequest::getFactoryId() const {
                return PortableHook::F_ID;
            }

            int TryPutRequest::getClassId() const {
                return PortableHook::TRY_PUT;
            }

            void TryPutRequest::write(serialization::PortableWriter& writer) const {
                writer.writeLong("timeout", timeout);
                writer.writeUTF("n", name);
                writer.writeInt("t", threadId);
                writer.writeLong("ttl", ttl);
                serialization::ObjectDataOutput& out = writer.getRawDataOutput();
                key.writeData(out);
                value.writeData(out);
            };
        }
    }
}

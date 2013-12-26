//
// Created by sancar koyunlu on 5/23/13.
// Copyright (c) 2013 hazelcast. All rights reserved.

#include "hazelcast/client/map/DeleteRequest.h"
#include "hazelcast/client/map/PortableHook.h"
#include "hazelcast/client/serialization/PortableWriter.h"
#include "hazelcast/client/serialization/Data.h"

namespace hazelcast {
    namespace client {
        namespace map {
            DeleteRequest::DeleteRequest(const std::string& name, serialization::Data& key, int threadId)
            :name(name)
            , key(key)
            , threadId(threadId) {

            };

            int DeleteRequest::getFactoryId() const {
                return PortableHook::F_ID;
            }

            int DeleteRequest::getClassId() const {
                return PortableHook::DELETE_R;
            }


            void DeleteRequest::write(serialization::PortableWriter& writer) const {
                writer.writeUTF("n", name);
                writer.writeInt("t", threadId);
                serialization::ObjectDataOutput& out = writer.getRawDataOutput();
                key.writeData(out);
            };
        }
    }
}

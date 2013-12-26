//
// Created by sancar koyunlu on 9/4/13.
// Copyright (c) 2013 hazelcast. All rights reserved.


#include "hazelcast/client/map/GetEntryViewRequest.h"
#include "hazelcast/client/map/PortableHook.h"
#include "hazelcast/client/serialization/Data.h"
#include "hazelcast/client/serialization/PortableWriter.h"

namespace hazelcast {
    namespace client {
        namespace map {
            GetEntryViewRequest::GetEntryViewRequest(const std::string& name,const serialization::Data& key)
            :name(name)
            , key(key) {

            };

            int GetEntryViewRequest::getFactoryId() const {
                return PortableHook::F_ID;
            };

            int GetEntryViewRequest::getClassId() const {
                return PortableHook::GET_ENTRY_VIEW;
            };


            void GetEntryViewRequest::write(serialization::PortableWriter& writer) const {
                writer.writeUTF("n", name);
                serialization::ObjectDataOutput& out = writer.getRawDataOutput();
                key.writeData(out);
            };

        }
    }
}


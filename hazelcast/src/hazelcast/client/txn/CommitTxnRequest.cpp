//
// Created by sancar koyunlu on 19/11/13.
// Copyright (c) 2013 hazelcast. All rights reserved.


#include "hazelcast/client/txn/CommitTxnRequest.h"
#include "hazelcast/client/txn/TxnPortableHook.h"

namespace hazelcast {
    namespace client {
        namespace txn {

            int CommitTxnRequest::getFactoryId() const {
                return TxnPortableHook::F_ID;
            }

            int CommitTxnRequest::getClassId() const {
                return TxnPortableHook::COMMIT;
            }

            void CommitTxnRequest::write(serialization::PortableWriter &writer) const {
            };
        }
    }
}

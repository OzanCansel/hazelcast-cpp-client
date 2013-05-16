//#include "IAtomicLong.h"
//#include "ClientService.h"
//
//namespace hazelcast {
//    namespace client {
//
//        IAtomicLong::IAtomicLong(std::string instanceName, impl::ClientService& clientService) : instanceName(instanceName)
//        , clientService(clientService) {
//
//        };
//
//        IAtomicLong::IAtomicLong(const IAtomicLong& rhs) : instanceName(rhs.instanceName)
//        , clientService(rhs.clientService) {
//        };
//
//        IAtomicLong::~IAtomicLong() {
//
//        };
//
//        std::string IAtomicLong::getName() const {
//            return instanceName;
//        };
//
//    }
//}
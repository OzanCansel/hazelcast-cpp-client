#ifndef HAZELCAST_IQUEUE
#define HAZELCAST_IQUEUE

#include "hazelcast/client/queue/OfferRequest.h"
#include "hazelcast/client/queue/PollRequest.h"
#include "hazelcast/client/queue/RemainingCapacityRequest.h"
#include "hazelcast/client/queue/RemoveRequest.h"
#include "hazelcast/client/queue/ContainsRequest.h"
#include "hazelcast/client/queue/DrainRequest.h"
#include "hazelcast/client/queue/PeekRequest.h"
#include "hazelcast/client/queue/SizeRequest.h"
#include "hazelcast/client/queue/CompareAndRemoveRequest.h"
#include "hazelcast/client/queue/AddAllRequest.h"
#include "hazelcast/client/queue/ClearRequest.h"
#include "hazelcast/client/queue/IteratorRequest.h"
#include "hazelcast/client/queue/AddListenerRequest.h"
#include "hazelcast/client/queue/RemoveListenerRequest.h"
#include "hazelcast/client/impl/PortableCollection.h"
#include "hazelcast/client/impl/ItemEventHandler.h"
#include "hazelcast/client/ItemEvent.h"
#include "hazelcast/client/impl/ServerException.h"
#include "hazelcast/client/spi/ServerListenerService.h"
#include "hazelcast/client/DistributedObject.h"
#include <stdexcept>

namespace hazelcast {
    namespace client {

        /**
         * Concurrent, blocking, distributed, observable, client queue.
         *
         * @tparam E item type
         */
        template<typename E>
        class HAZELCAST_API IQueue : public DistributedObject {
            friend class HazelcastClient;

        public:
            /**
             * Adds an item listener for this collection. Listener will get notified
             * for all collection add/remove events.
             *
             * @param listener     item listener
             * @param includeValue <tt>true</tt> updated item should be passed
             *                     to the item listener, <tt>false</tt> otherwise.
             * @return returns registration id.
             */
            template < typename L>
            std::string addItemListener(L &listener, bool includeValue) {
                queue::AddListenerRequest *request = new queue::AddListenerRequest(getName(), includeValue);
                spi::ClusterService &cs = getContext().getClusterService();
                serialization::pimpl::SerializationService &ss = getContext().getSerializationService();
                impl::ItemEventHandler<E, L> *entryEventHandler = new impl::ItemEventHandler<E, L>(getName(), cs, ss, listener, includeValue);
                return listen(request, entryEventHandler);
            };

            /**
             * Removes the specified item listener.
             * Returns silently if the specified listener is not added before.
             *
             * @param registrationId Id of listener registration.
             *
             * @return true if registration is removed, false otherwise
             */
            bool removeItemListener(const std::string &registrationId) {
                queue::RemoveListenerRequest *request = new queue::RemoveListenerRequest(getName(), registrationId);
                return stopListening(request, registrationId);
            };

            /**
             * Inserts the specified element into this queue.
             *
             * @param element to add
             * @return <tt>true</tt> if the element was added to this queue, else
             *         <tt>false</tt>
             * @throws IClassCastException if the type of the specified element is incompatible with the server side.
             */
            bool offer(const E &element) {
                return offer(element, 0);
            };

            /**
             * Puts the element into queue.
             * If queue is  full waits for space to became available.
             */
            void put(const E &e) {
                offer(e, -1);
            };

            /**
             * Inserts the specified element into this queue.
             * If queue is  full waits for space to became available for specified time.
             *
             * @param element to add
             * @param timeoutInMillis how long to wait before giving up, in units of
             * @return <tt>true</tt> if successful, or <tt>false</tt> if
             *         the specified waiting time elapses before space is available
             */
            bool offer(const E &element, long timeoutInMillis) {
                serialization::pimpl::Data data = toData(element);
                queue::OfferRequest *request = new queue::OfferRequest(getName(), data, timeoutInMillis);
                return *(invoke<bool>(request, partitionId));
            };

            /**
             *
             * @return the head of the queue. If queue is empty waits for an item to be added.
             */
            boost::shared_ptr<E> take() {
                return poll(-1);
            };

            /**
             *
             * @param timeoutInMillis time to wait if item is not available.
             * @return the head of the queue. If queue is empty waits for specified time.
             */
            boost::shared_ptr<E> poll(long timeoutInMillis) {
                queue::PollRequest *request = new queue::PollRequest(getName(), timeoutInMillis);
                return invoke<E>(request, partitionId);
            };

            /**
             *
             * @return remaining capacity
             */
            int remainingCapacity() {
                queue::RemainingCapacityRequest *request = new queue::RemainingCapacityRequest(getName());
                boost::shared_ptr<int> cap = invoke<int>(request, partitionId);
                return *cap;
            };

            /**
             *
             * @param element to be removed.
             * @return true if element removed successfully.
             */
            bool remove(const E &element) {
                serialization::pimpl::Data data = toData(element);
                queue::RemoveRequest *request = new queue::RemoveRequest(getName(), data);
                return *(invoke<bool>(request, partitionId));
            };

            /**
             *
             * @param element to be checked.
             * @return true if queue contains the element.
             */
            bool contains(const E &element) {
                std::vector<serialization::pimpl::Data> list(1);
                list[0] = toData(element);
                queue::ContainsRequest *request = new queue::ContainsRequest(getName(), list);
                return *(invoke<bool>(request, partitionId));
            };

            /**
             * Note that elements will be pushed_back to vector.
             *
             * @param elements the vector that elements will be drained to.
             * @return number of elements drained.
             */
            int drainTo(std::vector<E> &elements) {
                return drainTo(elements, -1);
            };

            /**
             * Note that elements will be pushed_back to vector.
             *
             * @param maxElements upper limit to be filled to vector.
             * @param elements vector that elements will be drained to.
             * @return number of elements drained.
             */
            int drainTo(std::vector<E> &elements, int maxElements) {
                queue::DrainRequest *request = new queue::DrainRequest(getName(), maxElements);
                boost::shared_ptr<impl::PortableCollection> result = invoke<impl::PortableCollection>(request, partitionId);
                const std::vector<serialization::pimpl::Data> &coll = result->getCollection();
                for (std::vector<serialization::pimpl::Data>::const_iterator it = coll.begin(); it != coll.end(); ++it) {
                    boost::shared_ptr<E> e = getContext().getSerializationService().template toObject<E>(*it);
                    elements.push_back(*e);
                }
                return coll.size();
            };

            /**
             * Returns immediately without waiting.
             *
             * @return removes head of the queue and returns it to user . If not available returns empty constructed shared_ptr.
             */
            boost::shared_ptr<E> poll() {
                return poll(0);
            };

            /**
             * Returns immediately without waiting.
             *
             * @return head of queue without removing it. If not available returns empty constructed shared_ptr.
             */
            boost::shared_ptr<E> peek() {
                queue::PeekRequest *request = new queue::PeekRequest(getName());
                return invoke<E>(request, partitionId);
            };

            /**
             *
             * @return size of this distributed queue
             */
            int size() {
                queue::SizeRequest *request = new queue::SizeRequest(getName());
                boost::shared_ptr<int> size = invoke<int>(request, partitionId);
                return *size;
            }

            /**
             *
             * @return true if queue is empty
             */
            bool isEmpty() {
                return size() == 0;
            };

            /**
             *
             * @returns all elements as std::vector
             */
            std::vector<E> toArray() {
                queue::IteratorRequest *request = new queue::IteratorRequest(getName());
                boost::shared_ptr<impl::PortableCollection> result = invoke<impl::PortableCollection>(request, partitionId);
                std::vector<serialization::pimpl::Data> const &coll = result->getCollection();
                return getObjectList(coll);
            };

            /**
             *
             * @param elements std::vector<E>
             * @return true if this queue contains all elements given in vector.
             * @throws IClassCastException if the type of the specified element is incompatible with the server side.
             */
            bool containsAll(const std::vector<E> &elements) {
                std::vector<serialization::pimpl::Data> list = getDataList(elements);
                queue::ContainsRequest *request = new queue::ContainsRequest(getName(), list);
                boost::shared_ptr<bool> contains = invoke<bool>(request, partitionId);
                return *contains;
            }

            /**
             *
             * @param elements std::vector<E>
             * @return true if all elements given in vector can be added to queue.
             * @throws IClassCastException if the type of the specified element is incompatible with the server side.
             */
            bool addAll(const std::vector<E> &elements) {
                std::vector<serialization::pimpl::Data> dataList = getDataList(elements);
                queue::AddAllRequest *request = new queue::AddAllRequest(getName(), dataList);
                boost::shared_ptr<bool> success = invoke<bool>(request, partitionId);
                return *success;
            }

            /**
             *
             * @param elements std::vector<E>
             * @return true if all elements are removed successfully.
             * @throws IClassCastException if the type of the specified element is incompatible with the server side.
             */
            bool removeAll(const std::vector<E> &elements) {
                std::vector<serialization::pimpl::Data> dataList = getDataList(elements);
                queue::CompareAndRemoveRequest *request = new queue::CompareAndRemoveRequest(getName(), dataList, false);
                boost::shared_ptr<bool> success = invoke<bool>(request, partitionId);
                return *success;
            }

            /**
             *
             * Removes the elements from this queue that are not available in given "elements" vector
             * @param elements std::vector<E>
             * @return true if operation is successful.
             * @throws IClassCastException if the type of the specified element is incompatible with the server side.
             */
            bool retainAll(const std::vector<E> &elements) {
                std::vector<serialization::pimpl::Data> dataList = getDataList(elements);
                queue::CompareAndRemoveRequest *request = new queue::CompareAndRemoveRequest(getName(), dataList, true);
                boost::shared_ptr<bool> success = invoke<bool>(request, partitionId);
                return *success;
            }

            /**
             * Removes all elements from queue.
             */
            void clear() {
                queue::ClearRequest *request = new queue::ClearRequest(getName());
                invoke<serialization::pimpl::Void>(request, partitionId);
            };

        private:

            int partitionId;

            IQueue(const std::string &instanceName, spi::ClientContext *context)
            :DistributedObject("hz:impl:queueService", instanceName, context) {
                serialization::pimpl::Data data = toData<std::string>(getName());
                partitionId = getPartitionId(data);
            };

            template<typename T>
            serialization::pimpl::Data toData(const T &object) {
                return getContext().getSerializationService().template toData<T>(&object);
            };


            std::vector<serialization::pimpl::Data> getDataList(const std::vector<E> &objects) {
                int size = objects.size();
                std::vector<serialization::pimpl::Data> dataList(size);
                for (int i = 0; i < size; i++) {
                    dataList[i] = toData(objects[i]);
                }
                return dataList;
            };

            std::vector<E> getObjectList(const std::vector<serialization::pimpl::Data> &dataList) {
                size_t size = dataList.size();
                std::vector<E> objects(size);
                for (size_t i = 0; i < size; i++) {
                    boost::shared_ptr<E> object = getContext().getSerializationService(). template toObject<E>(dataList[i]);
                    objects[i] = *object;
                }
                return objects;
            };

            void onDestroy() {
            };


        };
    }
}

#endif /* HAZELCAST_IQUEUE */


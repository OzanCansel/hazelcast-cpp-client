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

#include <boost/optional.hpp>
#include <boost/any.hpp>

#include "hazelcast/client/serialization/pimpl/compact/schema_writer.h"
#include "hazelcast/client/serialization/generic_record.h"
#include "hazelcast/client/big_decimal.h"
#include "hazelcast/client/local_time.h"
#include "hazelcast/client/local_date.h"
#include "hazelcast/client/local_date_time.h"
#include "hazelcast/client/offset_date_time.h"
#include "hazelcast/util/byte.h"

namespace hazelcast {
namespace client {
namespace serialization {

class HAZELCAST_API generic_record_builder
{
public:
    explicit generic_record_builder(std::string);
    generic_record build();
    generic_record_builder& set_boolean(std::string field, bool value);
    // generic_record_builder& set_int8(std::string field, byte value);
    // generic_record_builder& set_char(std::string field, char value);
    // generic_record_builder& set_int16(std::string field, int16_t value);
    // generic_record_builder& set_int32(std::string field, int32_t value);
    // generic_record_builder& set_int64(std::string field, int64_t value);
    // generic_record_builder& set_float32(std::string field, float value);
    // generic_record_builder& set_float64(std::string field, double value);
    // generic_record_builder& set_nullable_boolean(std::string field, boost::optional<bool> value);
    // generic_record_builder& set_nullable_int8(std::string field, boost::optional<byte> value);
    // generic_record_builder& set_nullable_int16(std::string field, boost::optional<int16_t> value);
    // generic_record_builder& set_nullable_int32(std::string field, boost::optional<int32_t> value);
    // generic_record_builder& set_nullable_int64(std::string field, boost::optional<int64_t> value);
    // generic_record_builder& set_nullable_float32(std::string field, boost::optional<float> value);
    // generic_record_builder& set_nullable_float64(std::string field, boost::optional<double> value);
    generic_record_builder& set_string(std::string field, boost::optional<std::string> value);
    generic_record_builder& set_string(std::string field, const char* cstr);
    // generic_record_builder& set_generic_record(std::string field, boost::optional<generic_record> value);
    // generic_record_builder& set_decimal(std::string field, boost::optional<big_decimal> value);
    // generic_record_builder& set_time(std::string field, boost::optional<local_time> value);
    // generic_record_builder& set_date(std::string field, boost::optional<local_date> value);
    // generic_record_builder& set_timestamp(std::string field, boost::optional<local_date_time> value);
    // generic_record_builder& set_timestamp_with_timezone(std::string field, boost::optional<offset_date_time> value);
    generic_record_builder& set_array_of_boolean(std::string field, boost::optional<std::vector<bool>> value);
    generic_record_builder& set_array_of_boolean(std::string field, std::initializer_list<bool>);
    // generic_record_builder& set_array_of_int8(std::string field, boost::optional<std::vector<int8_t>> value);
    // generic_record_builder& set_array_of_char(std::string field, boost::optional<std::vector<char>> value);
    // generic_record_builder& set_array_of_int16(std::string field, boost::optional<std::vector<int16_t>> value);
    // generic_record_builder& set_array_of_int32(std::string field, boost::optional<std::vector<int32_t>> value);
    // generic_record_builder& set_array_of_int64(std::string field, boost::optional<std::vector<int64_t>> value);
    // generic_record_builder& set_array_of_float32(std::string field, boost::optional<std::vector<float>> value);
    // generic_record_builder& set_array_of_float64(std::string field, boost::optional<std::vector<double>> value);
    // generic_record_builder& set_array_of_nullable_boolean(std::string field, boost::optional<std::vector<boost::optional<bool>>> value);
    // generic_record_builder& set_array_of_nullable_boolean(std::string field, std::vector<bool> value);
    // generic_record_builder& set_array_of_nullable_int8(std::string field, boost::optional<std::vector<boost::optional<byte>>> value);
    // generic_record_builder& set_array_of_nullable_int8(std::string field, std::vector<byte> value);
    // generic_record_builder& set_array_of_nullable_int16(std::string field, boost::optional<std::vector<boost::optional<int16_t>>> value);
    // generic_record_builder& set_array_of_nullable_int16(std::string field, std::vector<int16_t> value);
    // generic_record_builder& set_array_of_nullable_int32(std::string field, boost::optional<std::vector<boost::optional<int32_t>>> value);
    // generic_record_builder& set_array_of_nullable_int32(std::string field, std::vector<int32_t> value);
    // generic_record_builder& set_array_of_nullable_int64(std::string field, boost::optional<std::vector<boost::optional<int64_t>>> value);
    // generic_record_builder& set_array_of_nullable_int64(std::string field, std::vector<int64_t> value);
    // generic_record_builder& set_array_of_nullable_float32(std::string field, boost::optional<std::vector<boost::optional<float>>> value);
    // generic_record_builder& set_array_of_nullable_float32(std::string field, std::vector<float> value);
    // generic_record_builder& set_array_of_nullable_float64(std::string field, boost::optional<std::vector<boost::optional<double>>> value);
    // generic_record_builder& set_array_of_nullable_float64(std::string field, std::vector<double> value);
    generic_record_builder& set_array_of_string(std::string field, boost::optional<std::vector<boost::optional<std::string>>> value);
    generic_record_builder& set_array_of_string(std::string field, std::vector<std::string> value);
    // generic_record_builder& set_array_of_decimal(std::string field, boost::optional<std::vector<boost::optional<big_decimal>>> value);
    // generic_record_builder& set_array_of_decimal(std::string field, std::vector<big_decimal> value);
    // generic_record_builder& set_array_of_time(std::string field, boost::optional<std::vector<boost::optional<local_time>>> value);
    // generic_record_builder& set_array_of_time(std::string field, std::vector<local_time> value);
    // generic_record_builder& set_array_of_date(std::string field, boost::optional<std::vector<boost::optional<local_date>>> value);
    // generic_record_builder& set_array_of_date(std::string field, std::vector<local_date> value);
    // generic_record_builder& set_array_of_timestamp(std::string field, boost::optional<std::vector<boost::optional<local_date_time>>> value);
    // generic_record_builder& set_array_of_timestamp(std::string field, std::vector<local_date_time> value);
    // generic_record_builder& set_array_of_timestamp_with_timezone(std::string field, boost::optional<std::vector<boost::optional<offset_date_time>>> value);
    // generic_record_builder& set_array_of_timestamp_with_timezone(std::string field, std::vector<offset_date_time> value);
    generic_record_builder& set_array_of_generic_record(std::string field, boost::optional<std::vector<boost::optional<generic_record>>> value);
    generic_record_builder& set_array_of_generic_record(std::string field, std::vector<generic_record> value);

private:

    friend class generic_record;
    generic_record_builder(std::unordered_map<std::string, boost::any>, pimpl::schema);

    std::unordered_map<std::string, boost::any> objects_;
    pimpl::schema_writer writer_;
};

}
}
}
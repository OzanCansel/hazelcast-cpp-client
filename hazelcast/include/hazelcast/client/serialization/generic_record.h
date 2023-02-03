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

#include <boost/any.hpp>

#include "hazelcast/client/serialization/field_kind.h"
#include "hazelcast/client/big_decimal.h"
#include "hazelcast/client/local_time.h"
#include "hazelcast/client/local_date.h"
#include "hazelcast/client/local_date_time.h"
#include "hazelcast/client/offset_date_time.h"
#include "hazelcast/util/export.h"
#include "hazelcast/util/byte.h"

namespace hazelcast {
namespace client {
namespace serialization {
namespace pimpl {
class compact_stream_serializer;
}

namespace generic_record {
struct generic_record_builder;

class HAZELCAST_API generic_record
{
public:
    generic_record_builder new_builder() const;
    generic_record_builder new_builder_with_clone() const;
    std::vector<std::string> get_field_names() const;
    field_kind get_field_kind(const std::string& field_name) const;

    bool get_boolean(const std::string& field_name) const;
    bool& get_boolean(const std::string& field_name);
    // int8_t get_int8(const std::string& field_name) const;
    // int8_t& get_int8(const std::string& field_name);
    // char get_char(const std::string& field_name) const;
    // char& get_char(const std::string& field_name);
    // int16_t get_int16(const std::string& field_name) const;
    // int16_t& get_int16(const std::string& field_name);
    // int32_t get_int32(const std::string& field_name) const;
    // int32_t& get_int32(const std::string& field_name);
    // int64_t get_int64(const std::string& field_name) const;
    // int64_t& get_int64(const std::string& field_name);
    // float get_float32(const std::string& field_name) const;
    // float& get_float32(const std::string& field_name);
    // double get_float64(const std::string& field_name) const;
    // double& get_float64(const std::string& field_name);
    // boost::optional<bool> get_nullable_boolean(const std::string& field_name) const;
    // boost::optional<bool>& get_nullable_boolean(const std::string& field_name);
    // boost::optional<int8_t> get_nullable_int8(const std::string& field_name) const;
    // boost::optional<int8_t>& get_nullable_int8(const std::string& field_name);
    // boost::optional<int16_t> get_nullable_int16(const std::string& field_name) const;
    // boost::optional<int16_t>& get_nullable_int16(const std::string& field_name);
    // boost::optional<int32_t> get_nullable_int32(const std::string& field_name) const;
    // boost::optional<int32_t>& get_nullable_int32(const std::string& field_name);
    // boost::optional<int64_t> get_nullable_int64(const std::string& field_name) const;
    // boost::optional<int64_t>& get_nullable_int64(const std::string& field_name);
    // boost::optional<float> get_nullable_float32(const std::string& field_name) const;
    // boost::optional<float>& get_nullable_float32(const std::string& field_name);
    // boost::optional<double> get_nullable_float64(const std::string& field_name) const;
    // boost::optional<double>& get_nullable_float64(const std::string& field_name);
    const boost::optional<std::string>& get_string(const std::string& field_name) const;
    boost::optional<std::string>& get_string(const std::string& field_name);
    const boost::optional<generic_record>& get_generic_record(const std::string& field_name) const;
    boost::optional<generic_record>& get_generic_record(const std::string& field_name);
    // const boost::optional<big_decimal>& get_decimal(const std::string& field_name) const;
    // boost::optional<big_decimal>& get_decimal(const std::string& field_name);
    // const boost::optional<local_time>& get_time(const std::string& field_name) const;
    // boost::optional<local_time>& get_time(const std::string& field_name);
    // const boost::optional<local_date>& get_date(const std::string& field_name) const;
    // boost::optional<local_date>& get_date(const std::string& field_name);
    // const boost::optional<local_date_time>& get_timestamp(const std::string& field_name) const;
    // boost::optional<local_date_time>& get_timestamp(const std::string& field_name);
    // const boost::optional<offset_date_time>& get_timestamp_with_timezone(const std::string& field_name) const;
    // boost::optional<offset_date_time>& get_timestamp_with_timezone(const std::string& field_name);
    const boost::optional<std::vector<bool>>& get_array_of_boolean(const std::string& field_name) const;
    boost::optional<std::vector<bool>>& get_array_of_boolean(const std::string& field_name);
    // const boost::optional<std::vector<int8_t>>& get_array_of_int8(const std::string& field_name) const;
    // boost::optional<std::vector<int8_t>>& get_array_of_int8(const std::string& field_name);
    // const boost::optional<std::vector<int16_t>>& get_array_of_int16(const std::string& field_name) const;
    // boost::optional<std::vector<int16_t>>& get_array_of_int16(const std::string& field_name);
    // const boost::optional<std::vector<int32_t>>& get_array_of_int32(const std::string& field_name) const;
    // boost::optional<std::vector<int32_t>>& get_array_of_int32(const std::string& field_name);
    // const boost::optional<std::vector<int64_t>>& get_array_of_int64(const std::string& field_name) const;
    // boost::optional<std::vector<int64_t>>& get_array_of_int64(const std::string& field_name);
    // const boost::optional<std::vector<float>>& get_array_of_float32(const std::string& field_name) const;
    // boost::optional<std::vector<float>>& get_array_of_float32(const std::string& field_name);
    // const boost::optional<std::vector<double>>& get_array_of_float64(const std::string& field_name) const;
    // boost::optional<std::vector<double>>& get_array_of_float64(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<bool>>>& get_array_of_nullable_boolean(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<bool>>>& get_array_of_nullable_boolean(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<int8_t>>>& get_array_of_nullable_int8(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<int8_t>>>& get_array_of_nullable_int8(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<int16_t>>>& get_array_of_nullable_int16(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<int16_t>>>& get_array_of_nullable_int16(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<int32_t>>>& get_array_of_nullable_int32(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<int32_t>>>& get_array_of_nullable_int32(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<int64_t>>>& get_array_of_nullable_int64(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<int64_t>>>& get_array_of_nullable_int64(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<float>>>& get_array_of_nullable_float32(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<float>>>& get_array_of_nullable_float32(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<double>>>& get_array_of_nullable_float64(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<double>>>& get_array_of_nullable_float64(const std::string& field_name);
    const boost::optional<std::vector<boost::optional<std::string>>>& get_array_of_string(const std::string& field_name) const;
    boost::optional<std::vector<boost::optional<std::string>>>& get_array_of_string(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<big_decimal>>>& get_array_of_decimal(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<big_decimal>>>& get_array_of_decimal(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<local_time>>>& get_array_of_time(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<local_time>>>& get_array_of_time(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<local_date>>>& get_array_of_date(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<local_date>>>& get_array_of_date(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<local_date_time>>>& get_array_of_timestamp(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<local_date_time>>>& get_array_of_timestamp(const std::string& field_name);
    // const boost::optional<std::vector<boost::optional<offset_date_time>>>& get_array_of_timestamp_with_timezone(const std::string& field_name) const;
    // boost::optional<std::vector<boost::optional<offset_date_time>>>& get_array_of_timestamp_with_timezone(const std::string& field_name);
    const boost::optional<std::vector<boost::optional<generic_record>>>& get_array_of_generic_record(const std::string& field_name) const;
    boost::optional<std::vector<boost::optional<generic_record>>>& get_array_of_generic_record(const std::string& field_name);

private:
    friend class generic_record_builder;
    friend class pimpl::compact_stream_serializer;

    friend std::ostream HAZELCAST_API & operator<<(std::ostream& os, const generic_record&);

    generic_record(pimpl::schema, std::unordered_map<std::string, boost::any>);
    const pimpl::schema& get_schema() const;

    template<typename T>
    T get_field(const std::string& field_name) const
    {
        return boost::any_cast<T>(objects_.at(field_name));
    }

    template<typename T>
    T get_field(const std::string& field_name)
    {
        return boost::any_cast<T>(objects_.at(field_name));
    }

    std::unordered_map<std::string, boost::any> objects_;
    pimpl::schema schema_;
};

}
}
}
}
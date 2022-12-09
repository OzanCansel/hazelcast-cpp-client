/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

#include <utility>

#include <thread>
#include <chrono>

#include "hazelcast/client/serialization/serialization.h"
#include "hazelcast/client/serialization/field_kind.h"
#include "hazelcast/client/serialization/pimpl/compact/schema.h"
#include "hazelcast/client/protocol/codec/codecs.h"
#include "hazelcast/client/spi/impl/ClientInvocation.h"
#include "hazelcast/client/spi/ClientContext.h"
#include "hazelcast/client/cluster.h"
#include "hazelcast/util/Bits.h"
#include "hazelcast/client/client_properties.h"

namespace hazelcast {
namespace client {
namespace serialization {
namespace pimpl {

field_descriptor::field_descriptor(field_kind k, int32_t i, int32_t o, int8_t b)
  : kind{ k }
  , index{ i }
  , offset{ o }
  , bit_offset{ b }
{
}

bool
operator==(const field_descriptor& x, const field_descriptor& y)
{
    return x.kind == y.kind;
}

std::ostream&
operator<<(std::ostream& os, const field_descriptor& fd)
{
    return os << fd.kind;
}

} // namespace pimpl
} // namespace serialization
} // namespace client
} // namespace hazelcast

namespace hazelcast {
namespace client {
namespace serialization {

std::ostream&
operator<<(std::ostream& os, field_kind kind)
{
    switch (kind) {
        case field_kind::BOOLEAN:
            os << "BOOLEAN";
            break;
        case field_kind::ARRAY_OF_BOOLEAN:
            os << "ARRAY_OF_BOOLEAN";
            break;
        case field_kind::INT8:
            os << "INT8";
            break;
        case field_kind::ARRAY_OF_INT8:
            os << "ARRAY_OF_INT8";
            break;
        case field_kind::INT16:
            os << "INT16";
            break;
        case field_kind::ARRAY_OF_INT16:
            os << "ARRAY_OF_INT16";
            break;
        case field_kind::INT32:
            os << "INT32";
            break;
        case field_kind::ARRAY_OF_INT32:
            os << "ARRAY_OF_INT32";
            break;
        case field_kind::INT64:
            os << "INT64";
            break;
        case field_kind::ARRAY_OF_INT64:
            os << "ARRAY_OF_INT16";
            break;
        case field_kind::FLOAT32:
            os << "FLOAT32";
            break;
        case field_kind::ARRAY_OF_FLOAT32:
            os << "ARRAY_OF_FLOAT32";
            break;
        case field_kind::FLOAT64:
            os << "FLOAT64";
            break;
        case field_kind::ARRAY_OF_FLOAT64:
            os << "ARRAY_OF_FLOAT64";
            break;
        case field_kind::STRING:
            os << "STRING";
            break;
        case field_kind::ARRAY_OF_STRING:
            os << "ARRAY_OF_STRING";
            break;
        case field_kind::DECIMAL:
            os << "DECIMAL";
            break;
        case field_kind::ARRAY_OF_DECIMAL:
            os << "ARRAY_OF_DECIMAL";
            break;
        case field_kind::TIME:
            os << "TIME";
            break;
        case field_kind::ARRAY_OF_TIME:
            os << "ARRAY_OF_TIME";
            break;
        case field_kind::DATE:
            os << "DATE";
            break;
        case field_kind::ARRAY_OF_DATE:
            os << "ARRAY_OF_DATE";
            break;
        case field_kind::TIMESTAMP:
            os << "TIMESTAMP";
            break;
        case field_kind::ARRAY_OF_TIMESTAMP:
            os << "ARRAY_OF_TIMESTAMP";
            break;
        case field_kind::TIMESTAMP_WITH_TIMEZONE:
            os << "TIMESTAMP_WITH_TIMEZONE";
            break;
        case field_kind::ARRAY_OF_TIMESTAMP_WITH_TIMEZONE:
            os << "ARRAY_OF_TIMESTAMP_WITH_TIMEZONE";
            break;
        case field_kind::COMPACT:
            os << "COMPACT";
            break;
        case field_kind::ARRAY_OF_COMPACT:
            os << "ARRAY_OF_COMPACT";
            break;
        case field_kind::NULLABLE_BOOLEAN:
            os << "NULLABLE_BOOLEAN";
            break;
        case field_kind::ARRAY_OF_NULLABLE_BOOLEAN:
            os << "ARRAY_OF_NULLABLE_BOOLEAN";
            break;
        case field_kind::NULLABLE_INT8:
            os << "NULLABLE_INT8";
            break;
        case field_kind::ARRAY_OF_NULLABLE_INT8:
            os << "ARRAY_OF_NULLABLE_INT8";
            break;
        case field_kind::NULLABLE_INT16:
            os << "NULLABLE_INT16";
            break;
        case field_kind::ARRAY_OF_NULLABLE_INT16:
            os << "ARRAY_OF_NULLABLE_INT16";
            break;
        case field_kind::NULLABLE_INT32:
            os << "NULLABLE_INT32";
            break;
        case field_kind::ARRAY_OF_NULLABLE_INT32:
            os << "ARRAY_OF_NULLABLE_INT32";
            break;
        case field_kind::NULLABLE_INT64:
            os << "NULLABLE_INT64";
            break;
        case field_kind::ARRAY_OF_NULLABLE_INT64:
            os << "ARRAY_OF_NULLABLE_INT64";
            break;
        case field_kind::NULLABLE_FLOAT32:
            os << "NULLABLE_FLOAT32";
            break;
        case field_kind::ARRAY_OF_NULLABLE_FLOAT32:
            os << "ARRAY_OF_NULLABLE_FLOAT32";
            break;
        case field_kind::NULLABLE_FLOAT64:
            os << "NULLABLE_FLOAT64";
            break;
        case field_kind::ARRAY_OF_NULLABLE_FLOAT64:
            os << "ARRAY_OF_NULLABLE_FLOAT64";
            break;
    }

    return os;
}

} // namespace serialization
} // namespace client
} // namespace hazelcast

namespace hazelcast {
namespace client {
namespace serialization {

compact_writer::compact_writer(
  pimpl::default_compact_writer* default_compact_writer)
  : default_compact_writer(default_compact_writer)
  , schema_writer(nullptr)
{}

compact_writer::compact_writer(pimpl::schema_writer* schema_writer)
  : default_compact_writer(nullptr)
  , schema_writer(schema_writer)
{}

void
compact_writer::write_boolean(const std::string& field_name, bool value)
{
    if (default_compact_writer) {
        default_compact_writer->write_boolean(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::BOOLEAN);
    }
}
void
compact_writer::write_int8(const std::string& field_name, int8_t value)
{
    if (default_compact_writer) {
        default_compact_writer->write_int8(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::INT8);
    }
}

void
compact_writer::write_int16(const std::string& field_name, int16_t value)
{
    if (default_compact_writer) {
        default_compact_writer->write_int16(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::INT16);
    }
}

void
compact_writer::write_int32(const std::string& field_name, int32_t value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_int32(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::INT32);
    }
}

void
compact_writer::write_int64(const std::string& field_name, int64_t value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_int64(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::INT64);
    }
}

void
compact_writer::write_float32(const std::string& field_name, float value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_float32(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::FLOAT32);
    }
}

void
compact_writer::write_float64(const std::string& field_name, double value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_float64(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::FLOAT64);
    }
}

void
compact_writer::write_string(const std::string& field_name,
                             const boost::optional<std::string>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_string(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::STRING);
    }
}

void
compact_writer::write_decimal(const std::string& field_name,
                              const boost::optional<big_decimal>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_decimal(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::DECIMAL);
    }
}

void
compact_writer::write_time(
  const std::string& field_name,
  const boost::optional<hazelcast::client::local_time>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_time(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::TIME);
    }
}

void
compact_writer::write_date(
  const std::string& field_name,
  const boost::optional<hazelcast::client::local_date>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_date(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::DATE);
    }
}

void
compact_writer::write_timestamp(
  const std::string& field_name,
  const boost::optional<hazelcast::client::local_date_time>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_timestamp(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::TIMESTAMP);
    }
}

void
compact_writer::write_timestamp_with_timezone(
  const std::string& field_name,
  const boost::optional<hazelcast::client::offset_date_time>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_timestamp_with_timezone(field_name,
                                                              value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::TIMESTAMP_WITH_TIMEZONE);
    }
}

void
compact_writer::write_array_of_boolean(
  const std::string& field_name,
  const boost::optional<std::vector<bool>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_boolean(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_BOOLEAN);
    }
}

void
compact_writer::write_array_of_int8(
  const std::string& field_name,
  const boost::optional<std::vector<int8_t>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_int8(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_INT8);
    }
}

void
compact_writer::write_array_of_int16(
  const std::string& field_name,
  const boost::optional<std::vector<int16_t>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_int16(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_INT16);
    }
}

void
compact_writer::write_array_of_int32(
  const std::string& field_name,
  const boost::optional<std::vector<int32_t>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_int32(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_INT32);
    }
}

void
compact_writer::write_array_of_int64(
  const std::string& field_name,
  const boost::optional<std::vector<int64_t>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_int64(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_INT64);
    }
}

void
compact_writer::write_array_of_float32(
  const std::string& field_name,
  const boost::optional<std::vector<float>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_float32(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_FLOAT32);
    }
}

void
compact_writer::write_array_of_float64(
  const std::string& field_name,
  const boost::optional<std::vector<double>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_float64(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_FLOAT64);
    }
}

void
compact_writer::write_array_of_string(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<std::string>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_string(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_STRING);
    }
}

void
compact_writer::write_array_of_decimal(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<big_decimal>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_decimal(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_DECIMAL);
    }
}

void
compact_writer::write_array_of_time(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<local_time>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_time(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_TIME);
    }
}

void
compact_writer::write_array_of_date(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<local_date>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_date(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_DATE);
    }
}

void
compact_writer::write_array_of_timestamp(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<local_date_time>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_timestamp(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::ARRAY_OF_TIMESTAMP);
    }
}

void
compact_writer::write_array_of_timestamp_with_timezone(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<offset_date_time>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_timestamp_with_timezone(
          field_name, value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_TIMESTAMP_WITH_TIMEZONE);
    }
}

void
compact_writer::write_nullable_boolean(const std::string& field_name,
                                       const boost::optional<bool>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_nullable_boolean(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::NULLABLE_BOOLEAN);
    }
}

void
compact_writer::write_nullable_int8(const std::string& field_name,
                                    const boost::optional<int8_t>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_nullable_int8(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::NULLABLE_INT8);
    }
}

void
compact_writer::write_nullable_int16(const std::string& field_name,
                                     const boost::optional<int16_t>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_nullable_int16(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::NULLABLE_INT16);
    }
}

void
compact_writer::write_nullable_int32(const std::string& field_name,
                                     const boost::optional<int32_t>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_nullable_int32(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::NULLABLE_INT32);
    }
}

void
compact_writer::write_nullable_int64(const std::string& field_name,
                                     const boost::optional<int64_t>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_nullable_int64(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::NULLABLE_INT64);
    }
}

void
compact_writer::write_nullable_float32(const std::string& field_name,
                                       const boost::optional<float>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_nullable_float32(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::NULLABLE_FLOAT32);
    }
}

void
compact_writer::write_nullable_float64(const std::string& field_name,
                                       const boost::optional<double>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_nullable_float64(field_name, value);
    } else {
        schema_writer->add_field(field_name, field_kind::NULLABLE_FLOAT64);
    }
}

void
compact_writer::write_array_of_nullable_boolean(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<bool>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_nullable_boolean(field_name,
                                                                value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_NULLABLE_BOOLEAN);
    }
}

void
compact_writer::write_array_of_nullable_int8(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int8_t>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_nullable_int8(field_name, value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_NULLABLE_INT8);
    }
}

void
compact_writer::write_array_of_nullable_int16(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int16_t>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_nullable_int16(field_name,
                                                              value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_NULLABLE_INT16);
    }
}

void
compact_writer::write_array_of_nullable_int32(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int32_t>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_nullable_int32(field_name,
                                                              value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_NULLABLE_INT32);
    }
}

void
compact_writer::write_array_of_nullable_int64(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int64_t>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_nullable_int64(field_name,
                                                              value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_NULLABLE_INT64);
    }
}

void
compact_writer::write_array_of_nullable_float32(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<float>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_nullable_float32(field_name,
                                                                value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_NULLABLE_FLOAT32);
    }
}

void
compact_writer::write_array_of_nullable_float64(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<double>>>& value)
{
    if (default_compact_writer != nullptr) {
        default_compact_writer->write_array_of_nullable_float64(field_name,
                                                                value);
    } else {
        schema_writer->add_field(field_name,
                                 field_kind::ARRAY_OF_NULLABLE_FLOAT64);
    }
}

namespace pimpl {

compact_reader
create_compact_reader(
  pimpl::compact_stream_serializer& compact_stream_serializer,
  object_data_input& object_data_input,
  const pimpl::schema& schema)
{
    return compact_reader{ compact_stream_serializer,
                           object_data_input,
                           schema };
}

} // namespace pimpl

const compact_reader::offset_func compact_reader::BYTE_OFFSET_READER =
  pimpl::offset_reader::get_offset<int8_t>;
const compact_reader::offset_func compact_reader::SHORT_OFFSET_READER =
  pimpl::offset_reader::get_offset<int16_t>;
const compact_reader::offset_func compact_reader::INT_OFFSET_READER =
  pimpl::offset_reader::get_offset<int32_t>;

compact_reader::compact_reader(
  pimpl::compact_stream_serializer& compact_stream_serializer,
  serialization::object_data_input& object_data_input,
  const pimpl::schema& schema)
  : compact_stream_serializer(compact_stream_serializer)
  , object_data_input(object_data_input)
  , schema(schema)
{
    size_t final_position;
    size_t number_of_var_size_fields = schema.number_of_var_size_fields();
    if (number_of_var_size_fields != 0) {
        uint32_t data_length = object_data_input.read<int32_t>();
        data_start_position = object_data_input.position();
        variable_offsets_position = data_start_position + data_length;
        if (data_length < pimpl::offset_reader::BYTE_OFFSET_READER_RANGE) {
            get_offset = BYTE_OFFSET_READER;
            final_position =
              variable_offsets_position + number_of_var_size_fields;
        } else if (data_length <
                   pimpl::offset_reader::SHORT_OFFSET_READER_RANGE) {
            get_offset = SHORT_OFFSET_READER;
            final_position =
              variable_offsets_position +
              (number_of_var_size_fields * util::Bits::SHORT_SIZE_IN_BYTES);
        } else {
            get_offset = INT_OFFSET_READER;
            final_position =
              variable_offsets_position +
              (number_of_var_size_fields * util::Bits::INT_SIZE_IN_BYTES);
        }
    } else {
        get_offset = INT_OFFSET_READER;
        variable_offsets_position = 0;
        data_start_position = object_data_input.position();
        final_position =
          data_start_position + schema.fixed_size_fields_length();
    }
    // set the position to final so that the next one to read something from
    // `in` can start from correct position
    object_data_input.position(static_cast<int>(final_position));
}

bool
compact_reader::is_field_exists(const std::string& field_name,
                                field_kind kind) const
{
    const auto& fields = schema.fields();
    const auto& field_descriptor = fields.find(field_name);
    if (field_descriptor == fields.end()) {
        return false;
    }
    return field_descriptor->second.kind == kind;
}

const pimpl::field_descriptor&
compact_reader::get_field_descriptor(const std::string& field_name) const
{
    const auto& fields = schema.fields();
    const auto& field_descriptor = fields.find(field_name);
    if (field_descriptor == fields.end()) {
        BOOST_THROW_EXCEPTION(unknown_field(field_name));
    }
    return field_descriptor->second;
}

const pimpl::field_descriptor&
compact_reader::get_field_descriptor(const std::string& field_name,
                                     field_kind kind) const
{
    const auto& field_descriptor = get_field_descriptor(field_name);
    if (field_descriptor.kind != kind) {
        BOOST_THROW_EXCEPTION(
          unexpected_field_kind(field_descriptor.kind, field_name));
    }
    return field_descriptor;
}

std::function<int32_t(serialization::object_data_input&, uint32_t, uint32_t)>
compact_reader::get_offset_reader(int32_t data_length)
{
    if (data_length < pimpl::offset_reader::BYTE_OFFSET_READER_RANGE) {
        return BYTE_OFFSET_READER;
    } else if (data_length < pimpl::offset_reader::SHORT_OFFSET_READER_RANGE) {
        return SHORT_OFFSET_READER;
    } else {
        return INT_OFFSET_READER;
    }
}

exception::hazelcast_serialization
compact_reader::unexpected_null_value_in_array(const std::string& field_name,
                                               const std::string& method_suffix)
{
    return {
        "compact_reader",
        (boost::format(
           "Error while reading %1%. Array with null items can not be read via "
           "read_array_of_%2% methods. Use read_array_of_nullable_%2% "
           "instead") %
         field_name % method_suffix)
          .str()
    };
}
exception::hazelcast_serialization
compact_reader::unknown_field(const std::string& field_name) const
{
    return { "compact_reader",
             (boost::format("Unknown field name %1% on %2% ") % field_name %
              schema)
               .str() };
}

exception::hazelcast_serialization
compact_reader::unexpected_field_kind(field_kind kind,
                                      const std::string& field_name) const
{
    return { "compact_reader",
             (boost::format("Unexpected fieldKind %1% for %2% on %3%") % kind %
              field_name % schema)
               .str() };
}

exception::hazelcast_serialization
compact_reader::unexpected_null_value(const std::string& field_name,
                                      const std::string& method_suffix)
{
    return { "compact_reader",
             (boost::format(
                "Error while reading %1%. null value can not be read via "
                "read_%2% methods. Use read_nullable_%2%  instead.") %
              field_name % method_suffix)
               .str() };
}

size_t
compact_reader::read_fixed_size_position(
  const pimpl::field_descriptor& field_descriptor) const
{
    int32_t primitive_offset = field_descriptor.offset;
    return primitive_offset + data_start_position;
}

int32_t
compact_reader::read_var_size_position(
  const pimpl::field_descriptor& field_descriptor) const
{
    int32_t index = field_descriptor.index;
    int32_t offset =
      get_offset(object_data_input, variable_offsets_position, index);
    return offset == pimpl::offset_reader::NULL_OFFSET
             ? pimpl::offset_reader::NULL_OFFSET
             : offset + data_start_position;
}

bool
compact_reader::read_boolean(const std::string& fieldName)
{
    return read_primitive<bool>(
      fieldName, field_kind::BOOLEAN, field_kind::NULLABLE_BOOLEAN, "boolean");
}

int8_t
compact_reader::read_int8(const std::string& fieldName)
{
    return read_primitive<int8_t>(
      fieldName, field_kind::INT8, field_kind::NULLABLE_INT8, "int8");
}

int16_t
compact_reader::read_int16(const std::string& field_name)
{
    return read_primitive<int16_t>(
      field_name, field_kind::INT16, field_kind::NULLABLE_INT16, "int16");
}

int32_t
compact_reader::read_int32(const std::string& field_name)
{
    return read_primitive<int32_t>(
      field_name, field_kind::INT32, field_kind::NULLABLE_INT32, "int32");
}

int64_t
compact_reader::read_int64(const std::string& field_name)
{
    return read_primitive<int64_t>(
      field_name, field_kind::INT64, field_kind::NULLABLE_INT64, "int64");
}

float
compact_reader::read_float32(const std::string& field_name)
{
    return read_primitive<float>(
      field_name, field_kind::FLOAT32, field_kind::NULLABLE_FLOAT32, "float32");
}

double
compact_reader::read_float64(const std::string& field_name)
{
    return read_primitive<double>(
      field_name, field_kind::FLOAT64, field_kind::NULLABLE_FLOAT64, "float64");
}

boost::optional<std::string>
compact_reader::read_string(const std::string& field_name)
{
    return read_variable_size<std::string>(field_name, field_kind::STRING);
}

boost::optional<big_decimal>
compact_reader::read_decimal(const std::string& field_name)
{
    return read_variable_size<big_decimal>(field_name, field_kind::DECIMAL);
}

boost::optional<hazelcast::client::local_time>
compact_reader::read_time(const std::string& field_name)
{
    return read_variable_size<hazelcast::client::local_time>(field_name,
                                                             field_kind::TIME);
}

boost::optional<hazelcast::client::local_date>
compact_reader::read_date(const std::string& field_name)
{
    return read_variable_size<hazelcast::client::local_date>(field_name,
                                                             field_kind::DATE);
}

boost::optional<hazelcast::client::local_date_time>
compact_reader::read_timestamp(const std::string& field_name)
{
    return read_variable_size<hazelcast::client::local_date_time>(
      field_name, field_kind::TIMESTAMP);
}

boost::optional<hazelcast::client::offset_date_time>
compact_reader::read_timestamp_with_timezone(const std::string& field_name)
{
    return read_variable_size<hazelcast::client::offset_date_time>(
      field_name, field_kind::TIMESTAMP_WITH_TIMEZONE);
}

boost::optional<std::vector<bool>>
compact_reader::read_array_of_boolean(const std::string& field_name)
{
    return read_array_of_primitive<std::vector<bool>>(
      field_name,
      field_kind::ARRAY_OF_BOOLEAN,
      field_kind::ARRAY_OF_NULLABLE_BOOLEAN,
      "boolean");
}

boost::optional<std::vector<int8_t>>
compact_reader::read_array_of_int8(const std::string& field_name)
{
    return read_array_of_primitive<std::vector<int8_t>>(
      field_name,
      field_kind::ARRAY_OF_INT8,
      field_kind::ARRAY_OF_NULLABLE_INT8,
      "int8");
}

boost::optional<std::vector<int16_t>>
compact_reader::read_array_of_int16(const std::string& field_name)
{
    return read_array_of_primitive<std::vector<int16_t>>(
      field_name,
      field_kind::ARRAY_OF_INT16,
      field_kind::ARRAY_OF_NULLABLE_INT16,
      "int16");
}

boost::optional<std::vector<int32_t>>
compact_reader::read_array_of_int32(const std::string& field_name)
{
    return read_array_of_primitive<std::vector<int32_t>>(
      field_name,
      field_kind::ARRAY_OF_INT32,
      field_kind::ARRAY_OF_NULLABLE_INT32,
      "int32");
}
boost::optional<std::vector<int64_t>>
compact_reader::read_array_of_int64(const std::string& field_name)
{
    return read_array_of_primitive<std::vector<int64_t>>(
      field_name,
      field_kind::ARRAY_OF_INT64,
      field_kind::ARRAY_OF_NULLABLE_INT64,
      "int64");
}

boost::optional<std::vector<float>>
compact_reader::read_array_of_float32(const std::string& field_name)
{
    return read_array_of_primitive<std::vector<float>>(
      field_name,
      field_kind::ARRAY_OF_FLOAT32,
      field_kind::ARRAY_OF_NULLABLE_FLOAT32,
      "float32");
}

boost::optional<std::vector<double>>
compact_reader::read_array_of_float64(const std::string& field_name)
{
    return read_array_of_primitive<std::vector<double>>(
      field_name,
      field_kind::ARRAY_OF_FLOAT64,
      field_kind::ARRAY_OF_NULLABLE_FLOAT64,
      "float64");
}

boost::optional<std::vector<boost::optional<std::string>>>
compact_reader::read_array_of_string(const std::string& field_name)
{
    const auto& descriptor =
      get_field_descriptor(field_name, field_kind::ARRAY_OF_STRING);
    return read_array_of_variable_size<std::string>(descriptor);
}

boost::optional<std::vector<boost::optional<big_decimal>>>
compact_reader::read_array_of_decimal(const std::string& field_name)
{
    const auto& descriptor =
      get_field_descriptor(field_name, field_kind::ARRAY_OF_DECIMAL);
    return read_array_of_variable_size<big_decimal>(descriptor);
}

boost::optional<std::vector<boost::optional<local_time>>>
compact_reader::read_array_of_time(const std::string& field_name)
{
    const auto& descriptor =
      get_field_descriptor(field_name, field_kind::ARRAY_OF_TIME);
    return read_array_of_variable_size<local_time>(descriptor);
}

boost::optional<std::vector<boost::optional<local_date>>>
compact_reader::read_array_of_date(const std::string& field_name)
{
    const auto& descriptor =
      get_field_descriptor(field_name, field_kind::ARRAY_OF_DATE);
    return read_array_of_variable_size<local_date>(descriptor);
}

boost::optional<std::vector<boost::optional<local_date_time>>>
compact_reader::read_array_of_timestamp(const std::string& field_name)
{
    const auto& descriptor =
      get_field_descriptor(field_name, field_kind::ARRAY_OF_TIMESTAMP);
    return read_array_of_variable_size<local_date_time>(descriptor);
}

boost::optional<std::vector<boost::optional<offset_date_time>>>
compact_reader::read_array_of_timestamp_with_timezone(
  const std::string& field_name)
{
    const auto& descriptor = get_field_descriptor(
      field_name, field_kind::ARRAY_OF_TIMESTAMP_WITH_TIMEZONE);
    return read_array_of_variable_size<offset_date_time>(descriptor);
}

boost::optional<bool>
compact_reader::read_nullable_boolean(const std::string& field_name)
{
    return read_nullable_primitive<bool>(
      field_name, field_kind::BOOLEAN, field_kind::NULLABLE_BOOLEAN);
}

boost::optional<int8_t>
compact_reader::read_nullable_int8(const std::string& field_name)
{
    return read_nullable_primitive<int8_t>(
      field_name, field_kind::INT8, field_kind::NULLABLE_INT8);
}

boost::optional<int16_t>
compact_reader::read_nullable_int16(const std::string& field_name)
{
    return read_nullable_primitive<int16_t>(
      field_name, field_kind::INT16, field_kind::NULLABLE_INT16);
}

boost::optional<int32_t>
compact_reader::read_nullable_int32(const std::string& field_name)
{
    return read_nullable_primitive<int32_t>(
      field_name, field_kind::INT32, field_kind::NULLABLE_INT32);
}

boost::optional<int64_t>
compact_reader::read_nullable_int64(const std::string& field_name)
{
    return read_nullable_primitive<int64_t>(
      field_name, field_kind::INT64, field_kind::NULLABLE_INT64);
}

boost::optional<float>
compact_reader::read_nullable_float32(const std::string& field_name)
{
    return read_nullable_primitive<float>(
      field_name, field_kind::FLOAT32, field_kind::NULLABLE_FLOAT32);
}

boost::optional<double>
compact_reader::read_nullable_float64(const std::string& field_name)
{
    return read_nullable_primitive<double>(
      field_name, field_kind::FLOAT64, field_kind::NULLABLE_FLOAT64);
}

boost::optional<std::vector<boost::optional<bool>>>
compact_reader::read_array_of_nullable_boolean(const std::string& field_name)
{
    return read_array_of_nullable<bool>(field_name,
                                        field_kind::ARRAY_OF_BOOLEAN,
                                        field_kind::ARRAY_OF_NULLABLE_BOOLEAN);
}

boost::optional<std::vector<boost::optional<int8_t>>>
compact_reader::read_array_of_nullable_int8(const std::string& field_name)
{
    return read_array_of_nullable<int8_t>(field_name,
                                          field_kind::ARRAY_OF_INT8,
                                          field_kind::ARRAY_OF_NULLABLE_INT8);
}

boost::optional<std::vector<boost::optional<int16_t>>>
compact_reader::read_array_of_nullable_int16(const std::string& field_name)
{
    return read_array_of_nullable<int16_t>(field_name,
                                           field_kind::ARRAY_OF_INT16,
                                           field_kind::ARRAY_OF_NULLABLE_INT16);
}

boost::optional<std::vector<boost::optional<int32_t>>>
compact_reader::read_array_of_nullable_int32(const std::string& field_name)
{
    return read_array_of_nullable<int32_t>(field_name,
                                           field_kind::ARRAY_OF_INT32,
                                           field_kind::ARRAY_OF_NULLABLE_INT32);
}

boost::optional<std::vector<boost::optional<int64_t>>>
compact_reader::read_array_of_nullable_int64(const std::string& field_name)
{
    return read_array_of_nullable<int64_t>(field_name,
                                           field_kind::ARRAY_OF_INT64,
                                           field_kind::ARRAY_OF_NULLABLE_INT64);
}

boost::optional<std::vector<boost::optional<float>>>
compact_reader::read_array_of_nullable_float32(const std::string& field_name)
{
    return read_array_of_nullable<float>(field_name,
                                         field_kind::ARRAY_OF_FLOAT32,
                                         field_kind::ARRAY_OF_NULLABLE_FLOAT32);
}

boost::optional<std::vector<boost::optional<double>>>
compact_reader::read_array_of_nullable_float64(const std::string& field_name)
{
    return read_array_of_nullable<double>(
      field_name,
      field_kind::ARRAY_OF_FLOAT64,
      field_kind::ARRAY_OF_NULLABLE_FLOAT64);
}

namespace pimpl {

compact_writer
create_compact_writer(pimpl::default_compact_writer* default_compact_writer)
{
    return compact_writer{ default_compact_writer };
}

compact_writer
create_compact_writer(pimpl::schema_writer* schema_writer)
{
    return compact_writer{ schema_writer };
}

default_compact_writer::default_compact_writer(
  compact_stream_serializer& compact_stream_serializer,
  object_data_output& object_data_output,
  const schema& schema)
  : compact_stream_serializer_(compact_stream_serializer)
  , object_data_output_(object_data_output)
  , schema_(schema)
  , field_offsets(schema.number_of_var_size_fields())
{
    if (schema.number_of_var_size_fields() != 0) {
        data_start_position =
          object_data_output_.position() + util::Bits::INT_SIZE_IN_BYTES;
        // Skip for length and primitives.
        object_data_output_.write_zero_bytes(schema.fixed_size_fields_length() +
                                             util::Bits::INT_SIZE_IN_BYTES);
    } else {
        data_start_position = object_data_output_.position();
        // Skip for primitives. No need to write data length, when there is no
        // variable-size fields.
        object_data_output_.write_zero_bytes(schema.fixed_size_fields_length());
    }
}

void
default_compact_writer::write_boolean(const std::string& field_name, bool value)
{
    field_descriptor descriptor =
      check_field_definition(field_name, field_kind::BOOLEAN);
    int32_t offset_in_bytes = descriptor.offset;
    int8_t offset_in_bits = descriptor.bit_offset;
    size_t write_offset = offset_in_bytes + data_start_position;
    object_data_output_.write_boolean_bit_at(
      write_offset, offset_in_bits, value);
}

void
default_compact_writer::write_int8(const std::string& field_name, int8_t value)
{
    size_t position =
      get_fixed_size_field_position(field_name, field_kind::INT8);
    object_data_output_.write_at(position, value);
}

void
default_compact_writer::write_int16(const std::string& field_name,
                                    int16_t value)
{
    size_t position =
      get_fixed_size_field_position(field_name, field_kind::INT16);
    object_data_output_.write_at(position, value);
}

void
default_compact_writer::write_int32(const std::string& field_name,
                                    int32_t value)
{
    size_t position =
      get_fixed_size_field_position(field_name, field_kind::INT32);
    object_data_output_.write_at(position, value);
}

void
default_compact_writer::write_int64(const std::string& field_name,
                                    int64_t value)
{
    size_t position =
      get_fixed_size_field_position(field_name, field_kind::INT64);
    object_data_output_.write_at(position, value);
}

void
default_compact_writer::write_float32(const std::string& field_name,
                                      float value)
{
    size_t position =
      get_fixed_size_field_position(field_name, field_kind::FLOAT32);
    object_data_output_.write_at(position, value);
}

void
default_compact_writer::write_float64(const std::string& field_name,
                                      double value)
{
    size_t position =
      get_fixed_size_field_position(field_name, field_kind::FLOAT64);
    object_data_output_.write_at(position, value);
}

void
default_compact_writer::write_string(const std::string& field_name,
                                     const boost::optional<std::string>& value)
{
    write_variable_size_field(field_name, field_kind::STRING, value);
}

void
default_compact_writer::write_decimal(const std::string& field_name,
                                      const boost::optional<big_decimal>& value)
{
    write_variable_size_field(field_name, field_kind::DECIMAL, value);
}

void
default_compact_writer::write_time(
  const std::string& field_name,
  const boost::optional<hazelcast::client::local_time>& value)
{
    write_variable_size_field(field_name, field_kind::TIME, value);
}
void
default_compact_writer::write_date(
  const std::string& field_name,
  const boost::optional<hazelcast::client::local_date>& value)
{
    write_variable_size_field(field_name, field_kind::DATE, value);
}

void
default_compact_writer::write_timestamp(
  const std::string& field_name,
  const boost::optional<hazelcast::client::local_date_time>& value)
{
    write_variable_size_field(field_name, field_kind::TIMESTAMP, value);
}

void
default_compact_writer::write_timestamp_with_timezone(
  const std::string& field_name,
  const boost::optional<hazelcast::client::offset_date_time>& value)
{
    write_variable_size_field(
      field_name, field_kind::TIMESTAMP_WITH_TIMEZONE, value);
}

void
default_compact_writer::write_array_of_boolean(
  const std::string& field_name,
  const boost::optional<std::vector<bool>>& value)
{
    write_variable_size_field(field_name, field_kind::ARRAY_OF_BOOLEAN, value);
}

void
default_compact_writer::write_array_of_int8(
  const std::string& field_name,
  const boost::optional<std::vector<int8_t>>& value)
{
    write_variable_size_field(field_name, field_kind::ARRAY_OF_INT8, value);
}

void
default_compact_writer::write_array_of_int16(
  const std::string& field_name,
  const boost::optional<std::vector<int16_t>>& value)
{
    write_variable_size_field(field_name, field_kind::ARRAY_OF_INT16, value);
}

void
default_compact_writer::write_array_of_int32(
  const std::string& field_name,
  const boost::optional<std::vector<int32_t>>& value)
{
    write_variable_size_field(field_name, field_kind::ARRAY_OF_INT32, value);
}

void
default_compact_writer::write_array_of_int64(
  const std::string& field_name,
  const boost::optional<std::vector<int64_t>>& value)
{
    write_variable_size_field(field_name, field_kind::ARRAY_OF_INT64, value);
}

void
default_compact_writer::write_array_of_float32(
  const std::string& field_name,
  const boost::optional<std::vector<float>>& value)
{
    write_variable_size_field(field_name, field_kind::ARRAY_OF_FLOAT32, value);
}

void
default_compact_writer::write_array_of_float64(
  const std::string& field_name,
  const boost::optional<std::vector<double>>& value)
{
    write_variable_size_field(field_name, field_kind::ARRAY_OF_FLOAT64, value);
}

void
default_compact_writer::write_array_of_string(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<std::string>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_STRING, value);
}

void
default_compact_writer::write_array_of_decimal(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<big_decimal>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_DECIMAL, value);
}

void
default_compact_writer::write_array_of_time(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<local_time>>>& value)
{
    write_array_of_variable_size(field_name, field_kind::ARRAY_OF_TIME, value);
}

void
default_compact_writer::write_array_of_date(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<local_date>>>& value)
{
    write_array_of_variable_size(field_name, field_kind::ARRAY_OF_DATE, value);
}

void
default_compact_writer::write_array_of_timestamp(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<local_date_time>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_TIMESTAMP, value);
}

void
default_compact_writer::write_array_of_timestamp_with_timezone(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<offset_date_time>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_TIMESTAMP_WITH_TIMEZONE, value);
}

void
default_compact_writer::write_nullable_boolean(
  const std::string& field_name,
  const boost::optional<bool>& value)
{
    write_variable_size_field(field_name, field_kind::NULLABLE_BOOLEAN, value);
}
void
default_compact_writer::write_nullable_int8(
  const std::string& field_name,
  const boost::optional<int8_t>& value)
{
    write_variable_size_field(field_name, field_kind::NULLABLE_INT8, value);
}
void
default_compact_writer::write_nullable_int16(
  const std::string& field_name,
  const boost::optional<int16_t>& value)
{
    write_variable_size_field(field_name, field_kind::NULLABLE_INT16, value);
}
void
default_compact_writer::write_nullable_int32(
  const std::string& field_name,
  const boost::optional<int32_t>& value)
{
    write_variable_size_field(field_name, field_kind::NULLABLE_INT32, value);
}
void
default_compact_writer::write_nullable_int64(
  const std::string& field_name,
  const boost::optional<int64_t>& value)
{
    write_variable_size_field(field_name, field_kind::NULLABLE_INT64, value);
}
void
default_compact_writer::write_nullable_float32(
  const std::string& field_name,
  const boost::optional<float>& value)
{
    write_variable_size_field(field_name, field_kind::NULLABLE_FLOAT32, value);
}
void
default_compact_writer::write_nullable_float64(
  const std::string& field_name,
  const boost::optional<double>& value)
{
    write_variable_size_field(field_name, field_kind::NULLABLE_FLOAT64, value);
}

void
default_compact_writer::write_array_of_nullable_boolean(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<bool>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_NULLABLE_BOOLEAN, value);
}

void
default_compact_writer::write_array_of_nullable_int8(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int8_t>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_NULLABLE_INT8, value);
}

void
default_compact_writer::write_array_of_nullable_int16(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int16_t>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_NULLABLE_INT16, value);
}

void
default_compact_writer::write_array_of_nullable_int32(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int32_t>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_NULLABLE_INT32, value);
}

void
default_compact_writer::write_array_of_nullable_int64(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<int64_t>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_NULLABLE_INT64, value);
}

void
default_compact_writer::write_array_of_nullable_float32(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<float>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_NULLABLE_FLOAT32, value);
}

void
default_compact_writer::write_array_of_nullable_float64(
  const std::string& field_name,
  const boost::optional<std::vector<boost::optional<double>>>& value)
{
    write_array_of_variable_size(
      field_name, field_kind::ARRAY_OF_NULLABLE_FLOAT64, value);
}

void
default_compact_writer::end()
{
    if (schema_.number_of_var_size_fields() == 0) {
        // There are no variable size fields
        return;
    }
    size_t position = object_data_output_.position();
    size_t data_length = position - data_start_position;
    write_offsets(data_length, field_offsets);
    // write dataLength
    object_data_output_.write_at(data_start_position -
                                   util::Bits::INT_SIZE_IN_BYTES,
                                 (int32_t)data_length);
}

size_t
default_compact_writer::get_fixed_size_field_position(
  const std::string& field_name,
  enum field_kind field_kind) const
{
    const field_descriptor& field_descriptor =
      check_field_definition(field_name, field_kind);
    return field_descriptor.offset + data_start_position;
}

const field_descriptor&
default_compact_writer::check_field_definition(const std::string& field_name,
                                               field_kind kind) const
{
    auto iterator = schema_.fields().find(field_name);
    if (iterator == schema_.fields().end()) {
        BOOST_THROW_EXCEPTION(exception::hazelcast_serialization(
          "default_compact_writer",
          (boost::format("Invalid field name %1% for %2%") % field_name %
           schema_)
            .str()));
    }
    if (iterator->second.kind != kind) {
        BOOST_THROW_EXCEPTION(exception::hazelcast_serialization(
          "default_compact_writer",
          (boost::format("Invalid field type %1% for %2%") % field_name %
           schema_)
            .str()));
    }
    return iterator->second;
}

void
default_compact_writer::write_offsets(size_t data_length,
                                      const std::vector<int32_t>& offsets)
{
    if (data_length < offset_reader::BYTE_OFFSET_READER_RANGE) {
        for (int32_t offset : offsets) {
            object_data_output_.write<int8_t>(static_cast<int8_t>(offset));
        }
    } else if (data_length < offset_reader::SHORT_OFFSET_READER_RANGE) {
        for (int32_t offset : offsets) {
            object_data_output_.write<int16_t>(static_cast<int16_t>(offset));
        }
    } else {
        for (int32_t offset : offsets) {
            object_data_output_.write<int32_t>(offset);
        }
    }
}

void
default_compact_writer::set_position(const std::string& field_name,
                                     enum field_kind field_kind)
{
    const auto& field_descriptor =
      check_field_definition(field_name, field_kind);
    size_t pos = object_data_output_.position();
    size_t fieldPosition = pos - data_start_position;
    int index = field_descriptor.index;
    field_offsets[index] = static_cast<int32_t>(fieldPosition);
}

void
default_compact_writer::set_position_as_null(const std::string& field_name,
                                             enum field_kind field_kind)
{
    const auto& field_descriptor =
      check_field_definition(field_name, field_kind);
    int index = field_descriptor.index;
    field_offsets[index] = -1;
}

namespace rabin_finger_print {
/**
 * We use uint64_t for computation to match the behaviour of >>> operator
 * on java. We use >> instead.
 */
constexpr uint64_t INIT = 0xc15d213aa4d7a795L;

static std::array<uint64_t, 256>
init_fp_table()
{
    static std::array<uint64_t, 256> FP_TABLE;
    for (int i = 0; i < 256; ++i) {
        uint64_t fp = i;
        for (int j = 0; j < 8; ++j) {
            fp = (fp >> 1) ^ (INIT & -(fp & 1L));
        }
        FP_TABLE[i] = fp;
    }
    return FP_TABLE;
}
uint64_t
FP_TABLE_AT(int index)
{
    static auto FP_TABLE = init_fp_table();
    return FP_TABLE[index];
}

uint64_t
fingerprint64(uint64_t fp, byte b)
{
    return (fp >> 8) ^ FP_TABLE_AT((int)(fp ^ b) & 0xff);
}

uint64_t
fingerprint64(uint64_t fp, int v)
{
    fp = fingerprint64(fp, (byte)((v)&0xFF));
    fp = fingerprint64(fp, (byte)((v >> 8) & 0xFF));
    fp = fingerprint64(fp, (byte)((v >> 16) & 0xFF));
    fp = fingerprint64(fp, (byte)((v >> 24) & 0xFF));
    return fp;
}

uint64_t
fingerprint64(uint64_t fp, const std::string& value)
{
    fp = fingerprint64(fp, (int)value.size());
    for (const auto& item : value) {
        fp = fingerprint64(fp, (byte)item);
    }
    return fp;
}

/**
 * Calculates the fingerprint of the schema from its type name and fields.
 */
int64_t
fingerprint64(const std::string& type_name,
              std::map<std::string, field_descriptor>& fields)
{
    uint64_t fingerPrint = fingerprint64(INIT, type_name);
    fingerPrint = fingerprint64(fingerPrint, (int)fields.size());
    for (const auto& entry : fields) {
        const field_descriptor& descriptor = entry.second;
        fingerPrint = fingerprint64(fingerPrint, entry.first);
        fingerPrint = fingerprint64(fingerPrint, (int)descriptor.kind);
    }
    return static_cast<int64_t>(fingerPrint);
}

} // namespace rabin_finger_print

bool
kind_size_comparator(const field_descriptor* i, const field_descriptor* j)
{
    auto i_kind_size = field_operations::get(i->kind).kind_size_in_byte_func();
    auto j_kind_size = field_operations::get(j->kind).kind_size_in_byte_func();
    return i_kind_size > j_kind_size;
}

schema::schema(
  std::string type_name,
  std::unordered_map<std::string, field_descriptor>&& field_definition_map)
  : type_name_(std::move(type_name))
  , field_definition_map_(std::move(field_definition_map))
{
    std::vector<field_descriptor*> fixed_size_fields;
    std::vector<field_descriptor*> boolean_fields;
    std::vector<field_descriptor*> variable_size_fields;

    std::map<std::string, field_descriptor> sorted_fields(
      field_definition_map_.begin(), field_definition_map_.end());
    for (auto& item : sorted_fields) {
        field_descriptor& descriptor = item.second;
        field_kind kind = descriptor.kind;
        if (field_operations::get(kind).kind_size_in_byte_func() ==
            field_kind_based_operations::VARIABLE_SIZE) {
            variable_size_fields.push_back(&descriptor);
        } else if (kind == field_kind::BOOLEAN) {
            boolean_fields.push_back(&descriptor);
        } else {
            fixed_size_fields.push_back(&descriptor);
        }
    }

    std::sort(
      fixed_size_fields.begin(), fixed_size_fields.end(), kind_size_comparator);

    int offset = 0;
    for (auto descriptor : fixed_size_fields) {
        descriptor->offset = offset;
        offset +=
          field_operations::get(descriptor->kind).kind_size_in_byte_func();
    }

    int8_t bit_offset = 0;
    for (auto descriptor : boolean_fields) {
        descriptor->offset = offset;
        descriptor->bit_offset =
          static_cast<int8_t>(bit_offset % util::Bits::BITS_IN_BYTE);
        bit_offset++;
        if (bit_offset % util::Bits::BITS_IN_BYTE == 0) {
            offset += 1;
        }
    }
    if (bit_offset % util::Bits::BITS_IN_BYTE != 0) {
        offset += 1;
    }

    fixed_size_fields_length_ = offset;

    int index = 0;
    for (auto descriptor : variable_size_fields) {
        descriptor->index = index;
        index++;
    }

    for (auto& item : sorted_fields) {
        field_definition_map_[item.first] = item.second;
    }

    number_of_var_size_fields_ = index;
    schema_id_ = rabin_finger_print::fingerprint64(type_name_, sorted_fields);
}

int64_t
schema::schema_id() const
{
    return schema_id_;
}

size_t
schema::number_of_var_size_fields() const
{
    return number_of_var_size_fields_;
}

size_t
schema::fixed_size_fields_length() const
{
    return fixed_size_fields_length_;
}

const std::string&
schema::type_name() const
{
    return type_name_;
}

const std::unordered_map<std::string, field_descriptor>&
schema::fields() const
{
    return field_definition_map_;
}

bool
operator==(const schema& x, const schema& y)
{
    return x.number_of_var_size_fields() == y.number_of_var_size_fields() &&
           x.fixed_size_fields_length() == y.fixed_size_fields_length() &&
           x.schema_id() == y.schema_id() && x.type_name() == y.type_name() &&
           x.fields() == y.fields();
}

bool
operator!=(const schema& x, const schema& y)
{
    return !(x == y);
}

std::ostream&
operator<<(std::ostream& os, const schema& schema)
{
    os << "Schema { className = " << schema.type_name()
       << ", numberOfComplextFields = " << schema.number_of_var_size_fields()
       << ",primitivesLength = " << schema.fixed_size_fields_length()
       << ",fields {";
    for (const auto& item : schema.fields()) {
        os << item.first << " " << item.second << ",";
    }
    os << "}";
    return os;
}

} // namespace pimpl

namespace pimpl {

schema_writer::schema_writer(std::string type_name)
  : type_name(std::move(type_name))
{}

void
schema_writer::add_field(std::string field_name, enum field_kind kind)
{
    field_definition_map[std::move(field_name)] = field_descriptor{ kind };
}

schema
schema_writer::build() &&
{
    return schema{ type_name, std::move(field_definition_map) };
}

default_schema_service::default_schema_service(spi::ClientContext& context)
  : retry_pause_millis_{ context.get_client_properties().get_integer(
      context.get_client_properties().get_invocation_retry_pause_millis()) }
  , max_put_retry_count_{ context.get_client_properties().get_integer(
      client_property{ MAX_PUT_RETRY_COUNT, MAX_PUT_RETRY_COUNT_DEFAULT }) }
  , context_{ context }
{
}

schema
default_schema_service::get(int64_t schemaId)
{
    auto ptr = replicateds_.get(schemaId);

    if (!ptr) {
        throw exception::illegal_state{ "default_schema_service::get",
                                        "Schema doesn't exist for this type" };
    }

    return *ptr;
}

boost::future<void>
default_schema_service::replicate_schema(schema s)
{
    return replicate_schema_attempt(std::move(s));
}

boost::future<void>
default_schema_service::replicate_schema_attempt(schema s, int attempts)
{
    using hazelcast::client::protocol::ClientMessage;
    using namespace protocol::codec;

    auto max_retry_count{ max_put_retry_count_ };
    auto message = send_schema_request_encode(s);

    auto invocation =
      spi::impl::ClientInvocation::create(context_, message, SERVICE_NAME);

    return invocation->invoke().then(
      boost::launch::sync,
      [this, max_retry_count, attempts, s](
        boost::future<ClientMessage> future) {
          auto msg = future.get();

          auto replicated_member_uuids = send_schema_response_decode(msg);
          auto members = context_.get_cluster().get_members();

          for (const member& searchee : members) {
              auto contains =
                any_of(begin(replicated_member_uuids),
                       end(replicated_member_uuids),
                       [&searchee](const boost::uuids::uuid member_id) {
                           return searchee.get_uuid() == member_id;
                       });

              if (!contains) {
                  if (attempts >= max_retry_count) {
                      throw exception::illegal_state{
                          "default_schema_service::replicate_schema_attempt",
                          (boost::format("The schema %1% cannot be "
                                         "replicated in the cluster, after "
                                         "%2% retries. It might be the case "
                                         "that the client is experiencing a "
                                         "split-brain, and continue putting "
                                         "the data associated with that "
                                         "schema might result in data loss. "
                                         "It might be possible to replicate "
                                         "the schema after some time, when "
                                         "the cluster is healed.") %
                             s % max_retry_count)
                            .str()
                      };
                  } else {
                      std::this_thread::sleep_for(
                        std::chrono::milliseconds{ retry_pause_millis_ });

                      replicate_schema_attempt(std::move(s), attempts + 1)
                        .get();

                      return;
                  }
              }
          }

          auto s_p = std::make_shared<schema>(std::move(s));
          auto existing = replicateds_.put_if_absent(s.schema_id(), s_p);

          if (!existing) {
              return;
          }

          if (*s_p != *existing) {
              throw exception::illegal_state{
                  "default_schema_service::replicate_schema_attempt",
                  (boost::format("Schema with schemaId %1% "
                                 "already exists. Existing "
                                 "schema %2%, new schema %3%") %
                   s.schema_id() % *existing % s)
                    .str()
              };
          }
      });
}

bool
default_schema_service::is_schema_replicated(const schema& s)
{
    return bool(replicateds_.get(s.schema_id()));
}

compact_stream_serializer::compact_stream_serializer(
  default_schema_service& service)
  : schema_service{ service }
{
}

field_kind_based_operations::field_kind_based_operations()
  : kind_size_in_byte_func(DEFAULT_KIND_SIZE_IN_BYTES)
{}

field_kind_based_operations::field_kind_based_operations(
  std::function<int()> kind_size_in_byte_func)
  : kind_size_in_byte_func(std::move(kind_size_in_byte_func))
{}
field_kind_based_operations
field_operations::get(field_kind kind)
{
    using util::Bits;

    switch (kind) {
        case field_kind::BOOLEAN:
            return field_kind_based_operations{ []() { return 0; } };
        case field_kind::INT8:
            return field_kind_based_operations{ []() { return 1; } };
        case field_kind::INT16:
            return field_kind_based_operations{ []() {
                return Bits::SHORT_SIZE_IN_BYTES;
            } };
        case field_kind::INT32:
            return field_kind_based_operations{ []() {
                return Bits::INT_SIZE_IN_BYTES;
            } };
        case field_kind::INT64:
            return field_kind_based_operations{ []() {
                return Bits::LONG_SIZE_IN_BYTES;
            } };
        case field_kind::FLOAT32:
            return field_kind_based_operations{ []() {
                return Bits::FLOAT_SIZE_IN_BYTES;
            } };
        case field_kind::FLOAT64:
            return field_kind_based_operations{ []() {
                return Bits::DOUBLE_SIZE_IN_BYTES;
            } };
        default:
            return field_kind_based_operations{};
    }

    return field_kind_based_operations{};
}
} // namespace pimpl
} // namespace serialization
} // namespace client
} // namespace hazelcast

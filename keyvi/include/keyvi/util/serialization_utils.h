/* * keyvi - A key value store.
 *
 * Copyright 2015 Hendrik Muhs<hendrik.muhs@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * serialization_utils.h
 *
 *  Created on: May 12, 2014
 *      Author: hendrik
 */

#ifndef KEYVI_UTIL_SERIALIZATION_UTILS_H_
#define KEYVI_UTIL_SERIALIZATION_UTILS_H_

#include <string>

#include <boost/lexical_cast.hpp>
// boost json parser depends on boost::spirit, and spirit is not thread-safe by default. so need to enable thread-safety
#define BOOST_SPIRIT_THREADSAFE
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "dictionary/util/endian.h"

namespace keyvi {
namespace util {

class SerializationUtils {
 public:
  static void WriteJsonRecord(std::ostream& stream, const boost::property_tree::ptree& properties) {
    std::stringstream string_buffer;

    boost::property_tree::write_json(string_buffer, properties, false);
    std::string header = string_buffer.str();

    uint32_t size = htobe32(header.size());

    stream.write(reinterpret_cast<const char*>(&size), sizeof(uint32_t));
    stream << header;
  }

  static void WriteJsonRecord(std::ostream& stream, const rapidjson::Document& record) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    record.Accept(writer);

    uint32_t size = htobe32(buffer.GetSize());

    stream.write(reinterpret_cast<const char*>(&size), sizeof(uint32_t));
    stream.write(buffer.GetString(), buffer.GetSize());
  }

  static void ReadJsonRecord(std::istream& stream, rapidjson::Document& record) {
    uint32_t header_size;
    stream.read(reinterpret_cast<char*>(&header_size), sizeof(int));
    header_size = be32toh(header_size);
    char* buffer = new char[header_size];
    stream.read(buffer, header_size);
    record.Parse(buffer, header_size);
  }

  static void ReadValueStoreProperties(std::istream& stream, rapidjson::Document& properties) {
    ReadJsonRecord(stream, properties);
    const auto offset = stream.tellg();

    // check for file truncation
    const size_t vsSize = boost::lexical_cast<size_t>(properties["size"].GetString());
    if (vsSize > 0) {
      stream.seekg(vsSize - 1, stream.cur);
      if (stream.peek() == EOF) {
        throw std::invalid_argument("file is corrupt(truncated)");
      }
    }

    stream.seekg(offset);
    return;
  }

  /**
   * Utility method to return a json document from a JSON string.
   * @param record a string containing a JSON
   * @return the parsed document
   */
  static void ReadJsonRecord(const std::string& json_string, rapidjson::Document& record) {
    if (!json_string.empty()) {
      record.Parse(json_string);
    }
    return;
  }
};

} /* namespace util */
} /* namespace keyvi */

#endif  // KEYVI_UTIL_SERIALIZATION_UTILS_H_

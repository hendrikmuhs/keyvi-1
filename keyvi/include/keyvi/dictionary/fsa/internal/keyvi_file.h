/* * keyvi - A key value store.
 *
 * Copyright 2015, 2016 Hendrik Muhs<hendrik.muhs@gmail.com>,
 *                      Narek Gharibyan<narekgharibyan@gmail.com>
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
 * keyvi_file.h
 *
 *  Created on: November 21, 2016
 *      Author: Narek Gharibyan <narekgharibyan@gmail.com>
 */

#ifndef KEYVI_DICTIONARY_FSA_INTERNAL_KEYVI_FILE_H_
#define KEYVI_DICTIONARY_FSA_INTERNAL_KEYVI_FILE_H_

#include <cstdint>
#include <fstream>
#include <string>

#include <boost/lexical_cast.hpp>

#include "rapidjson/document.h"

#include "dictionary/fsa/internal/constants.h"
#include "dictionary/fsa/internal/ivalue_store.h"
#include "dictionary/fsa/internal/memory_map_flags.h"
#include "dictionary/fsa/internal/value_store_factory.h"

#include "util/serialization_utils.h"

#define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {
namespace fsa {
namespace internal {

class KeyviFile {
 public:
  explicit KeyviFile(const std::string& filename) : KeyviFile(filename, loading_strategy_types::lazy, true) {}

  explicit KeyviFile(const std::string& file_name,
                     loading_strategy_types loading_strategy,
                     const bool load_value_store)
      : file_name_(file_name), loading_strategy_(loading_strategy) {
    std::ifstream file_stream(file_name, std::ios::binary);

    if (!file_stream.good()) {
      throw std::invalid_argument("file not found");
    }

    // todo: make this optional
    file_mapping_ = boost::interprocess::file_mapping(file_name_.c_str(), boost::interprocess::read_only);

    char magic[KEYVI_FILE_MAGIC_LEN];
    file_stream.read(magic, KEYVI_FILE_MAGIC_LEN);

    // check magic
    if (std::strncmp(magic, KEYVI_FILE_MAGIC, KEYVI_FILE_MAGIC_LEN) == 0) {
      ReadJsonFormat(file_stream);

      if (load_value_store) {
        LoadValueStore(file_stream);
      }

      return;
    }
    throw std::invalid_argument("not a keyvi file");
  }

  uint64_t GetStartState() const { return start_state_; }

  uint64_t GetNumberOfKeys() const { return number_of_keys_; }

  value_store_t GetValueStoreType() const { return value_store_type_; }

  size_t GetSparseArraySize() const { return sparse_array_size_; }

  std::streampos GetPersistenceOffset() const { return persistence_offset_; }

  boost::interprocess::file_mapping& GetFileMapping() { return file_mapping_; }

  internal::IValueStoreReader* GetValueStore() const {
    /*if (value_store_reader_.get() == nullptr) {
      std::ifstream file_stream(file_name_, std::ios::binary);

      if (!file_stream.good()) {
        throw std::invalid_argument("file not found");
      }

      LoadValueStore(file_stream);
    }*/

    return value_store_reader_.get();
  }

 private:
  std::string file_name_;
  loading_strategy_types loading_strategy_;
  boost::interprocess::file_mapping file_mapping_;
  uint64_t start_state_;
  uint64_t number_of_keys_;
  value_store_t value_store_type_;
  size_t sparse_array_size_;
  std::streampos persistence_offset_;
  std::streampos value_store_offset_;
  std::unique_ptr<internal::IValueStoreReader> value_store_reader_;

  void LoadValueStore(std::ifstream& file_stream) {
    file_stream.seekg(value_store_offset_);

    boost::interprocess::file_mapping file_mapping{
        boost::interprocess::file_mapping(file_name_.c_str(), boost::interprocess::read_only)};

    value_store_reader_.reset(
        internal::ValueStoreFactory::MakeReader(value_store_type_, file_stream, &file_mapping, loading_strategy_));
  }

  void ReadJsonFormat(std::ifstream& file_stream) {
    rapidjson::Document automata_properties;

    keyvi::util::SerializationUtils::ReadJsonRecord(file_stream, automata_properties);

    if (boost::lexical_cast<int>(automata_properties["version"].GetString()) < KEYVI_FILE_VERSION_MIN) {
      throw std::invalid_argument("this version of keyvi file is unsupported");
    }

    start_state_ = boost::lexical_cast<uint64_t>(automata_properties["start_state"].GetString());

    TRACE("start state %d", start_state_);

    number_of_keys_ = boost::lexical_cast<uint64_t>(automata_properties["number_of_keys"].GetString());
    value_store_type_ = static_cast<internal::value_store_t>(
        boost::lexical_cast<int>(automata_properties["value_store_type"].GetString()));

    rapidjson::Document sparse_array_properties;
    keyvi::util::SerializationUtils::ReadJsonRecord(file_stream, sparse_array_properties);

    if (boost::lexical_cast<int>(sparse_array_properties["version"].GetString()) < KEYVI_FILE_PERSISTENCE_VERSION_MIN) {
      throw std::invalid_argument("this versions of keyvi file is unsupported");
    }

    persistence_offset_ = file_stream.tellg();

    const size_t bucket_size = sizeof(uint16_t);
    sparse_array_size_ = boost::lexical_cast<size_t>(sparse_array_properties["size"].GetString());

    // check for file truncation
    file_stream.seekg((size_t)file_stream.tellg() + sparse_array_size_ + bucket_size * sparse_array_size_ - 1);
    if (file_stream.peek() == EOF) {
      throw std::invalid_argument("file is corrupt(truncated)");
    }

    file_stream.get();
    value_store_offset_ = file_stream.tellg();
  }
};  // namespace internal

}  // namespace internal
}  // namespace fsa
}  // namespace dictionary
}  // namespace keyvi
#endif  // KEYVI_DICTIONARY_FSA_INTERNAL_KEYVI_FILE_H_

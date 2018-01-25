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
 * read_only_segment.h
 *
 *  Created on: Jan 20, 2018
 *      Author: hendrik
 */

#ifndef KEYVI_INDEX_INTERNAL_READ_ONLY_SEGMENT_H_
#define KEYVI_INDEX_INTERNAL_READ_ONLY_SEGMENT_H_

#include <cstdio>
#include <ctime>
#include <memory>
#include <mutex>  //NOLINT
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include <boost/filesystem.hpp>

#include <msgpack.hpp>

#include "dictionary/dictionary.h"

// #define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace index {
namespace internal {

class ReadOnlySegment {
 public:
  explicit ReadOnlySegment(const boost::filesystem::path& path, const bool load = true)
      : dictionary_path_(path),
        deleted_keys_path_(path),
        deleted_keys_during_merge_path_(path),
        dictionary_filename_(path.filename().string()),
        dictionary_(),
        has_deleted_keys_(false),
        deleted_keys_(),
        last_modification_time_deleted_keys_(0),
        last_modification_time_deleted_keys_during_merge_(0) {
    deleted_keys_path_ += ".dk";
    deleted_keys_during_merge_path_ += ".dkm";
    if (load) {
      Load();
    }
  }

  dictionary::dictionary_t& operator*() {
    if (!dictionary_) {
      Load();
    }

    return dictionary_;
  }

  dictionary::dictionary_t& GetDictionary() {
    if (!dictionary_) {
      Load();
    }

    return dictionary_;
  }

  void ReloadDeletedKeys() { LoadDeletedKeys(); }

  const boost::filesystem::path& GetDictionaryPath() const { return dictionary_path_; }

  const boost::filesystem::path& GetDeletedKeysPath() const { return deleted_keys_path_; }

  const boost::filesystem::path& GetDeletedKeysDuringMergePath() const { return deleted_keys_during_merge_path_; }

  const std::string& GetDictionaryFilename() const { return dictionary_filename_; }

  bool HasDeletedKeys() const { return has_deleted_keys_; }

  const std::shared_ptr<std::unordered_set<std::string>> DeletedKeys() {
    // check has_deleted_keys_ or make this private

    std::shared_ptr<std::unordered_set<std::string>> deleted_keys = deleted_keys_weak_.lock();
    if (!deleted_keys) {
      std::unique_lock<std::mutex> lock(mutex_);
      deleted_keys_weak_ = deleted_keys_;
      deleted_keys = deleted_keys_;
    }
    return deleted_keys;
  }

  bool IsDeleted(const std::string& key) {
    if (has_deleted_keys_) {
      return (DeletedKeys()->count(key) > 0);
    }

    return false;
  }

 protected:
  void Load() {
    // load dictionary
    dictionary_.reset(new dictionary::Dictionary(dictionary_path_.string()));

    // load deleted keys
    LoadDeletedKeys();
  }

 private:
  //! path of the underlying dictionary
  boost::filesystem::path dictionary_path_;

  //! list of deleted keys
  boost::filesystem::path deleted_keys_path_;

  //! deleted keys while segment gets merged with other segments
  boost::filesystem::path deleted_keys_during_merge_path_;

  //! just the filename part of the dictionary
  std::string dictionary_filename_;

  //! the dictionary itself
  dictionary::dictionary_t dictionary_;

  //! quick and cheap check whether this segment has deletes (assuming that deletes are rare)
  std::atomic_bool has_deleted_keys_;

  //! deleted keys
  std::shared_ptr<std::unordered_set<std::string>> deleted_keys_;

  //! a weak ptr to deleted keys for access in the main thread
  std::weak_ptr<std::unordered_set<std::string>> deleted_keys_weak_;

  //! a mutex to secure access to the deleted keys shared pointer
  std::mutex mutex_;

  //! last modification time for the deleted keys file
  std::time_t last_modification_time_deleted_keys_;

  //! last modification time for the deleted keys file during a merge operation
  std::time_t last_modification_time_deleted_keys_during_merge_;

  void LoadDeletedKeys() {
    boost::system::error_code ec;
    std::time_t last_write_dk = boost::filesystem::last_write_time(deleted_keys_path_, ec);
    // effectively ignore if file does not exist
    if (ec) {
      last_write_dk = last_modification_time_deleted_keys_;
    }

    std::time_t last_write_dkm = boost::filesystem::last_write_time(deleted_keys_during_merge_path_, ec);
    // effectively ignore if file does not exist
    if (ec) {
      last_write_dkm = last_modification_time_deleted_keys_during_merge_;
    }

    // if any list has changed, reload it
    if (last_write_dk > last_modification_time_deleted_keys_ ||
        last_write_dkm > last_modification_time_deleted_keys_during_merge_) {
      std::shared_ptr<std::unordered_set<std::string>> deleted_keys =
          std::make_shared<std::unordered_set<std::string>>();
      std::unordered_set<std::string> deleted_keys_dk = LoadAndUnserializeDeletedKeys(deleted_keys_path_.string());

      deleted_keys->swap(deleted_keys_dk);
      // deleted_keys->insert(deleted_keys_dk.begin(), deleted_keys_dk.end());
      std::unordered_set<std::string> deleted_keys_dkm =
          LoadAndUnserializeDeletedKeys(deleted_keys_during_merge_path_.string());

      deleted_keys->insert(deleted_keys_dkm.begin(), deleted_keys_dkm.end());

      // safe swap
      {
        std::unique_lock<std::mutex> lock(mutex_);
        deleted_keys_.swap(deleted_keys);
      }

      has_deleted_keys_ = true;
    }
  }

  inline static std::unordered_set<std::string> LoadAndUnserializeDeletedKeys(const std::string& filename) {
    std::unordered_set<std::string> deleted_keys;
    std::ifstream deleted_keys_stream(filename, std::ios::binary);
    if (deleted_keys_stream.good()) {
      std::stringstream buffer;
      buffer << deleted_keys_stream.rdbuf();

      msgpack::unpacked unpacked_object;
      msgpack::unpack(unpacked_object, buffer.str().data(), buffer.str().size());
      std::unordered_set<std::string> deleted_keys;
      unpacked_object.get().convert(deleted_keys);
    }
    return deleted_keys;
  }
};

typedef std::shared_ptr<ReadOnlySegment> read_only_segment_t;
typedef std::vector<read_only_segment_t> read_only_segment_vec_t;
typedef std::shared_ptr<read_only_segment_vec_t> read_only_segments_t;
typedef const std::shared_ptr<read_only_segment_vec_t> const_read_only_segments_t;

} /* namespace internal */
} /* namespace index */
} /* namespace keyvi */

#endif  // KEYVI_INDEX_INTERNAL_READ_ONLY_SEGMENT_H_
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

#ifndef KEYVI_INDEX_INTERNAL_SEGMENT_H_
#define KEYVI_INDEX_INTERNAL_SEGMENT_H_

#include <cstdio>
#include <memory>
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

class Segment final {
 public:
  explicit Segment(const boost::filesystem::path& path, const bool load = true)
      : path_(path),
        filename_(path.filename().string()),
        deleted_keys_(),
        deleted_keys_during_merge_(),
        dictionary_(),
        in_merge_(false),
        new_delete_(false) {
    if (load) {
      Load();
    }
  }

  explicit Segment(const boost::filesystem::path& path, const std::vector<std::shared_ptr<Segment>> parent_segments,
                   const bool load = true)
      : path_(path),
        filename_(path.filename().string()),
        deleted_keys_(),
        deleted_keys_during_merge_(),
        dictionary_(),
        in_merge_(false),
        new_delete_(false) {
    if (load) {
      Load();
    }

    // move deletions that happened during merge into the list of deleted keys
    for (const auto& p_segment : parent_segments) {
      deleted_keys_.insert(p_segment->deleted_keys_during_merge_.begin(), p_segment->deleted_keys_during_merge_.end());
    }

    // persist the current list of deleted keys
    if (deleted_keys_.size()) {
      new_delete_ = true;
      Persist();
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

  const boost::filesystem::path& GetPath() const { return path_; }

  const std::string& GetFilename() const { return filename_; }

  void ElectedForMerge() {
    Persist();
    in_merge_ = true;
  }

  void MergeFailed() {
    in_merge_ = false;
    if (deleted_keys_during_merge_.size() > 0) {
      deleted_keys_.insert(deleted_keys_during_merge_.begin(), deleted_keys_during_merge_.end());

      new_delete_ = true;
      Persist();
      deleted_keys_during_merge_.clear();
      // remove dkm file
      std::string deleted_keys_file_merge{GetPath().string() + ".dkm"};
      std::remove(deleted_keys_file_merge.c_str());
    }
  }

  bool MarkedForMerge() const { return in_merge_; }

  void RemoveFiles() {
    // delete files, not all files might exist, therefore ignore the output
    std::remove(GetPath().c_str());

    std::string deleted_keys_file_merge{GetPath().string() + ".dkm"};
    std::remove(deleted_keys_file_merge.c_str());

    std::string deleted_keys_file{GetPath().string() + ".dk"};
    std::remove(deleted_keys_file.c_str());
  }

  void DeleteKey(const std::string& key) {
    if (!GetDictionary()->Contains(key)) {
      return;
    }

    if (in_merge_) {
      TRACE("delete key (in merge) %s", key.c_str());
      deleted_keys_during_merge_.insert(key);
    } else {
      TRACE("delete key (no merge) %s", key.c_str());
      deleted_keys_.insert(key);
    }
    new_delete_ = true;
  }

  // persist deleted keys
  void Persist() {
    if (!new_delete_) {
      return;
    }
    TRACE("persist deleted keys");
    boost::filesystem::path deleted_keys_file = path_;

    // its ensured that before merge persist is called, so we have to persist only one or the other file
    if (in_merge_) {
      deleted_keys_file += ".dkm";
      std::ofstream out_stream(deleted_keys_file.string(), std::ios::binary);
      msgpack::pack(out_stream, deleted_keys_during_merge_);
    } else {
      deleted_keys_file += ".dk";
      std::ofstream out_stream(deleted_keys_file.string(), std::ios::binary);
      msgpack::pack(out_stream, deleted_keys_);
    }
  }

 private:
  boost::filesystem::path path_;
  std::string filename_;
  std::unordered_set<std::string> deleted_keys_;
  std::unordered_set<std::string> deleted_keys_during_merge_;
  dictionary::dictionary_t dictionary_;
  bool in_merge_;
  bool new_delete_;

  void Load() { dictionary_.reset(new dictionary::Dictionary(path_.string())); }
};

typedef std::shared_ptr<Segment> segment_t;
typedef std::vector<segment_t> segment_vec_t;
typedef std::shared_ptr<std::vector<segment_t>> segments_t;
typedef const std::shared_ptr<std::vector<segment_t>> const_segments_t;

} /* namespace internal */
} /* namespace index */
} /* namespace keyvi */

#endif  // KEYVI_INDEX_INTERNAL_SEGMENT_H_

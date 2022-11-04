/* * keyvi - A key value store.
 *
 * Copyright 2021 Hendrik Muhs<hendrik.muhs@gmail.com>
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

#ifndef KEYVI_DICTIONARY_K_D_DICTIONARY_H_
#define KEYVI_DICTIONARY_K_D_DICTIONARY_H_

#include <memory>
#include <string>
#include <vector>

#include "morton-nd/mortonND_LUT.h"

#include "keyvi/dictionary/dictionary.h"
#include "keyvi/dictionary/fsa/automata.h"
#include "keyvi/dictionary/fsa/state_traverser.h"
#include "keyvi/dictionary/fsa/traverser_types.h"
#include "keyvi/dictionary/match.h"
#include "keyvi/dictionary/match_iterator.h"
#include "keyvi/dictionary/matching/fuzzy_matching.h"
#include "keyvi/dictionary/matching/near_matching.h"
#include "keyvi/dictionary/util/endian.h"

// #define ENABLE_TRACING
#include "keyvi/dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {

class KDDictionary final {
 public:
  /**
   * Initialize a k-d-dictionary from a file.
   *
   * @param filename filename to load keyvi file from.
   * @param loading_strategy optional: Loading strategy to use.
   */
  explicit KDDictionary(const std::string& filename,
                        loading_strategy_types loading_strategy = loading_strategy_types::lazy)
      : dictionary_(filename, loading_strategy) {
    TRACE("KDDictionary from file %s", filename.c_str());
  }

  explicit KDDictionary(fsa::automata_t f) : dictionary_(f) {}

  fsa::automata_t GetFsa() const { return dictionary_.GetFsa(); }

  std::string GetStatistics() const { return dictionary_.GetStatistics(); }

  uint64_t GetSize() const { return dictionary_.GetSize(); }

  Match operator[](const std::vector<double>& input_vector) const {
    uint64_t mapped_x1 = static_cast<std::uint64_t>(((input_vector[0] - min_) / (max_ - min_)) * (1L << 32));
    uint64_t mapped_x2 = static_cast<std::uint64_t>(((input_vector[1] - min_) / (max_ - min_)) * (1L << 32));

    uint64_t encoded = htobe64(encoder_.Encode(mapped_x1, mapped_x2));
    std::string key(reinterpret_cast<const char*>(&encoded), 8);
    return dictionary_[key];
  }

  /**
   * Exact Match function.
   *
   * @param key the key to lookup.
   * @return a match iterator
   */
  MatchIterator::MatchIteratorPair Get(const std::vector<double>& input_vector) const {
    uint64_t mapped_x1 = static_cast<std::uint64_t>(((input_vector[0] - min_) / (max_ - min_)) * (1L << 32));
    uint64_t mapped_x2 = static_cast<std::uint64_t>(((input_vector[1] - min_) / (max_ - min_)) * (1L << 32));

    uint64_t encoded = htobe64(encoder_.Encode(mapped_x1, mapped_x2));
    std::string key(reinterpret_cast<const char*>(&encoded), 8);
    return dictionary_.Get(key);
  }

  /**
   */
  MatchIterator::MatchIteratorPair GetNearestNeighbors(const std::vector<double>& input_vector) const {
    uint64_t mapped_x1 = static_cast<std::uint64_t>(((input_vector[0] - min_) / (max_ - min_)) * (1L << 32));
    uint64_t mapped_x2 = static_cast<std::uint64_t>(((input_vector[1] - min_) / (max_ - min_)) * (1L << 32));

    uint64_t encoded = htobe64(encoder_.Encode(mapped_x1, mapped_x2));
    std::string key(reinterpret_cast<const char*>(&encoded), 8);
    return dictionary_.GetNear(key, 1, true);
  }

  std::string GetManifest() const { return dictionary_.GetManifest(); }

 private:
  Dictionary dictionary_;
  // todo: hardcoded for now, make it flexible based on dictionary properties
  mortonnd::MortonNDLutEncoder<2, 32, 8> encoder_;
  double min_ = 0.0;
  double max_ = 1.0;
};

typedef std::shared_ptr<KDDictionary> k_d_dictionary_t;

} /* namespace dictionary */
} /* namespace keyvi */

#endif  //  KEYVI_DICTIONARY_K_D_DICTIONARY_H_

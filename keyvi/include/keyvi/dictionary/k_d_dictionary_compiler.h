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

#ifndef KEYVI_DICTIONARY_K_D_DICTIONARY_COMPILER_H_
#define KEYVI_DICTIONARY_K_D_DICTIONARY_COMPILER_H_

#include <string>
#include <vector>

#include "morton-nd/mortonND_LUT.h"

#include "keyvi/dictionary/dictionary_compiler_common.h"
#include "keyvi/util/configuration.h"

// #define ENABLE_TRACING
#include "keyvi/dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {

/**
 * KD-Dictionary Compiler
 */
template <class DictionaryCompilerT, std::size_t Dimensions>
class KDDictionaryCompiler final {
  using ValueStoreT = typename DictionaryCompilerT::ValueStoreT;
  using callback_t = std::function<void(size_t, size_t, void*)>;

  static_assert(Dimensions > 0, "Dimensions must be > 0");
  // static_assert(Dimensions == 2, "only 2 dimensions supported in the 1st version");

 public:
  /**
   * Instantiate a k-d-dictionary compiler, k-d stands for k-dimensional. With this dictionary compiler you can
   * index k-dimensional points.
   *
   * @param params compiler parameters
   */
  explicit KDDictionaryCompiler(const keyvi::util::parameters_t& params = keyvi::util::parameters_t(), double min = 0.0,
                                double max = 1.0)
      : compiler_(params), encoder_(), min_(min), max_(max) {}

  KDDictionaryCompiler& operator=(KDDictionaryCompiler const&) = delete;
  KDDictionaryCompiler(const KDDictionaryCompiler& that) = delete;

  void Add(const std::vector<double>& input_vector, typename ValueStoreT::value_t value = ValueStoreT::no_value) {
    if (input_vector.size() != Dimensions) {
      throw compiler_exception("input vector size does not match dimensions");
    }

    uint64_t mapped_x1 = static_cast<std::uint64_t>(((input_vector[0] - min_) / (max_ - min_)) * (1L << 32));
    uint64_t mapped_x2 = static_cast<std::uint64_t>(((input_vector[1] - min_) / (max_ - min_)) * (1L << 32));

    // todo: use endian function
    uint64_t encoded = __builtin_bswap64(encoder_.Encode(mapped_x1, mapped_x2));
    std::string key(reinterpret_cast<const char*>(&encoded), 8);

    compiler_.Add(key, value);
  }

  /**
   * Do the final compilation
   */
  void Compile(callback_t progress_callback = nullptr, void* user_data = nullptr) {
    compiler_.Compile(progress_callback, user_data);
  }

  /**
   * Set a custom manifest to be embedded into the index file.
   *
   * @param manifest as string
   */
  void SetManifest(const std::string& manifest) { compiler_.SetManifest(manifest); }

  void Write(std::ostream& stream) { compiler_.Write(stream); }

  void WriteToFile(const std::string& filename) { compiler_.WriteToFile(filename); }

 private:
  DictionaryCompilerT compiler_;
  // todo: calculate field length based on dimensions
  mortonnd::MortonNDLutEncoder<Dimensions, 32, 8> encoder_;
  double min_;
  double max_;
};

} /* namespace dictionary */
} /* namespace keyvi */

#endif  // KEYVI_DICTIONARY_K_D_DICTIONARY_COMPILER_H_

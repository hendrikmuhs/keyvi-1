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
#include "keyvi/dictionary/util/endian.h"
#include "keyvi/util/configuration.h"
#include "keyvi/util/vint.h"

// #define ENABLE_TRACING
#include "keyvi/dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {

/**
 * KD-Dictionary Compiler
 */
template <keyvi::dictionary::fsa::internal::value_store_t ValueStoreType, std::size_t Dimensions>
class KDDictionaryCompilerBase {
 private:
  using ValueStoreT = typename fsa::internal::ValueStoreComponents<ValueStoreType>::value_store_writer_t;
  using callback_t = std::function<void(size_t, size_t, void*)>;

 public:
  /**
   * Instantiate a k-d-dictionary compiler, k-d stands for k-dimensional. With this dictionary compiler you can
   * index k-dimensional points.
   *
   * @param params compiler parameters
   */
  explicit KDDictionaryCompilerBase(const keyvi::util::parameters_t& params = keyvi::util::parameters_t(),
                                    float min = 0.0, float max = 1.0)
      : compiler_(params), min_(min), max_(max) {}

  KDDictionaryCompilerBase& operator=(KDDictionaryCompilerBase const&) = delete;
  KDDictionaryCompilerBase(const KDDictionaryCompilerBase& that) = delete;

  /**
   * Do the final compilation
   */
  void Compile(callback_t progress_callback = nullptr, void* user_data = nullptr) {
	kd_compiler_.Compile(nullptr, nullptr);
    compiler_.Compile(progress_callback, user_data);
  }

  /**
   * Set a custom manifest to be embedded into the index file.
   *
   * @param manifest as string
   */
  void SetManifest(const std::string& manifest) { compiler_.SetManifest(manifest); }

  void Write(std::ostream& stream) {
	  kd_compiler_.Write(stream);
	  compiler_.Write(stream);
  }

  void WriteToFile(const std::string& filename) {
	  std::ofstream out_stream = keyvi::util::OsUtils::OpenOutFileStream(filename);
	  Write(out_stream);
	  out_stream.close();
  }

 protected:
  keyvi::dictionary::DictionaryCompiler<dictionary_type_t::FLOAT_VECTOR> kd_compiler_;
  keyvi::dictionary::DictionaryCompiler<ValueStoreType> compiler_;
  float min_;
  float max_;
};

template <keyvi::dictionary::fsa::internal::value_store_t ValueStoreType, std::size_t Dimensions>
class KDDictionaryCompiler final : public KDDictionaryCompilerBase<ValueStoreType, Dimensions> {
  static_assert(true, "the number of dimensions you are asking for is currently not supported");
};

template <keyvi::dictionary::fsa::internal::value_store_t ValueStoreType>
class KDDictionaryCompiler<ValueStoreType, 2> final : public KDDictionaryCompilerBase<ValueStoreType, 2> {
 private:
  using ValueStoreT = typename fsa::internal::ValueStoreComponents<ValueStoreType>::value_store_writer_t;
  using KDDictionaryCompilerBase<ValueStoreType, 2>::kd_compiler_;
  using KDDictionaryCompilerBase<ValueStoreType, 2>::compiler_;
  using KDDictionaryCompilerBase<ValueStoreType, 2>::min_;
  using KDDictionaryCompilerBase<ValueStoreType, 2>::max_;

 public:
  explicit KDDictionaryCompiler(const keyvi::util::parameters_t& params = keyvi::util::parameters_t(), float min = 0.0,
                                float max = 1.0)
      : KDDictionaryCompilerBase<ValueStoreType, 2>(params, min, max), encoder_() {}

  void Add(const std::vector<float>& input_vector, typename ValueStoreT::value_t value = ValueStoreT::no_value) {
    if (input_vector.size() != 2) {
      throw compiler_exception("input vector size does not match dimensions");
    }

    uint64_t mapped_x1 = static_cast<std::uint64_t>(((input_vector[0] - min_) / (max_ - min_)) * (1L << 32));
    uint64_t mapped_x2 = static_cast<std::uint64_t>(((input_vector[1] - min_) / (max_ - min_)) * (1L << 32));

    // converting to big endian, important for lookup
    uint64_t encoded = htobe64(encoder_.Encode(mapped_x1, mapped_x2));
    std::string key(reinterpret_cast<const char*>(&encoded), 8);


    uint64_t v = kd_compiler_.Add2(key, input_vector);

    //size_t length;
    //uint8_t buffer[32];

    //keyvi::util::encodeVarInt(v, buffer, &length);

    //std::string key2(&buffer,length);
    std::string key2(reinterpret_cast<const char*>(&v), 8);


    compiler_.Add(key2, value);
  }

 private:
  // todo: calculate field length based on dimensions
  mortonnd::MortonNDLutEncoder<2, 32, 8> encoder_;
};

} /* namespace dictionary */
} /* namespace keyvi */

#endif  // KEYVI_DICTIONARY_K_D_DICTIONARY_COMPILER_H_

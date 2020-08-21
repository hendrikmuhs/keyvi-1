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

#ifndef KEYVI_UTIL_MSGPACK_JSON_HANDLER_H_
#define KEYVI_UTIL_MSGPACK_JSON_HANDLER_H_

#include <memory>
#include <vector>

#include "keyvi/util/msgpack_util.h"
#include "rapidjson/reader.h"

/**
 * Parse Json strings directly to msgpack.
 */

namespace keyvi {
namespace util {

class MsgPackHandler {
 public:
  class MsgpackHandlerBuffer {
   public:
    explicit MsgpackHandlerBuffer(bool single_precision_float = false)
        : buffer_stack_(), packer_stack_(), stack_level_(0), single_precision_float_(single_precision_float) {
      buffer_stack_.push_back(std::unique_ptr<msgpack_buffer>(new msgpack_buffer()));
      if (single_precision_float_) {
        buffer_stack_.back()->set_single_precision_float();
      }
      packer_stack_.push_back(
          std::unique_ptr<msgpack::packer<msgpack_buffer>>(new msgpack::packer<msgpack_buffer>(*buffer_stack_.back())));
    }

    msgpack::packer<msgpack_buffer>& ParentPacker() { return *packer_stack_[stack_level_ - 1].get(); }

    msgpack_buffer& ParrentBuffer() { return *buffer_stack_[stack_level_ - 1].get(); }

    msgpack_buffer& Buffer() { return *buffer_stack_[stack_level_].get(); }

    msgpack::packer<msgpack_buffer>& CurrentPacker() { return *packer_stack_[stack_level_].get(); }

    void Push() {
      ++stack_level_;

      if (stack_level_ >= buffer_stack_.size()) {
        buffer_stack_.push_back(std::unique_ptr<msgpack_buffer>(new msgpack_buffer()));
        if (single_precision_float_) {
          buffer_stack_.back()->set_single_precision_float();
        }
        packer_stack_.push_back(std::unique_ptr<msgpack::packer<msgpack_buffer>>(
            new msgpack::packer<msgpack_buffer>(*buffer_stack_.back())));
      }
      buffer_stack_[stack_level_]->clear();
    }

    void Pop() { --stack_level_; }

    void Shrink() {}

   private:
    std::vector<std::unique_ptr<msgpack_buffer>> buffer_stack_;
    std::vector<std::unique_ptr<msgpack::packer<msgpack_buffer>>> packer_stack_;
    size_t stack_level_;
    bool single_precision_float_;
  };

  explicit MsgPackHandler(MsgpackHandlerBuffer* buffer) : cached_buffer_(buffer) {}

  bool Null() {
    cached_buffer_->CurrentPacker().pack_nil();
    return true;
  }
  bool Bool(bool b) {
    b ? cached_buffer_->CurrentPacker().pack_true() : cached_buffer_->CurrentPacker().pack_false();
    return true;
  }

  bool Int(int i) {
    cached_buffer_->CurrentPacker().pack_int(i);
    return true;
  }

  bool Uint(unsigned u) {
    cached_buffer_->CurrentPacker().pack_unsigned_int(u);
    return true;
  }

  bool Int64(int64_t i) {
    cached_buffer_->CurrentPacker().pack_int64(i);
    return true;
  }

  bool Uint64(uint64_t u) {
    cached_buffer_->CurrentPacker().pack_uint64(u);
    return true;
  }

  bool Double(double d) {
    cached_buffer_->CurrentPacker().pack_double(d);
    return true;
  }

  bool RawNumber(const char* str, rapidjson::SizeType length, bool copy) {
    String(str, length, copy);
    return true;
  }

  bool String(const char* str, rapidjson::SizeType length, bool copy) {
    cached_buffer_->CurrentPacker().pack_str(length);
    cached_buffer_->CurrentPacker().pack_str_body(str, length);
    return true;
  }

  bool StartObject() {
    cached_buffer_->Push();
    return true;
  }

  bool Key(const char* str, rapidjson::SizeType length, bool copy) {
    String(str, length, copy);
    return true;
  }

  bool EndObject(rapidjson::SizeType memberCount) {
    cached_buffer_->ParentPacker().pack_map(memberCount);
    cached_buffer_->ParrentBuffer().write(cached_buffer_->Buffer().data(), cached_buffer_->Buffer().size());
    cached_buffer_->Pop();
    return true;
  }

  bool StartArray() {
    cached_buffer_->Push();
    return true;
  }

  bool EndArray(rapidjson::SizeType elementCount) {
    cached_buffer_->ParentPacker().pack_array(elementCount);
    cached_buffer_->ParrentBuffer().write(cached_buffer_->Buffer().data(), cached_buffer_->Buffer().size());
    cached_buffer_->Pop();

    return true;
  }

  const msgpack::sbuffer& data() { return cached_buffer_->Buffer(); }

 private:
  MsgpackHandlerBuffer* cached_buffer_;
};

} /* namespace util */
} /* namespace keyvi */

#endif  // KEYVI_UTIL_MSGPACK_JSON_HANDLER_H_

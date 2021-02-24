/* keyvi - A key value store.
 *
 * Copyright 2020 Hendrik Muhs<hendrik.muhs@gmail.com>
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
 * zip_state_traverser.h
 *
 *  Created on: Dec 16, 2020
 *      Author: hendrik
 */

#ifndef KEYVI_DICTIONARY_FSA_ZIP_STATE_TRAVERSER_H_
#define KEYVI_DICTIONARY_FSA_ZIP_STATE_TRAVERSER_H_

#include <algorithm>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <utility>
#include <vector>

#include <boost/heap/skew_heap.hpp>

#include "keyvi/dictionary/fsa/automata.h"
#include "keyvi/dictionary/fsa/comparable_state_traverser.h"

// #define ENABLE_TRACING
#include "keyvi/dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {
namespace fsa {

template <class innerTraverserType>
class ZipStateTraverser final {
 private:
  using traverser_t = std::shared_ptr<ComparableStateTraverser<innerTraverserType>>;

  struct TraverserCompare {
    bool operator()(const traverser_t &t1, const traverser_t &t2) const { return *t1 > *t2; }
  };

 public:
  using label_t = typename innerTraverserType::label_t;
  using heap_t =
      boost::heap::skew_heap<traverser_t, boost::heap::compare<TraverserCompare>, boost::heap::mutable_<true>>;
  explicit ZipStateTraverser(const std::vector<automata_t> &fsas, bool advance = true) {
    size_t order = 0;
    for (const automata_t &f : fsas) {
      traverser_t traverser = std::make_shared<ComparableStateTraverser<innerTraverserType>>(f, advance, order++);
      // the traverser could be exhausted after it has been advanced
      if (*traverser) {
        traverser_queue_.push(traverser);
      }
    }

    if (advance) {
      FillInValues();
    }
  }

  explicit ZipStateTraverser(const std::initializer_list<automata_t> fsas, bool advance = true) {
    size_t order = 0;
    for (auto f : fsas) {
      traverser_t traverser = std::make_shared<ComparableStateTraverser<innerTraverserType>>(f, advance, order++);
      // the traverser could be exhausted after it has been advanced
      if (*traverser) {
        traverser_queue_.push(traverser);
      }
    }

    if (advance) {
      FillInValues();
    }
  }

  explicit ZipStateTraverser(const std::vector<std::pair<automata_t, uint64_t>> &fsa_start_state_pairs,
                             bool advance = true) {
    size_t order = 0;
    for (auto f : fsa_start_state_pairs) {
      if (f.second > 0) {
        traverser_t traverser =
            std::make_shared<ComparableStateTraverser<innerTraverserType>>(f.first, f.second, advance, order++);
        // the traverser could be exhausted after it has been advanced
        if (*traverser) {
          traverser_queue_.push(traverser);
        }
      }
    }

    if (advance) {
      FillInValues();
    }
  }

  ZipStateTraverser() = delete;
  ZipStateTraverser &operator=(ZipStateTraverser const &) = delete;
  ZipStateTraverser(const ZipStateTraverser &that) = delete;

  void operator++(int) {
    TRACE("iterator++, forwarding %ld inner traversers", equal_states_);

    while (equal_states_ > 0) {
      // get the top element
      auto it = traverser_queue_.begin();

      // advance the inner traverser and update or remove it from the queue
      (*it)->operator++(0);
      if (*(*it)) {
        traverser_queue_.decrease(heap_t::s_handle_from_iterator(it));
      } else {
        traverser_queue_.erase(heap_t::s_handle_from_iterator(it));
      }
      --equal_states_;
    }
    FillInValues();
  }

  operator bool() const { return !traverser_queue_.empty(); }

  bool IsFinalState() const { return final_; }

  size_t GetDepth() const { return depth_; }

  uint64_t GetStateValue() const { return state_value_; }

  uint32_t GetInnerWeight() const { return inner_weight_; }

  uint64_t GetStateId() const { return state_id_; }

  size_t GetOrder() const { return order_; }

  void Prune() {
    while (equal_states_ > 0) {
      // get the top element
      auto it = traverser_queue_.begin();

      // prune the inner traverser and update or remove it from the queue
      (*it)->Prune();
      if (*(*it)) {
        traverser_queue_.decrease(heap_t::s_handle_from_iterator(it));
      } else {
        traverser_queue_.erase(heap_t::s_handle_from_iterator(it));
      }
      --equal_states_;
    }
    FillInValues();
  }

  label_t GetStateLabel() const { return state_label_; }

 private:
  heap_t traverser_queue_;
  bool final_ = false;
  size_t depth_ = 0;
  uint64_t state_value_ = 0;
  uint32_t inner_weight_ = 0;
  uint64_t state_id_ = 0;
  label_t state_label_ = 0;
  size_t order_;
  automata_t fsa_;
  size_t equal_states_ = 1;

  void FillInValues() {
    TRACE("fill in");

    if (!traverser_queue_.empty()) {
      const traverser_t &t = traverser_queue_.top();

      final_ = t->IsFinalState();
      depth_ = t->GetDepth();
      state_value_ = t->GetStateValue();
      inner_weight_ = t->GetInnerWeight();
      state_id_ = t->GetStateId();
      state_label_ = t->GetStateLabel();
      fsa_ = t->GetFsa();
      order_ = t->GetOrder();
      // memorize how many inner traverser are at a equal inner state
      equal_states_ = 1;

      // traverse the queue in _sorted_ order
      auto it = traverser_queue_.ordered_begin();
      it++;

      while (traverser_queue_.size() > equal_states_ && *t == *(*it)) {
        TRACE("dedup");
        equal_states_++;
        // if not final yet check if other states are final
        if (!final_ && (*it)->IsFinalState()) {
          final_ = true;
          state_value_ = (*it)->GetStateValue();
          fsa_ = t->GetFsa();
          order_ = t->GetOrder();
        }

        // take the max from inner weights
        if ((*it)->GetInnerWeight() > inner_weight_) {
          inner_weight_ = (*it)->GetInnerWeight();
        }
        it++;
      }
    }
  }
};

} /* namespace fsa */
} /* namespace dictionary */
} /* namespace keyvi */

#endif  // KEYVI_DICTIONARY_FSA_ZIP_STATE_TRAVERSER_H_

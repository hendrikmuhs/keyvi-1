/* * keyvi - A key value store.
 *
 * Copyright 2018 Hendrik Muhs<hendrik.muhs@gmail.com>
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
 * expression_matching.h
 *
 *  Created on: Mar 6, 2018
 *      Author: hendrik
 */

#ifndef KEYVI_DICTIONARY_MATCHING_EXPRESSION_MATCHING_H_
#define KEYVI_DICTIONARY_MATCHING_EXPRESSION_MATCHING_H_

#include <algorithm>
#include <string>
#include <vector>

#include "dictionary/fsa/automata.h"
#include "dictionary/match.h"

// #define ENABLE_TRACING
#include "dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {
namespace matching {

class ExpressionMatching final {
 public:
  ExpressionMatching(const fsa::automata_t& fsa, const std::string& query)
      : fsa_(fsa), query_(query), stack_(), first_match_() {
    // pre-allocate space for the stack
    stack_.reserve(std::min(query.length(), 50ul));

    stack_.emplace_back(MatchingState::EXAXT, fsa_->GetStartState(), 0, 0);
    uint64_t state = TraverseToNextFinalState();
    if (state) {
      first_match_ = Match(0, query_.length(), query_, 0, fsa_, fsa_->GetStateValue(state));
    }
  }

  Match FirstMatch() const { return first_match_; }

  Match NextMatch() {
    uint64_t state = TraverseToNextFinalState();

    if (state) {
      return Match(0, query_.length(), query_, 0, fsa_, fsa_->GetStateValue(state));
    }

    return Match();
  }

 private:
  enum class MatchingState { EXAXT = 0, FOLLOW_WILDCARD_EXPRESSION = 1, WILDCARD_EXPRESSION = 2, NO_MORE_MATCHING = 3 };

  struct TransitionInfo {
    TransitionInfo(MatchingState matching_state, uint64_t state, size_t offset_in_query, size_t expression_depth)
        : matching_state_(matching_state),
          state_(state),
          offset_in_query_(offset_in_query),
          expression_depth_(expression_depth) {}

    MatchingState matching_state_;
    uint64_t state_;
    size_t offset_in_query_;
    size_t expression_depth_;
  };

  fsa::automata_t fsa_;
  const std::string& query_;

  std::vector<TransitionInfo> stack_;
  Match first_match_;

  uint64_t TraverseToNextFinalState() {
    for (;;) {
      // check if stack run out of candidates
      if (stack_.empty()) {
        TRACE("run out of candidates");
        break;
      }

      TransitionInfo& info = stack_.back();
      // ensure that we do not run over the query string
      if (info.offset_in_query_ == query_.length()) {
        uint64_t state = info.state_;
        stack_.pop_back();

        // check for final state
        if (fsa_->IsFinalState(state)) {
          TRACE("found final state");
          return state;
        }
        TRACE("stack at end but no final state found");
        continue;
      }

      if (info.matching_state_ == MatchingState::EXAXT) {
        // remember that we tried to match exact
        info.matching_state_ =
            info.expression_depth_ > 0 ? MatchingState::FOLLOW_WILDCARD_EXPRESSION : MatchingState::WILDCARD_EXPRESSION;
        TRACE("try to match exact");

        size_t state = fsa_->TryWalkTransition(info.state_, query_[info.offset_in_query_]);

        if (state) {
          TRACE("matched %c", query_[info.offset_in_query_]);
          stack_.emplace_back(MatchingState::EXAXT, state, info.offset_in_query_ + 1, 0);
          continue;
        }
        // todo: hardcoded for wild card, support other expressions
      }

      if (info.matching_state_ == MatchingState::FOLLOW_WILDCARD_EXPRESSION) {
        TRACE("follow expression");
        info.matching_state_ = MatchingState::WILDCARD_EXPRESSION;
        stack_.emplace_back(MatchingState::EXAXT, info.state_, info.offset_in_query_ + 1, info.expression_depth_ + 1);
        continue;
      }

      if (info.matching_state_ == MatchingState::WILDCARD_EXPRESSION) {
        // remember that we tried this expression
        info.matching_state_ = MatchingState::NO_MORE_MATCHING;

        TRACE("try to match expression");

        // hardcoded wildcard to '\x1b'
        size_t state = fsa_->TryWalkTransition(info.state_, '\x1b');
        if (state) {
          TRACE("expression match %c", query_[info.offset_in_query_]);
          stack_.emplace_back(MatchingState::EXAXT, state, info.offset_in_query_ + 1, 1);

          continue;
        }
      }
      // no match or expression, pop stack and continue
      TRACE("dead end, pop stack");
      stack_.pop_back();
    }
    return 0ul;
  }
};

}  // namespace matching
}  // namespace dictionary
}  // namespace keyvi

#endif  // KEYVI_DICTIONARY_MATCHING_EXPRESSION_MATCHING_H_

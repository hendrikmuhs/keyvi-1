//
// keyvi - A key value store.
//
// Copyright 2015 Hendrik Muhs<hendrik.muhs@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

/*
 * automata_test.cpp
 *
 *  Created on: Sep 20, 2015
 *      Author: hendrik
 */
#include <smmintrin.h>
#include <immintrin.h>

#include <boost/test/unit_test.hpp>

#include "keyvi/dictionary/fsa/automata.h"
#include "keyvi/dictionary/fsa/traverser_types.h"
#include "keyvi/testing/temp_dictionary.h"

namespace keyvi {
namespace dictionary {
namespace fsa {

BOOST_AUTO_TEST_SUITE(AutomataTests)

BOOST_AUTO_TEST_CASE(GetOutGoingTransitionsTest) {
  std::vector<std::string> test_data = {"\01cd", "aaaa", "aabb", "agbc", "ajcd", "azcd"};
  testing::TempDictionary dictionary(&test_data);
  automata_t f = dictionary.GetFsa();

  traversal::TraversalStack<> stack;

  f->GetOutGoingTransitions<SSE42>(f->GetStartState(), &stack.GetStates(), &stack.traversal_stack_payload);

  BOOST_CHECK_EQUAL(2, stack.GetStates().traversal_state_payload.transitions.size());
  BOOST_CHECK_EQUAL(f->TryWalkTransition(f->GetStartState(), '\01'),
                    stack.GetStates().traversal_state_payload.transitions[0].state);
  BOOST_CHECK_EQUAL(f->TryWalkTransition(f->GetStartState(), 'a'),
                    stack.GetStates().traversal_state_payload.transitions[1].state);
  BOOST_CHECK_EQUAL('\01', stack.GetStates().traversal_state_payload.transitions[0].label);
  BOOST_CHECK_EQUAL('a', stack.GetStates().traversal_state_payload.transitions[1].label);

  // check all outgoings for 'a'
  uint32_t state_a = f->TryWalkTransition(f->GetStartState(), 'a');

  f->GetOutGoingTransitions<SSE42>(state_a, &stack.GetStates(), &stack.traversal_stack_payload);

  BOOST_CHECK_EQUAL(4, stack.GetStates().traversal_state_payload.transitions.size());
  BOOST_CHECK_EQUAL(f->TryWalkTransition(state_a, 'a'), stack.GetStates().traversal_state_payload.transitions[0].state);
  BOOST_CHECK_EQUAL(f->TryWalkTransition(state_a, 'g'), stack.GetStates().traversal_state_payload.transitions[1].state);
  BOOST_CHECK_EQUAL(f->TryWalkTransition(state_a, 'j'), stack.GetStates().traversal_state_payload.transitions[2].state);
  BOOST_CHECK_EQUAL(f->TryWalkTransition(state_a, 'z'), stack.GetStates().traversal_state_payload.transitions[3].state);

  BOOST_CHECK_EQUAL('a', stack.GetStates().traversal_state_payload.transitions[0].label);
  BOOST_CHECK_EQUAL('g', stack.GetStates().traversal_state_payload.transitions[1].label);
  BOOST_CHECK_EQUAL('j', stack.GetStates().traversal_state_payload.transitions[2].label);
  BOOST_CHECK_EQUAL('z', stack.GetStates().traversal_state_payload.transitions[3].label);
}

BOOST_AUTO_TEST_CASE(GetOutGoingTransitionsWeightTest) {
  std::vector<std::pair<std::string, uint32_t>> test_data = {
      {"the fox jumped over the fence and broke his nose", 22},
      {"the fox jumped over the fence and broke his feet", 24},
      {"the fox jumped over the fence and broke his tongue", 444},
      {"the fox jumped over the fence and broke his arm", 2},
  };
  testing::TempDictionary dictionary(&test_data);
  automata_t f = dictionary.GetFsa();

  traversal::TraversalStack<traversal::WeightedTransition> stack;

  f->GetOutGoingTransitions<SSE42>(f->GetStartState(), &stack.GetStates(), &stack.traversal_stack_payload, 42);

  BOOST_CHECK_EQUAL(1, stack.GetStates().traversal_state_payload.transitions.size());
  BOOST_CHECK_EQUAL(444, stack.GetStates().traversal_state_payload.transitions[0].weight);
}

BOOST_AUTO_TEST_CASE(EmptyTest) {
  std::vector<std::pair<std::string, uint32_t>> test_data = {};
  testing::TempDictionary dictionary(&test_data);

  BOOST_CHECK(dictionary.GetFsa()->Empty());
}

BOOST_AUTO_TEST_CASE(AvxExp) {
   unsigned char s[] = {
    0x00, 0x01, 0x99, 0x99, 0x99, 0x05, 0x06, 0x99, 0x99, 0x09, 0x99, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12,
    0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 
    
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25,
    0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};
   // OUTGOING_TRANSITIONS_MASK

    __m256i* labels_as_m256 = reinterpret_cast<__m256i*>(&s);
    __m256i* mask_as_m256 = reinterpret_cast<__m256i*>(OUTGOING_TRANSITIONS_MASK);

    // check 32 bytes at a time
//    for (int offset = 0; offset < 8; ++offset) {(
    __m256i mask =  _mm256_cmpeq_epi8(_mm256_loadu_si256(labels_as_m256), _mm256_loadu_si256(mask_as_m256));
    uint32_t mask_int = _mm256_movemask_epi8(mask);
    std::cout << mask_int <<std::endl;

    __m128i* labels_as_m128 = reinterpret_cast<__m128i*>(&s);
    __m128i* mask_as_m128 = reinterpret_cast<__m128i*>(OUTGOING_TRANSITIONS_MASK);

    // check 16 bytes at a time
    __m128i mask_128 =
          _mm_cmpestrm(_mm_loadu_si128(labels_as_m128), 16, _mm_loadu_si128(mask_as_m128), 16,
                       _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_MASKED_POSITIVE_POLARITY | _SIDD_BIT_MASK);

     uint16_t mask_int_128 = _mm_extract_epi16(mask_128, 0);
     std::cout << mask_int_128 <<std::endl;

}

BOOST_AUTO_TEST_SUITE_END()

} /* namespace fsa */
} /* namespace dictionary */
} /* namespace keyvi */

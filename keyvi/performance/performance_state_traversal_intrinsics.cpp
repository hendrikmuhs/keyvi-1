/* keyvi - A key value store.
 *
 * Copyright 2024 Hendrik Muhs<hendrik.muhs@gmail.com>
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
 * performance_state_traversal_intrinsics.cpp
 */

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#ifdef __AVX2__
#include <immintrin.h>
#include <smmintrin.h>
#endif

#include <chrono>  // NOLINT [build/c++11]
#include <cstdint>
#include <iostream>
#include <random>
#include <vector>

#include "keyvi/dictionary/fsa/automata.h"

using clock_type = std::chrono::system_clock;

uint64_t OUTGOING_TRANSITIONS_MASK_64[32];
#ifdef __SSE4_2__
__m128i OUTGOING_TRANSITIONS_MASK_128[16];
#endif
#ifdef __AVX2__
__m256i OUTGOING_TRANSITIONS_MASK_256[8];
#endif

inline void GetOutGoingStatesSSE42(const unsigned char* state,
                                   keyvi::dictionary::fsa::traversal::TraversalState<>* traversal_state,
                                   keyvi::dictionary::fsa::traversal::TraversalPayload<>* traversal_payload) {
#ifdef __SSE4_2__
  const __m128i* labels_as_m128 = reinterpret_cast<const __m128i*>(state);
  unsigned char symbol = 0;

  // check 16 bytes at a time
  for (int offset = 0; offset < 16; ++offset, ++labels_as_m128, symbol += 16) {
    __m128i mask =
        _mm_cmpestrm(_mm_loadu_si128(labels_as_m128), 16, OUTGOING_TRANSITIONS_MASK_128[offset], 16,
                     _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_MASKED_POSITIVE_POLARITY | _SIDD_BIT_MASK);

    uint64_t mask_int = _mm_extract_epi64(mask, 0);

    while (mask_int != 0) {
      uint64_t t = mask_int & -mask_int;
      int r = __builtin_ctzl(mask_int);
      traversal_state->Add(symbol + r, symbol + r, traversal_payload);
      mask_int ^= t;
    }
  }

  traversal_state->PostProcess(traversal_payload);
#endif
}

inline void GetOutGoingStatesAVX(const unsigned char* state,
                                 keyvi::dictionary::fsa::traversal::TraversalState<>* traversal_state,
                                 keyvi::dictionary::fsa::traversal::TraversalPayload<>* traversal_payload) {
#ifdef __AVX2__
  const __m256i* labels_as_m256 = reinterpret_cast<const __m256i*>(state);
  unsigned char symbol = 0;

  // check 32 bytes at a time
  for (int offset = 0; offset != 8; ++offset, ++labels_as_m256, symbol += 32) {
    __m256i mask = _mm256_cmpeq_epi8(_mm256_loadu_si256(labels_as_m256), OUTGOING_TRANSITIONS_MASK_256[offset]);

    uint32_t mask_int = _mm256_movemask_epi8(mask);

    while (mask_int != 0) {
      uint64_t t = mask_int & -mask_int;
      int r = __builtin_ctzl(mask_int);
      traversal_state->Add(symbol + r, symbol + r, traversal_payload);
      mask_int ^= t;
    }
  }
  traversal_state->PostProcess(traversal_payload);
#endif
}

inline void GetOutGoingStatesXOR(const unsigned char* state,
                                 keyvi::dictionary::fsa::traversal::TraversalState<>* traversal_state,
                                 keyvi::dictionary::fsa::traversal::TraversalPayload<>* traversal_payload) {
  const uint64_t* labels_as_ll = reinterpret_cast<const uint64_t*>(state);
  unsigned char symbol = 0;

  // check 8 bytes at a time
  for (int offset = 0; offset < 32; ++offset, ++labels_as_ll, symbol += 8) {
    uint64_t xor_labels_with_mask = *labels_as_ll ^ OUTGOING_TRANSITIONS_MASK_64[offset];
    if (xor_labels_with_mask != 0) {
      if ((xor_labels_with_mask & 0x00000000000000ffULL) == 0) {
        traversal_state->Add(symbol, symbol, traversal_payload);
      }
      if ((xor_labels_with_mask & 0x000000000000ff00ULL) == 0) {
        traversal_state->Add(symbol + 1, symbol + 1, traversal_payload);
      }
      if ((xor_labels_with_mask & 0x0000000000ff0000ULL) == 0) {
        traversal_state->Add(symbol + 2, symbol + 2, traversal_payload);
      }
      if ((xor_labels_with_mask & 0x00000000ff000000ULL) == 0) {
        traversal_state->Add(symbol + 3, symbol + 3, traversal_payload);
      }
      if ((xor_labels_with_mask & 0x000000ff00000000ULL) == 0) {
        traversal_state->Add(symbol + 4, symbol + 4, traversal_payload);
      }
      if ((xor_labels_with_mask & 0x0000ff0000000000ULL) == 0) {
        traversal_state->Add(symbol + 5, symbol + 5, traversal_payload);
      }
      if ((xor_labels_with_mask & 0x00ff000000000000ULL) == 0) {
        traversal_state->Add(symbol + 6, symbol + 6, traversal_payload);
      }
      if ((xor_labels_with_mask & 0xff00000000000000ULL) == 0) {
        traversal_state->Add(symbol + 7, symbol + 7, traversal_payload);
      }
    }
  }

  traversal_state->PostProcess(traversal_payload);
}

bool CompareTransitions(const keyvi::dictionary::fsa::traversal::TraversalState<>& a,
                        const keyvi::dictionary::fsa::traversal::TraversalState<>& b) {
  if (a.size() != b.size()) {
    std::cerr << "expected size [" << a.size() << "] got [" << b.size() << std::endl;
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (a.traversal_state_payload.transitions[i].state != b.traversal_state_payload.transitions[i].state) {
      std::cerr << "expected state [" << std::to_string(a.traversal_state_payload.transitions[i].state) << "] got ["
                << std::to_string(b.traversal_state_payload.transitions[i].state) << "]" << std::endl;
      return false;
    }
    if (a.traversal_state_payload.transitions[i].label != b.traversal_state_payload.transitions[i].label) {
      std::cerr << "expected label [" << a.traversal_state_payload.transitions[i].label << "] got ["
                << b.traversal_state_payload.transitions[i].label << "]" << std::endl;
      return false;
    }
  }
  return true;
}

clock_type::duration CheckImplementations(const std::vector<std::vector<unsigned char>>& test_states) {
  auto const start = clock_type::now();

  keyvi::dictionary::fsa::traversal::TraversalPayload<> traversal_payload;
  keyvi::dictionary::fsa::traversal::TraversalState<> traversal_state[3];

  for (auto it : test_states) {
    traversal_state[0].Clear();
    GetOutGoingStatesXOR(it.data(), &traversal_state[0], &traversal_payload);
    traversal_state[1].Clear();
    GetOutGoingStatesSSE42(it.data(), &traversal_state[1], &traversal_payload);
    traversal_state[2].Clear();
    GetOutGoingStatesAVX(it.data(), &traversal_state[2], &traversal_payload);

#ifdef __SSE4_2__
    if (CompareTransitions(traversal_state[0], traversal_state[1]) == false) {
      std::cerr << "mismatch between XOR and SSE42" << std::endl;
      std::exit(1);
    }
#endif
#ifdef __AVX2__
    if (CompareTransitions(traversal_state[1], traversal_state[2]) == false) {
      std::cerr << "mismatch between XOR and AVX" << std::endl;
      std::exit(1);
    }
#endif
  }
  return clock_type::now() - start;
}

clock_type::duration BenchXOR(const std::vector<std::vector<unsigned char>>& test_states) {
  auto const start = clock_type::now();

  keyvi::dictionary::fsa::traversal::TraversalPayload<> traversal_payload;
  keyvi::dictionary::fsa::traversal::TraversalState<> traversal_state;

  for (auto it : test_states) {
    traversal_state.Clear();
    GetOutGoingStatesXOR(it.data(), &traversal_state, &traversal_payload);
  }
  return clock_type::now() - start;
}

clock_type::duration BenchSSE42(const std::vector<std::vector<unsigned char>>& test_states) {
#ifdef __SSE4_2__
  auto const start = clock_type::now();

  keyvi::dictionary::fsa::traversal::TraversalPayload<> traversal_payload;
  keyvi::dictionary::fsa::traversal::TraversalState<> traversal_state;

  for (auto it : test_states) {
    traversal_state.Clear();
    GetOutGoingStatesSSE42(it.data(), &traversal_state, &traversal_payload);
  }
  return clock_type::now() - start;
#else
  return clock_type::duration::max();
#endif
}

clock_type::duration BenchAVX(const std::vector<std::vector<unsigned char>>& test_states) {
#ifdef __AVX2__
  auto const start = clock_type::now();

  keyvi::dictionary::fsa::traversal::TraversalPayload<> traversal_payload;
  keyvi::dictionary::fsa::traversal::TraversalState<> traversal_state;

  for (auto it : test_states) {
    traversal_state.Clear();
    GetOutGoingStatesAVX(it.data(), &traversal_state, &traversal_payload);
  }
  return clock_type::now() - start;
#else
  return clock_type::duration::max();
#endif
}

int main() {
  const int rounds = 1000000;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib_this_state(1, 50);
  std::uniform_int_distribution<> distrib_some_byte(1, 255);

  std::vector<std::vector<unsigned char>> test_states;

  for (int i = 0; i != rounds; ++i) {
    // generate a state
    std::vector<unsigned char> a_state;
    for (int n = 0; n != 255; ++n) {
      // with a 2% chance push the right state
      if (distrib_this_state(gen) == 5) {
        a_state.push_back(n);
        continue;
      }
      a_state.push_back(distrib_some_byte(gen));
    }
    test_states.push_back(a_state);
  }

  uint64_t* mask_as_64t = reinterpret_cast<uint64_t*>(keyvi::dictionary::fsa::OUTGOING_TRANSITIONS_MASK);
  for (int i = 0; i != 32; ++i) {
    OUTGOING_TRANSITIONS_MASK_64[i] = *mask_as_64t;
    ++mask_as_64t;
  }
#ifdef __SSE4_2__
  __m128i* mask_as_m128 = reinterpret_cast<__m128i*>(keyvi::dictionary::fsa::OUTGOING_TRANSITIONS_MASK);
  for (int i = 0; i != 16; ++i) {
    OUTGOING_TRANSITIONS_MASK_128[i] = _mm_loadu_si128(mask_as_m128);
    ++mask_as_m128;
  }
#endif
#ifdef __AVX2__
  __m256i* mask_as_m256 = reinterpret_cast<__m256i*>(keyvi::dictionary::fsa::OUTGOING_TRANSITIONS_MASK);
  for (int i = 0; i != 8; ++i) {
    OUTGOING_TRANSITIONS_MASK_256[i] = _mm256_loadu_si256(mask_as_m256);
    ++mask_as_m256;
  }
#endif

  CheckImplementations(test_states);

  auto duration_generic = BenchXOR(test_states);
  std::cout << "Generic implementation: " << duration_generic.count() / rounds << "ns" << std::endl;
  auto duration_sse42 = BenchSSE42(test_states);
  std::cout << "SSE4.2 implementation: " << duration_sse42.count() / rounds << "ns" << std::endl;
  auto duration_avx = BenchAVX(test_states);
  std::cout << "AVX implementation: " << duration_avx.count() / rounds << "ns" << std::endl;
}

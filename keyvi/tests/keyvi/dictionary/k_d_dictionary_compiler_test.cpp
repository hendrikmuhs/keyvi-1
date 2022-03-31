//
// keyvi - A key value store.
//
// Copyright 2021 Hendrik Muhs<hendrik.muhs@gmail.com>
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

#include <boost/test/unit_test.hpp>

#include "keyvi/dictionary/dictionary_compiler.h"
#include "keyvi/dictionary/dictionary_types.h"
#include "keyvi/dictionary/fsa/internal/json_value_store.h"
#include "keyvi/dictionary/k_d_dictionary.h"
#include "keyvi/dictionary/k_d_dictionary_compiler.h"
#include "keyvi/dictionary/match.h"
#include "keyvi/dictionary/match_iterator.h"

namespace keyvi {
namespace dictionary {

BOOST_AUTO_TEST_SUITE(KDDictionaryCompilerTests)

BOOST_AUTO_TEST_CASE(simple) {
  KDDictionaryCompiler<DictionaryCompiler<dictionary_type_t::JSON>, 2> compiler(
      keyvi::util::parameters_t({{"memory_limit_mb", "10"}}));

  compiler.Add({0.2, 0.3}, "1");
  compiler.Add({0.200000001, 0.3000000003}, "1.5");

  compiler.Add({0.1111, 0.3214}, "2");
  compiler.Add({0.2, 0.888}, "3");

  compiler.Compile();
  compiler.WriteToFile("kdd_test.kv");
  k_d_dictionary_t d(new KDDictionary("kdd_test.kv"));

  Match m = d->operator[]({0.2, 0.3});
  BOOST_CHECK_EQUAL("1", m.GetValueAsString());

  for (auto m : d->GetNearestNeighbors({0.20000000000000002222, 0.3000000000000333})) {
    std::cout << m.GetValueAsString() << std::endl;
  }
}

BOOST_AUTO_TEST_SUITE_END()

} /* namespace dictionary */
} /* namespace keyvi */

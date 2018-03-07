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
 * expression_matching_test.cpp
 *
 *  Created on: Mar 7, 2018
 *      Author: hendrik
 */

#include <boost/test/unit_test.hpp>

#include "dictionary/matching/expression_matching.h"
#include "testing/temp_dictionary.h"

namespace keyvi {
namespace dictionary {
namespace matching {

BOOST_AUTO_TEST_SUITE(ExpressionMatchingTests)

BOOST_AUTO_TEST_CASE(exact) {
  std::vector<std::pair<std::string, std::string>> test_data = {
      {"abc", "{a:1}"}, {"abbc", "{b:2}"}, {"abbcd", "{c:3}"}};
  testing::TempDictionary dictionary = testing::TempDictionary::makeTempDictionaryFromJson(&test_data);

  ExpressionMatching expression_matcher(dictionary.GetFsa(), "abc");

  BOOST_CHECK(!expression_matcher.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abc", expression_matcher.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:1}\"", expression_matcher.FirstMatch().GetValueAsString());

  ExpressionMatching expression_matcher2(dictionary.GetFsa(), "abbc");
  BOOST_CHECK(!expression_matcher2.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abbc", expression_matcher2.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{b:2}\"", expression_matcher2.FirstMatch().GetValueAsString());
}

BOOST_AUTO_TEST_CASE(wildcard_expression) {
  std::string expression("a");
  expression += '\x1b';
  expression += 'c';

  std::vector<std::pair<std::string, std::string>> test_data = {{expression, "{a:1}"}};
  testing::TempDictionary dictionary = testing::TempDictionary::makeTempDictionaryFromJson(&test_data);

  ExpressionMatching expression_matcher(dictionary.GetFsa(), "abc");

  BOOST_CHECK(!expression_matcher.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abc", expression_matcher.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:1}\"", expression_matcher.FirstMatch().GetValueAsString());

  BOOST_CHECK(expression_matcher.NextMatch().IsEmpty());

  ExpressionMatching expression_matcher2(dictionary.GetFsa(), "abbc");

  BOOST_CHECK(!expression_matcher2.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abbc", expression_matcher2.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:1}\"", expression_matcher2.FirstMatch().GetValueAsString());

  BOOST_CHECK(expression_matcher2.NextMatch().IsEmpty());

  ExpressionMatching expression_matcher3(dictionary.GetFsa(), "agegeiec");

  BOOST_CHECK(!expression_matcher3.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("agegeiec", expression_matcher3.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:1}\"", expression_matcher3.FirstMatch().GetValueAsString());

  BOOST_CHECK(expression_matcher3.NextMatch().IsEmpty());

  ExpressionMatching expression_matcher4(dictionary.GetFsa(), "abbcaa");

  BOOST_CHECK(expression_matcher4.FirstMatch().IsEmpty());
  ExpressionMatching expression_matcher5(dictionary.GetFsa(), "abb");

  BOOST_CHECK(expression_matcher5.FirstMatch().IsEmpty());
}

BOOST_AUTO_TEST_CASE(wildcard_expression_multiple_wildcards) {
  std::string expression("a");
  expression += '\x1b';
  expression += 'c';
  expression += '\x1b';
  expression += 'e';

  std::vector<std::pair<std::string, std::string>> test_data = {{expression, "{a:1}"}};
  testing::TempDictionary dictionary = testing::TempDictionary::makeTempDictionaryFromJson(&test_data);

  ExpressionMatching expression_matcher(dictionary.GetFsa(), "abcde");

  BOOST_CHECK(!expression_matcher.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abcde", expression_matcher.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:1}\"", expression_matcher.FirstMatch().GetValueAsString());

  BOOST_CHECK(expression_matcher.NextMatch().IsEmpty());

  ExpressionMatching expression_matcher2(dictionary.GetFsa(), "abbcddddeee");

  BOOST_CHECK(!expression_matcher2.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abbcddddeee", expression_matcher2.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:1}\"", expression_matcher2.FirstMatch().GetValueAsString());

  BOOST_CHECK(expression_matcher2.NextMatch().IsEmpty());
}

BOOST_AUTO_TEST_CASE(wildcard_expression_multiple_expressions) {
  std::string expression("a");
  expression += '\x1b';
  expression += 'c';
  expression += '\x1b';
  expression += 'e';

  std::string expression2("a");
  expression2 += '\x1b';
  expression2 += "xyz";
  expression2 += '\x1b';
  expression2 += "vw";

  std::vector<std::pair<std::string, std::string>> test_data = {{expression, "{a:1}"}, {expression2, "{a:2}"}};
  testing::TempDictionary dictionary = testing::TempDictionary::makeTempDictionaryFromJson(&test_data);

  ExpressionMatching expression_matcher(dictionary.GetFsa(), "abcde");

  BOOST_CHECK(!expression_matcher.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abcde", expression_matcher.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:1}\"", expression_matcher.FirstMatch().GetValueAsString());

  BOOST_CHECK(expression_matcher.NextMatch().IsEmpty());

  ExpressionMatching expression_matcher2(dictionary.GetFsa(), "abcxyzcdevw");

  BOOST_CHECK(!expression_matcher2.FirstMatch().IsEmpty());
  BOOST_CHECK_EQUAL("abcxyzcdevw", expression_matcher2.FirstMatch().GetMatchedString());
  BOOST_CHECK_EQUAL("\"{a:2}\"", expression_matcher2.FirstMatch().GetValueAsString());

  BOOST_CHECK(expression_matcher2.NextMatch().IsEmpty());
}

BOOST_AUTO_TEST_SUITE_END()

} /* namespace matching */
} /* namespace dictionary */
} /* namespace keyvi */

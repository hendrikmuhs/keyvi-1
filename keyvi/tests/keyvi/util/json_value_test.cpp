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
 * json_value_test.cpp
 *
 *  Created on: Oct 27, 2016
 *      Author: hendrik
 */

#include <iostream>
#include <sstream>

#include <boost/test/unit_test.hpp>

#include "keyvi/util/json_value.h"
#include "keyvi/util/msgpack_json_handler.h"
#include "rapidjson/reader.h"

namespace keyvi {
namespace util {

BOOST_AUTO_TEST_SUITE(JsonValueTests)

BOOST_AUTO_TEST_CASE(EncodeDecodeTest) {
  std::string input = "{'auto':'car','price':344,'features':[1,2,3]}";
  std::string encoded = EncodeJsonValue(input);

  std::string output = DecodeJsonValue(encoded);

  BOOST_CHECK_EQUAL('"' + input + '"', output);
}

BOOST_AUTO_TEST_CASE(TestHandler) {
  const char json[] =
      " { \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3, "
      "4], \"d\" : {\"k\" : \"v\"} }";

  MsgPackHandler::MsgpackHandlerBuffer msgpack_buffer;
  MsgPackHandler handler(&msgpack_buffer);
  rapidjson::Reader reader;
  rapidjson::StringStream ss(json);
  reader.Parse(ss, handler);

  // msgpack::unpacked doc;
  msgpack::object_handle doc = msgpack::unpack(handler.data().data(), handler.data().size());
  msgpack::object deserialized = doc.get();

  // msgpack::object supports ostream.
  std::stringstream sbuffer;
  sbuffer << deserialized;
  std::cout << sbuffer.str() << std::endl;

  // std::cout << doc << std::endl;

  rapidjson::Document json_document;
  doc.get().convert(&json_document);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  json_document.Accept(writer);
  std::cout << buffer.GetString() << std::endl;
}

BOOST_AUTO_TEST_SUITE_END()

} /* namespace util */
} /* namespace keyvi */

/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.ipc.codec.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import reactor.ipc.buffer.Buffer
import reactor.ipc.codec.json.JsonCodec
import spock.lang.Specification

import java.util.function.Consumer
import java.util.function.Function

class JsonCodecSpec extends Specification {

	def "JSON can be decoded into a Map"() {
		given: 'A JSON codec'
		JsonCodec<Map<String, Object>, Object> codec = new JsonCodec<Map<String, Object>, Object>(Map);

		when: 'The decoder is passed some JSON'
		Map<String, Object> decoded;
		Function<Buffer, Map<String, Object>> decoder = codec.decoder({ decoded = it} as Consumer<Map<String, Object>>)
		decoder.apply(Buffer.wrap("{\"a\": \"alpha\"}"));

		then: 'The decoded map has the expected entries'
		decoded.size() == 1
		decoded['a'] == 'alpha'
	}

	def "JSON can be decoded into a JsonNode"() {
		given: 'A JSON codec'
		JsonCodec<JsonNode, Object> codec = new JsonCodec<JsonNode, Object>(JsonNode);

		when: 'The decoder is passed some JSON'
		JsonNode decoded
		Function<Buffer, JsonNode> decoder = codec.decoder({ decoded = it} as Consumer<JsonNode>)
		decoder.apply(Buffer.wrap("{\"a\": \"alpha\"}"));

		then: 'The decoded JsonNode is an object node with the expected entries'
		decoded instanceof ObjectNode
		decoded.get('a').textValue() == 'alpha'
	}

}

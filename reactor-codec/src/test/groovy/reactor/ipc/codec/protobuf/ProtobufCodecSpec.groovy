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
package reactor.ipc.codec.protobuf

import reactor.ipc.buffer.Buffer
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class ProtobufCodecSpec extends Specification {

	TestObjects.RichObject obj

	def setup() {
		obj = TestObjects.RichObject.newBuilder().
				setName("first").
				setPercent(0.5f).
				setTotal(100l).
				build()
	}

	def "properly serializes and deserializes objects"() {

		given: "a ProtobufCodec and a Buffer"
			def codec = new ProtobufCodec<TestObjects.RichObject, TestObjects.RichObject>()
			Buffer buffer

		when: "an object is serialized"
			buffer = codec.apply(obj)

		then: "the object ws serialized"
			buffer.remaining() == 71

		when: "an object is deserialized"
			TestObjects.RichObject newObj = codec.decoder(null).apply(buffer)

		then: "the object was deserialized"
			newObj.name == "first"
			newObj.percent == 0.5f
			newObj.total == 100l

	}

}

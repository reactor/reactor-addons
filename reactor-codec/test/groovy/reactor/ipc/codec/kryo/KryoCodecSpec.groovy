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
package reactor.ipc.codec.kryo

import com.esotericsoftware.kryo.Kryo
import reactor.ipc.buffer.Buffer
import reactor.ipc.codec.kryo.KryoCodec
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class KryoCodecSpec extends Specification {

	Kryo kryo

	def setup() {
		kryo = new Kryo()
		kryo.register(RichObject)
	}

	def "properly serializes and deserializes objects"() {

		given: "a Kryo codec and a Buffer"
			def codec = new KryoCodec<RichObject, RichObject>(kryo, true)
			RichObject obj = new RichObject("first", 0.5f, 100l)
			Buffer buffer

		when: "an objects are serialized"
			buffer = codec.apply(obj)

		then: "all objects were serialized"
			buffer.remaining() == 76

		when: "an object is deserialized"
			RichObject newObj = codec.decoder(null).apply(buffer)

		then: "the object was deserialized"
			newObj.name == "first"
			newObj.percent == 0.5f
			newObj.total == 100l

	}

	static class RichObject {
		String name
		Float percent
		Long total

		RichObject() {
		}

		RichObject(String name, Float percent, Long total) {
			this.name = name
			this.percent = percent
			this.total = total
		}
	}

}

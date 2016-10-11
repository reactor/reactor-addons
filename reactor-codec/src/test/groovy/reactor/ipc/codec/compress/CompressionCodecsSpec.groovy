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
package reactor.ipc.codec.compress

import reactor.ipc.buffer.Buffer
import reactor.ipc.codec.compress.GzipCodec
import reactor.ipc.codec.compress.SnappyCodec
import spock.lang.Specification

import static reactor.ipc.codec.StandardCodecs.PASS_THROUGH_CODEC

/**
 * @author Jon Brisbin
 */
class CompressionCodecsSpec extends Specification {

	GzipCodec<Buffer, Buffer> gzip
	SnappyCodec<Buffer, Buffer> snappy

	def setup() {
		gzip = new GzipCodec<Buffer, Buffer>(PASS_THROUGH_CODEC)
		snappy = new SnappyCodec<Buffer, Buffer>(PASS_THROUGH_CODEC)
	}

	def "compression codecs preserve integrity of data"() {

		given:
			Buffer buffer

		when: "an object is encoded with GZIP"
			buffer = gzip.apply(Buffer.wrap("Hello World!"))

		then: "the Buffer was encoded and compressed"
			buffer.remaining() == 32

		when: "an object is decoded with GZIP"
			String hw = gzip.decoder(null).apply(buffer).asString()

		then: "the Buffer was decoded and uncompressed"
			hw == "Hello World!"

		when: "an object is encoded with Snappy"
			buffer = snappy.apply(Buffer.wrap("Hello World!"))

		then: "the Buffer was encoded and compressed"
			buffer.remaining() == 34

		when: "an object is decoded with Snappy"
			hw = snappy.decoder(null).apply(buffer).asString()

		then: "the Buffer was decoded and uncompressed"
			hw == "Hello World!"

	}

}

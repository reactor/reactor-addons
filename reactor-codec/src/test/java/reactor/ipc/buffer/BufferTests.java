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
package reactor.ipc.buffer;

import org.junit.Test;
import reactor.ipc.buffer.Buffer;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Sergey Shcherbakov
 */
public class BufferTests {
/*
	@Test
	@Ignore
	public void testAutoExpand() {

		int initial_small_size = Buffer.SMALL_IO_BUFFER_SIZE;
		int initial_max_size = Buffer.SMALL_IO_BUFFER_SIZE;
		try {
			Buffer b = new Buffer();
			Buffer.SMALL_IO_BUFFER_SIZE = 20;        // to speed up the test
			Buffer.SMALL_IO_BUFFER_SIZE = 100;
			for (int i = 0; i < Buffer.SMALL_IO_BUFFER_SIZE - Buffer.SMALL_IO_BUFFER_SIZE; i++) {
				b.append((byte) 0x1);
			}
		} finally {
			Buffer.SMALL_IO_BUFFER_SIZE = initial_small_size;
			Buffer.SMALL_IO_BUFFER_SIZE = initial_max_size;
		}
	}*/

	@Test
	public void testEquals() {
		Buffer buffer = Buffer.wrap("Hello");

		assertTrue(buffer.equals(Buffer.wrap("Hello")));
		assertFalse(buffer.equals(Buffer.wrap("Other")));
	}

}

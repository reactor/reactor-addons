/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.retry;

import java.util.Iterator;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RetryTestUtils {

	static void assertDelays(Queue<? extends Context<?>> retries, Long... delayMs) {
		assertEquals(delayMs.length, retries.size());
		int index = 0;
		for (Iterator<? extends Context<?>> it = retries.iterator(); it.hasNext(); ) {
			Context<?> repeatContext = it.next();
			assertEquals(delayMs[index].longValue(), repeatContext.backoff().toMillis());
			index++;
		}
	}

	static void assertRandomDelays(Queue<? extends Context<?>> retries, int firstMs, int maxMs) {
		long prevMs = 0;
		int randomValues = 0;
		for (Context<?> context : retries) {
			long backoffMs = context.backoff().toMillis();
			if (prevMs == 0)
				assertEquals(firstMs, backoffMs);
			else
				assertTrue("Unexpected delay " + backoffMs, backoffMs >= firstMs && backoffMs <= maxMs);
			if (backoffMs != firstMs && backoffMs != prevMs)
				randomValues++;
			prevMs = backoffMs;
		}
		assertTrue("Delays not random", randomValues >= 2); // Allow for at most one edge case.
	}
}

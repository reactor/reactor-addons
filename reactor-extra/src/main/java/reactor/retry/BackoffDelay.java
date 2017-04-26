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

import java.time.Duration;

public class BackoffDelay {

	static final BackoffDelay ZERO = new BackoffDelay(Duration.ZERO);

	final Duration min;
	final Duration max;
	final Duration delay;

	public BackoffDelay(Duration fixedBackoff) {
		this(fixedBackoff, fixedBackoff, fixedBackoff);
	}

	public BackoffDelay(Duration min, Duration max, Duration delay) {
		this.min = min;
		this.max = max;
		this.delay = delay;
	}

	public Duration minDelay() {
		return min;
	}

	public Duration maxDelay() {
		return max;
	}

	public Duration delay() {
		return delay;
	}

}

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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Context provided to repeat predicate {@link Repeat#onlyIf(java.util.function.Predicate)} and
 * the repeat callback {@link Repeat#doOnRepeat(java.util.function.Consumer)}.
 *
 * @param <T> Application context type
 */
public interface RepeatContext<T> extends Context<T> {

	/**
	 * Returns the value provided in the companion Flux for repeats.
	 * <ul>
	 *   <li>For {@link Flux#retryWhen(java.util.function.Function)} and {@link Mono#retryWhen(java.util.function.Function)},
	 *      value is set to null and the exception is returned by {@link #getException()}.</li>
	 *   <li>For {@link Flux#repeatWhen(java.util.function.Function)} and {@link Mono#repeatWhen(java.util.function.Function)},
	 *      value is the number of items emitted in the last attempt.
	 *   <li>For {@link Mono#repeatWhenEmpty(java.util.function.Function)} and {@link Mono#repeatWhenEmpty(int, java.util.function.Function)},
	 *      value is a zero-based incrementing Long, which is {@link #getAttempts()} - 1.
	 * </ul>
	 * @return value the value emitted on the companion Flux for repeats.
	 */
	public Long companionValue();
}

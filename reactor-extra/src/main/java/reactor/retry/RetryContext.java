/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.retry;

/**
 * Context provided to retry predicate {@link Retry#onlyIf(java.util.function.Predicate)} and
 * the retry callback {@link Retry#doOnRetry(java.util.function.Consumer)}.
 *
 * @param <T> Application context type
 * @deprecated To be removed in 3.6.0 at the earliest. Use equivalent features of reactor-core like
 * {@link reactor.util.retry.RetrySpec} and {@link reactor.util.retry.RetryBackoffSpec} instead.
 */
@Deprecated
public interface RetryContext<T> extends IterationContext<T> {

	/**
	 * Returns the exception from the last iteration.
	 * @return exception that resulted in retry
	 */
	public Throwable exception();
}

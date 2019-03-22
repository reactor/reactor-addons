/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.adapter.misc;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.TimedScheduler;
import reactor.test.StepVerifier;

/**
 * @author Stephane Maldini
 */
public class SwingAdapterTest {

	@Test
	public void normal() {
		TimedScheduler swingScheduler = SwingScheduler.create();

		Flux<Integer> swingFlux = Flux.range(0, 1_000_000)
		                              .publishOn(swingScheduler);

		StepVerifier.create(swingFlux)
		            .expectNextCount(1_000_000)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void timed() {
		TimedScheduler swingScheduler = SwingScheduler.create();

		Flux<Long> swingFlux = Flux.intervalMillis(100, swingScheduler)
		                           .take(3);

		StepVerifier.create(swingFlux)
		            .expectNext(0L, 1L, 2L)
		            .expectComplete()
		            .verify();
	}
}

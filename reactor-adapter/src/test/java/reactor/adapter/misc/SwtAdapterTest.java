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
package reactor.adapter.misc;

import org.eclipse.swt.widgets.Display;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.TimedScheduler;
import reactor.test.StepVerifier;

/**
 * @author Stephane Maldini
 */
public class SwtAdapterTest {

	@Test
	@Ignore("cannot test without display")
	public void normal() {
		TimedScheduler swtScheduler = SwtScheduler.from(new Display());

		Flux<Integer> swtFlux = Flux.range(0, 1_000_000)
		                              .publishOn(swtScheduler);

		StepVerifier.create(swtFlux)
		            .expectNextCount(1_000_000)
		            .expectComplete()
		            .verify();
	}
}

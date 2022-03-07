/*
 * Copyright (c) 2011-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.swing;

import org.eclipse.swt.widgets.Display;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

/**
 * @author Stephane Maldini
 * @deprecated To be removed exceptionally fast in 3.5.0. See https://github.com/reactor/reactor-addons/issues/273
 */
@Deprecated
public class SwtAdapterTest {

	@Test
	@Ignore("cannot test without display")
	public void normal() {
		Scheduler swtScheduler = SwtScheduler.from(new Display());

		Flux<Integer> swtFlux = Flux.range(0, 1_000_000)
		                              .publishOn(swtScheduler);

		StepVerifier.create(swtFlux)
		            .expectNextCount(1_000_000)
		            .expectComplete()
		            .verify();
	}
}

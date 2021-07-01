/*
 * Copyright (c) 2017 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.scheduler.clock;

import java.time.Duration;
import java.time.ZonedDateTime;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.scheduler.VirtualTimeScheduler;

public class SchedulerClockTest {

	@Test
	public void shouldReturnExpectedTime() {
		VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
		SchedulerClock clock = SchedulerClock.of(scheduler);
		ZonedDateTime beforeAdvance = ZonedDateTime.now(clock);
		Assert.assertEquals(0, clock.millis());
		Assert.assertEquals(0,
				clock.instant()
				     .toEpochMilli());

		scheduler.advanceTimeBy(Duration.ofSeconds(1));
		ZonedDateTime afterAdvance = ZonedDateTime.now(clock);

		Assert.assertTrue(beforeAdvance.isBefore(afterAdvance));
		Assert.assertEquals(1000, clock.millis());
		Assert.assertEquals(1000,
				clock.instant()
				     .toEpochMilli());
	}
}

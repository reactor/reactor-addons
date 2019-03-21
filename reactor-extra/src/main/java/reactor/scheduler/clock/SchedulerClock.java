/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.scheduler.clock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.NonNull;

/**
 * Clock adapter around {@link Scheduler}. That adoption gives better integration with
 * Java 8 DateTime API.
 * <p>
 * This feature may be useful for testing purpose, especially for integration with {@code
 * VirtualTimeScheduler}. For example:
 * <p>
 * <pre>
 *     <code>
 *         VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
 *         SchedulerClock clock = SchedulerClock.of(scheduler);
 *
 *         ZonedDateTime beforeAdvance = ZonedDateTime.now(clock);
 *         scheduler.advanceTimeBy(Duration.ofSeconds(1));
 *         ZonedDateTime afterAdvance = ZonedDateTime.now(clock);
 *
 *         Assert.assertTrue(beforeAdvance.isBefore(afterAdvance));
 *     </code>
 * </pre>
 * <p>
 *
 * @author Oleh Dokuka
 * @author Peter Royal
 * @since 3.1.4
 */
public class SchedulerClock extends Clock {

	/**
	 * Nanos per second.
	 */
	private static final long NANOS_PER_SECOND = 1000_000_000L;

	private final Scheduler scheduler;
	private final ZoneId    zone;

	private SchedulerClock(Scheduler scheduler, ZoneId zone) {
		this.scheduler = scheduler;
		this.zone = zone;
	}

	@Override
	@NonNull
	public ZoneId getZone() {
		return zone;
	}

	@Override
	@NonNull
	public SchedulerClock withZone(ZoneId zone) {
		return new SchedulerClock(scheduler, zone);
	}

	/**
	 * Return wrapped Scheduler instance
	 *
	 * @return {@link Scheduler} instance
	 */
	@NonNull
	public Scheduler getScheduler() {
		return scheduler;
	}

	/**
	 * Builder method that returns new instance of {@link SchedulerClock} which is
	 * constructed from old {@link ZoneId} and passed {@link Scheduler} instance
	 *
	 * @return {@link SchedulerClock} instance
	 */
	@NonNull
	public SchedulerClock withScheduler(Scheduler scheduler) {
		return new SchedulerClock(scheduler, zone);
	}

	@Override
	public long millis() {
		return scheduler.now(TimeUnit.MILLISECONDS);
	}

	/**
	 * Gets the current instant of the clock.
	 * <p>
	 * This returns an instant representing the current instant as defined by the clock.
	 * <p>
	 * Note: If scheduler does not support time in nanoseconds the returned {@link
	 * Instant} will be limited by milliseconds
	 *
	 * @return the current instant from this clock, not null
	 */
	@Override
	@NonNull
	public Instant instant() {
		long nano = scheduler.now(TimeUnit.NANOSECONDS);
		long secs = Math.floorDiv(nano, NANOS_PER_SECOND);
		long nos = Math.floorMod(nano, NANOS_PER_SECOND);

		return Instant.ofEpochSecond(secs, nos);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		SchedulerClock that = (SchedulerClock) o;

		return scheduler.equals(that.scheduler) && zone.equals(that.zone);

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + scheduler.hashCode();
		result = 31 * result + zone.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "SchedulerClock{" + "scheduler=" + scheduler + ", zone=" + zone + '}';
	}

	/**
	 * Create instance of {@link SchedulerClock} from given {@link Scheduler} and system
	 * default {@link ZoneId#systemDefault()}
	 *
	 * @param scheduler {@link Scheduler} instance
	 *
	 * @return new {@link SchedulerClock}
	 */
	@NonNull
	public static SchedulerClock of(@NonNull Scheduler scheduler) {
		return new SchedulerClock(scheduler, ZoneId.systemDefault());
	}

	/**
	 * Create instance of {@link SchedulerClock} from given {@link Scheduler} and {@link
	 * ZoneId}
	 *
	 * @param scheduler {@link Scheduler} instance
	 *
	 * @return new {@link SchedulerClock}
	 */
	@NonNull
	public static SchedulerClock of(@NonNull Scheduler scheduler,
			@NonNull ZoneId zoneId) {
		return new SchedulerClock(scheduler, zoneId);
	}
}

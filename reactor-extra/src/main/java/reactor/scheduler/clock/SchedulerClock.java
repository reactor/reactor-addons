package reactor.scheduler.clock;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.NonNull;

public class SchedulerClock extends Clock {

	private final Scheduler scheduler;
	private final ZoneId    zone;

	private SchedulerClock(Scheduler scheduler, ZoneId zone) {
		this.scheduler = scheduler;
		this.zone = zone;
	}

	@Override
	public ZoneId getZone() {
		return zone;
	}

	@Override
	public SchedulerClock withZone(ZoneId zone) {
		return new SchedulerClock(scheduler, zone);
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	public SchedulerClock withScheduler(Scheduler scheduler) {
		return new SchedulerClock(scheduler, zone);
	}

	@Override
	public long millis() {
		return scheduler.now(TimeUnit.MILLISECONDS);
	}

	@Override
	public Instant instant() {
		return Instant.ofEpochMilli(millis());
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

	public static SchedulerClock of(@NonNull Scheduler scheduler) {
		return new SchedulerClock(scheduler, ZoneId.systemDefault());
	}

	public static SchedulerClock of(@NonNull Scheduler scheduler,
			@NonNull ZoneId zoneId) {
		return new SchedulerClock(scheduler, zoneId);
	}
}

package reactor.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Randomized Jitter with a factor, that works on BackoffDelay with min <= d <= max and
 * maintain that invariant on the randomized {@literal d}.
 *
 * @author Simon Baslé
 */
class RandomJitter implements Jitter {

	private final double randomFactor;

	public RandomJitter(double randomFactor) {
		if (randomFactor < 0 || randomFactor > 1) throw new IllegalArgumentException("random factor must be between 0 and 1 (default 0.5)");
		this.randomFactor = randomFactor;
	}

	@Override
	public Duration apply(BackoffDelay backoff) {
		//check the invariant
		if (backoff.delay.compareTo(backoff.min) < 0) {
			throw new IllegalArgumentException("jitter can only be applied on a delay that is >= to min backoff");
		}
		if (backoff.delay.compareTo(backoff.max) > 0) {
			throw new IllegalArgumentException("jitter can only be applied on a delay that is <= to max backoff");
		}

		//short-circuit delay == 0 case
		if (backoff.delay.isZero()) {
			return backoff.delay;
		}

		ThreadLocalRandom random = ThreadLocalRandom.current();

		long jitterOffset = jitterOffsetCapped(backoff);
		long lowBound = lowJitterBound(backoff, jitterOffset);
		long highBound = highJitterBound(backoff, jitterOffset);

		long jitter;
		if (highBound == lowBound) {
			if (highBound == 0) jitter = 0;
			else jitter = random.nextLong(highBound);
		}
		else {
			jitter = random.nextLong(lowBound, highBound);
		}
		return backoff.delay.plusMillis(jitter);
	}

	/**
	 * Compute the jitter offset that will be used for the bounds of the random jitter,
	 * in a way that is safe for large {@link Duration} that go over Long.MAX_VALUE ms.
	 */
	long jitterOffsetCapped(BackoffDelay backoff) {
		try {
			return backoff.delay.multipliedBy((long) (100 * randomFactor))
			             .dividedBy(100)
			             .toMillis();
		}
		catch (ArithmeticException ae) {
			return Math.round(Long.MAX_VALUE * randomFactor);
		}
	}

	/**
	 * Compute a lower bound for the random jitter that won't let the final delay go
	 * below {@link BackoffDelay#minDelay()}.
	 *
	 * @param backoff the original backoff constraints to work with
	 * @param jitterOffset the jitter offset
	 * @return a lower bound for the random generation function, so that delay + jitter &gt;= min
	 */
	long lowJitterBound(BackoffDelay backoff, long jitterOffset) {
		return Math.max(
				backoff.min.minus(backoff.delay).toMillis(),
				-jitterOffset);
	}

	/**
	 * Compute a higher bound for the random jitter that won't let the final delay go
	 * over {@link BackoffDelay#maxDelay()}.
	 *
	 * @param backoff the original backoff constraints to work with
	 * @param jitterOffset the jitter offset
	 * @return a higher bound for the random generation function, so that delay + jitter &lt;= max
	 */
	long highJitterBound(BackoffDelay backoff, long jitterOffset) {
		return Math.min(
				backoff.max.minus(backoff.delay).toMillis(),
				jitterOffset);
	}

	@Override
	public String toString() {
		return "Jitter{RANDOM-" + randomFactor + "}";
	}
}

package reactor.ratelimiter;

/**
 * @author Simon Baslé
 */
public class RateLimitedException extends RuntimeException {

	public RateLimitedException() {
		super("Rate limited");
	}

	public RateLimitedException(String message) {
		super(message);
	}
}

package reactor.ratelimiter;

import reactor.core.Disposable;

/**
 * @author Simon Baslé
 */
interface RateLimiterSource {

	boolean permitTry();
	void waitForPermit(PermitConsumer permitConsumer);
	void cancelForPermit(PermitConsumer permitConsumer);
	int permits();

}

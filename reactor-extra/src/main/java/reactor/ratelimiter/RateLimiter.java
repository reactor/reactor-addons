package reactor.ratelimiter;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * @author Simon Baslé
 */
public interface RateLimiter extends Disposable {

	Mono<Void> acquireOrReject();

	Mono<Void> acquireOrWait();

	Mono<Boolean> tryAcquire();

}

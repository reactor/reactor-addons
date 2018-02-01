package reactor.ratelimiter;

import reactor.core.Disposable;

/**
 * @author Simon Baslé
 */
interface PermitConsumer extends Disposable {

	void permitAcquired();

	void permitRejected(Throwable cause);

}

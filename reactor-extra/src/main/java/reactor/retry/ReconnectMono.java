package reactor.retry;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public abstract class ReconnectMono<T> extends Mono<T> implements Disposable {

}

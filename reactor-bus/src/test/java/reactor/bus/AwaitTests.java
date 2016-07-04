/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.bus;

import java.util.function.Consumer;

import org.junit.Test;
import reactor.bus.selector.Selectors;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.TopicProcessor;
import reactor.core.publisher.WorkQueueProcessor;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class AwaitTests extends AbstractReactorTest {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		EventBus innerReactor = EventBus.config().concurrency(4).processor(WorkQueueProcessor.create("simple-work",
		  64)).get();

		for (int i = 0; i < 10000; i++) {
			final MonoProcessor<String> deferred = MonoProcessor.create();

			innerReactor.schedule((Consumer) t -> deferred.onNext("foo"), null);


			String latchRes = deferred.block(5000);
			assertThat("latch is not counted down", "foo".equals(latchRes));
		}
	}


	@Test
	public void testDoesntDeadlockOnError() throws InterruptedException {

		EventBus r = EventBus.create(TopicProcessor.create("rb", 8));

		EmitterProcessor<Event<Throwable>> stream = EmitterProcessor.create();
		stream.connect();

		Mono<Long> promise = stream.log().take(16).count().subscribe();
		r.on(Selectors.T(Throwable.class), stream::onNext);
		r.on(Selectors.$("test"), (Event<?> ev) -> {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				//IGNORE
			}
			throw new RuntimeException();
		});

		for (int i = 0; i < 16; i++) {
			r.notify("test", Event.wrap("test"));
		}

		assert promise.block(5000) == 16;
		try{
			r.getProcessor().onComplete();
		}catch(Throwable c){
			c.printStackTrace();
		}

	}

}

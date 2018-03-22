/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;
import static reactor.core.Fuseable.QueueSubscription;

public class FileWriterMono extends Mono<ByteBuffer> {

	final Path                            path;
	final int                             prefetch;
	final Scheduler                       scheduler;
	final Publisher<? extends ByteBuffer> parent;

	public FileWriterMono(Publisher<? extends ByteBuffer> parent,
			Path path,
			int prefetch,
			Scheduler scheduler) {

		this.path = path;
		this.prefetch = prefetch;
		this.scheduler = scheduler;
		this.parent = parent;
	}

	@Override
	public void subscribe(CoreSubscriber<? super ByteBuffer> actual) {

	}

	static final class FileWriterSubscriber
			implements CoreSubscriber<ByteBuffer>, Scannable, Subscription, Runnable {

		final CoreSubscriber<? super ByteBuffer> actual;
		final int                                prefetch;
		final Scheduler.Worker                   worker;
		final FileChannel                        channel;

		volatile Queue<ByteBuffer> queue;

		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<FileWriterMono.FileWriterSubscriber, Subscription> SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(FileWriterMono.FileWriterSubscriber.class, Subscription.class, "subscription");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FileWriterMono.FileWriterSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(FileWriterMono.FileWriterSubscriber.class, "wip");

		volatile int terminated;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<FileWriterMono.FileWriterSubscriber> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(FileWriterMono.FileWriterSubscriber.class, "terminated");

		int fusionMode;

		public FileWriterSubscriber(CoreSubscriber<? super ByteBuffer> actual,
				Path path,
				int prefetch,
				Scheduler.Worker worker) throws IOException {
			this.actual = actual;
			this.prefetch = prefetch;
			this.worker = worker;
			this.channel = FileChannel.open(path);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUBSCRIPTION, this, s)) {
				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked")
					QueueSubscription<ByteBuffer> qs = (QueueSubscription<ByteBuffer>) s;
					int m = qs.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

					if (m == Fuseable.SYNC) {
						fusionMode = m;
						queue = qs;
						trySchedule();
						return;
					}
					if (m == Fuseable.ASYNC) {
						fusionMode = m;
						queue = qs;
						s.request(prefetch == Integer.MAX_VALUE ? Long.MAX_VALUE :
								prefetch);
						return;
					}
				}

				queue = Queues.<ByteBuffer>get(prefetch).get();
				s.request(prefetch == Integer.MAX_VALUE ? Long.MAX_VALUE : prefetch);
			}
		}

		@Override
		public void onNext(ByteBuffer buffer) {

		}

		@Override
		public void onError(Throwable throwable) {
			try {
				if (channel.isOpen()) {
					channel.close();
				}
			}
			catch (IOException t) {
				Operators.onErrorDropped(t, currentContext());
			}
			finally {
				if (TERMINATED.compareAndSet(this, 0, 1)) {
//					a.onError(e);
					worker.dispose();
				}
			}
		}

		@Override
		public void onComplete() {

		}

		@Override
		public void request(long l) {

		}

		@Override
		public void cancel() {

		}

		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}

		@Override
		public void run() {

		}

		void trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
//				doError(actual, Operators.onRejectedExecution(ree, actual.currentContext()));
			}
		}
	}
}

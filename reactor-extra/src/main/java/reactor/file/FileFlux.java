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

import java.nio.ByteBuffer;
import java.nio.file.Path;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class FileFlux extends Flux<ByteBuffer> {

	static final int DEFAULT_BUFFER_CAPACITY = 1024;

	public static FileFlux from(Path path) {
		return new FileChannelReaderFlux(path, DEFAULT_BUFFER_CAPACITY, Schedulers.parallel());
	}

	public static FileFlux from(Path path, int bufferCapacity, Scheduler scheduler) {
		return new FileChannelReaderFlux(path, bufferCapacity, scheduler);
	}

	public Flux<ByteBuffer> lines() {
		return null;
	}

	public Mono<Void> save(Path path) {
return null;
	}
}

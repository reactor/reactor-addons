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

package reactor.ipc.codec;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Receiver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.ipc.buffer.Buffer;

/**
 * Implementations of a {@literal Codec} are responsible for decoding a {@code SRC} into an
 * instance of {@code IN} and passing that to the given {@link java.util.function.Consumer}. A
 * codec also provides an encoder to take an instance of {@code OUT} and encode to an
 * instance of {@code SRC}.
 *
 * @param <SRC> The type that the codec decodes from and encodes to
 * @param <IN>  The type produced by decoding
 * @param <OUT> The type consumed by encoding
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class Codec<SRC, IN, OUT> implements Function<OUT, SRC> {

	static public final byte DEFAULT_DELIMITER = (byte) '\0';

	static final Supplier<?> NO_CONTEXT = new Supplier<Object>() {
		@Override
		public Object get() {
			return null;
		}
	};

	protected final Byte delimiter;

	protected final Supplier<?> decoderContextProvider;

	/**
	 * Create a new Codec set with a \0 delimiter to finish any Buffer encoded value or scan for delimited decoded
	 * Buffers.
	 */
	protected Codec() {
		this(DEFAULT_DELIMITER);
	}

	/**
	 * A delimiter can be used to trail any decoded buffer or to finalize encoding from any incoming value
	 *
	 * @param delimiter delimiter can be left undefined (null) to bypass appending at encode time and scanning at
	 *                     decode
	 *                  time.
	 */
	protected Codec(Byte delimiter) {
		this(delimiter, NO_CONTEXT);
	}
	/**
	 * A delimiter can be used to trail any decoded buffer or to finalize encoding from any incoming value
	 *
	 * @param delimiter delimiter can be left undefined (null) to bypass appending at encode time and scanning at
	 *                     decode
	 *                  time.
	 */
	protected Codec(Byte delimiter, Supplier<?> decoderContextProvider) {
		this.delimiter = delimiter;
		this.decoderContextProvider = decoderContextProvider;
	}

	/**
	 * Provide the caller with a decoder to turn a source object into an instance of the input
	 * type.
	 *
	 * @return The decoded object.
	 * @since 2.0.4
	 */
	public Flux<IN> decode(final Publisher<SRC> publisherToDecode) {
		return new DecoderOperator(publisherToDecode);
	}

	/**
	 * Provide the caller with a decoder to turn a source object into an instance of the input
	 * type.
	 *
	 * @return The decoded object.
	 */
	public Function<SRC, IN> decoder() {
		return decoder(null);
	}

	/**
	 * Provide the caller with a decoder to turn a source object into an instance of the input
	 * type.
	 *
	 * @param next The {@link Consumer} to call after the object has been decoded.
	 * @return The decoded object.
	 */
	public Function<SRC, IN> decoder(Consumer<IN> next){
		return decoder(next, decoderContextProvider.get());
	}

	protected <C> Function<SRC, IN> decoder(Consumer<IN> next, C context){
		return new DefaultInvokeOrReturnFunction<>(next, context);
	}

	/**
	 * Provide the caller with an encoder to turn an output sequence into an sequence of the source
	 * type.
	 *
	 * @return The encoded source sequence.
	 * @since 2.0.4
	 */
	@SuppressWarnings("unchecked")
	public Flux<SRC> encode(Publisher<? extends OUT> publisherToEncode) {
		return new EncoderOperator(publisherToEncode);
	}

	/**
	 * Provide the caller with an encoder to turn an output object into an instance of the source
	 * type.
	 *
	 * @return The encoded source object.
	 */
	public Function<OUT, SRC> encoder() {
		return this;
	}


	protected static <IN> IN invokeCallbackOrReturn(Consumer<IN> consumer, IN v) {
		if (consumer != null) {
			consumer.accept(v);
			return null;
		} else {
			return v;
		}
	}

	/**
	 * Decode a buffer
	 *
	 * @param buffer
	 * @return
	 */
	public final IN decodeNext(SRC buffer) {
		return decodeNext(buffer, decoderContextProvider.get());
	}

	/**
	 *
	 * @param buffer
	 * @param context
	 * @return
	 */
	protected abstract IN decodeNext(SRC buffer, Object context);

	/**
	 * Add a trailing delimiter if defined
	 *
	 * @param buffer the buffer to prepend to this codec delimiter if any
	 * @return the positioned and expanded buffer reference if any delimiter, otherwise the passed buffer
	 */
	protected Buffer addDelimiterIfAny(Buffer buffer) {
		if (delimiter != null) {
			return buffer.append(delimiter).flip();
		} else {
			return buffer;
		}
	}

	private final class DecoderBarrier extends Operators.SubscriberAdapter<SRC, IN> {
		final Function<SRC, IN> decoder;

		public DecoderBarrier(final Subscriber<? super IN> subscriber) {
			super(subscriber);
			decoder = decoder(new Consumer<IN>() {
				@Override
				public void accept(IN in) {
					subscriber.onNext(in);
				}
			});
		}

		@Override
		protected void doNext(SRC src) {
			decoder.apply(src);
		}

		@Override
		public String toString() {
			return Codec.this.getClass().getSimpleName().replaceAll("Codec","Decoder");
		}
	}

	private class EncoderBarrier extends Operators.SubscriberAdapter<OUT, SRC> {

		final private Function<OUT, SRC> encoder;

		public EncoderBarrier(final Subscriber<? super SRC> subscriber) {
			super(subscriber);
			encoder = encoder();
		}

		@Override
		protected void doNext(OUT src) {
			subscriber.onNext(encoder.apply(src));
		}

		@Override
		public String toString() {
			return Codec.this.getClass()
			                 .getSimpleName()
			                 .replaceAll("Codec", "Encoder");
		}
	}

	protected class DefaultInvokeOrReturnFunction<C> implements Function<SRC, IN> {
		protected final Consumer<IN> consumer;
		protected final C context;

		public DefaultInvokeOrReturnFunction(Consumer<IN> consumer) {
			this(consumer, null);
		}
		public DefaultInvokeOrReturnFunction(Consumer<IN> consumer, C context) {
			this.consumer = consumer;
			this.context = context;
		}

		@Override
		public IN apply(SRC buffer) {
			return invokeCallbackOrReturn(consumer, decodeNext(buffer, context));
		}
	}

	protected abstract static class CodecSource<I, O> extends Flux<O> implements Receiver {
		protected final Publisher<? extends I> source;

		public CodecSource(Publisher<? extends I> source) {
			this.source = source;
		}

		@Override
		public String toString() {
			return "{" +
					" codec : \"" + super.toString() + "\" " +
					'}';
		}

		@Override
		public final Publisher<? extends I> upstream() {
			return source;
		}
	}

	private class DecoderOperator extends CodecSource<SRC, IN> {

		public DecoderOperator(Publisher<? extends SRC> source) {
			super(source);
		}

		@Override
		public void subscribe(Subscriber<? super IN> s) {
			source.subscribe(new DecoderBarrier(s));
		}
	}

	private class EncoderOperator extends CodecSource<OUT, SRC> {

		public EncoderOperator(Publisher<? extends OUT> source) {
			super(source);
		}

		@Override
		public void subscribe(Subscriber<? super SRC> s) {
			source.subscribe(new EncoderBarrier(s));
		}
	}
}

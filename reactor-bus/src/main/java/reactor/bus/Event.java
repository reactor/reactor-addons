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

import reactor.core.tuple.Tuple;
import reactor.core.tuple.Tuple2;
import reactor.core.util.UUIDUtils;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Wrapper for an object that needs to be processed by {@link java.util.function.Consumer}s.
 *
 * @param <T> The type of the wrapped object
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Event<T> implements Serializable {

	public static Event<Void> VOID = new Event<Void>(null);

	private static final long serialVersionUID = -2476263092040373361L;
	private final Consumer<Throwable> errorConsumer;
	private final Headers             headers;
	private final T                   data;

	/**
	 * Creates a new Event with the given {@code key} and {@code data}.
	 *
	 * @param data The data
	 */
	public Event(T data) {
		this(data, null, null);
	}

	/**
	 * Creates a new Event with the given {@code key}, {@code data}, {@code headers} and {@core errorConsumer}.
	 *
	 * @param data          The data
	 * @param headers       The headers
	 * @param errorConsumer error consumer callback
	 */
	public Event(T data, Headers headers, Consumer<Throwable> errorConsumer) {
		this.data = data;
		this.headers = headers;
		this.errorConsumer = errorConsumer;
	}

	/**
	 * Get the {@link Headers} attached to this event.
	 *
	 * @return The Event's Headers
	 */
	public Headers getHeaders() {
		return headers;
	}

	/**
	 * Get the internal data being wrapped.
	 *
	 * @return The data.
	 */
	public T getData() {
		return data;
	}

	/**
	 * Get the internal error consumer callback being wrapped.
	 *
	 * @return the consumer.
	 */
	public Consumer<Throwable> getErrorConsumer() {
		return errorConsumer;
	}

	/**
	 * Create a copy of this event, reusing same headers, data and replyTo
	 *
	 * @return {@literal event copy}
	 */
	public Event<T> copy() {
		return copy(data);
	}

	/**
	 * Create a copy of this event, reusing same headers and replyTo
	 *
	 * @return {@literal event copy}
	 */
	public <E> Event<E> copy(E data) {
		return new Event<E>(data, headers, errorConsumer);
	}

	/**
	 * Consumes error, using a producer defined callback
	 *
	 * @param throwable The error to consume
	 */
	public void consumeError(Throwable throwable) {
		if (null != errorConsumer) {
			errorConsumer.accept(throwable);
		}
	}

	public static <T> Event<T> wrap(T data) {
		return new Event<T>(data);
	}

	@Override
	public String toString() {
		return "Event{" +
			   ", headers=" + headers +
			   ", data=" + data +
			   '}';
	}

	/**
	 * Headers are a Map-like structure of name-value pairs. Header names are case-insensitive, as determined by {@link
	 * String#CASE_INSENSITIVE_ORDER}. A header can be removed by setting its value to {@code null}.
	 */
	public static class Headers implements Serializable, Iterable<Tuple2<String, Object>> {

		/**
		 * The name of the origin header
		 *
		 * @see #setOrigin(String)
		 * @see #setOrigin(UUID)
		 * @see #getOrigin()
		 */
		public static final String ORIGIN = "x-reactor-origin";

		private static final long serialVersionUID = 4984692586458514948L;

		private final Object monitor = UUIDUtils.create();
		private final Map<String, Object> headers;

		private Headers(boolean sealed, Map<String, Object> headers) {
			Map<String, Object> copy = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
			copyHeaders(headers, copy);
			if (sealed) {
				this.headers = Collections.unmodifiableMap(copy);
			} else {
				this.headers = copy;
			}
		}

		/**
		 * Creates a new Headers instance by copying the contents of the given {@code headers} Map. Note that, as the
		 * map is
		 * copied, subsequent changes to its contents will have no effect upon the Headers.
		 *
		 * @param headers The map to copy.
		 */
		public Headers(Map<String, Object> headers) {
			this(false, headers);
		}

		/**
		 * Create an empty Headers
		 */
		public Headers() {
			this(false, null);
		}

		/**
		 * Sets all of the headers represented by entries in the given {@code headers} Map. Any entry with a null
		 * value will
		 * cause the header matching the entry's name to be removed.
		 *
		 * @param headers The map of headers to set.
		 * @return {@code this}
		 */
		public Headers setAll(Map<String, Object> headers) {
			if (null == headers || headers.isEmpty()) {
				return this;
			} else {
				synchronized (this.monitor) {
					copyHeaders(headers, this.headers);
				}
			}
			return this;
		}

		/**
		 * Set the header value. If {@code value} is {@code null} the header with the given {@code name} will be
		 * removed.
		 *
		 * @param name  The name of the header.
		 * @param value The header's value.
		 * @return {@code this}
		 */
		public <V> Headers set(String name, V value) {
			synchronized (this.monitor) {
				setHeader(name, value, headers);
			}
			return this;
		}

		/**
		 * Set the origin header. The origin is simply a unique id to indicate to consumers where it should send
		 * replies. If
		 * {@code id} is {@code null} the origin header will be removed.
		 *
		 * @param id The id of the origin component.
		 * @return {@code this}
		 */
		public Headers setOrigin(UUID id) {
			String idString = id == null ? null : id.toString();
			return setOrigin(idString);
		}

		/**
		 * Get the origin header
		 *
		 * @return The origin header, may be {@code null}.
		 */
		public String getOrigin() {
			synchronized (this.monitor) {
				return (String) headers.get(ORIGIN);
			}
		}

		/**
		 * Set the origin header. The origin is simply a unique id to indicate to consumers where it should send
		 * replies. If
		 * {@code id} is {@code null} this origin header will be removed.
		 *
		 * @param id The id of the origin component.
		 * @return {@code this}
		 */
		public Headers setOrigin(String id) {
			synchronized (this.monitor) {
				setHeader(ORIGIN, id, headers);
			}
			return this;
		}

		/**
		 * Get the value for the given header.
		 *
		 * @param name The header name.
		 * @return The value of the header, or {@code null} if none exists.
		 */
		@SuppressWarnings("unchecked")
		public <V> V get(String name) {
			synchronized (monitor) {
				return (V) headers.get(name);
			}
		}

		/**
		 * Determine whether the headers contain a value for the given name.
		 *
		 * @param name The header name.
		 * @return {@code true} if a value exists, {@code false} otherwise.
		 */
		public boolean contains(String name) {
			synchronized (monitor) {
				return headers.containsKey(name);
			}
		}

		/**
		 * Get these headers as an unmodifiable {@link Map}.
		 *
		 * @return The unmodifiable header map
		 */
		public Map<String, Object> asMap() {
			synchronized (monitor) {
				return Collections.unmodifiableMap(headers);
			}
		}

		/**
		 * Get the headers as a read-only version
		 *
		 * @return A read-only version of the headers.
		 */
		public Headers readOnly() {
			synchronized (monitor) {
				return new Headers(true, headers);
			}
		}

		/**
		 * Returns an unmodifiable Iterator over a copy of this Headers' contents.
		 */
		@Override
		public Iterator<Tuple2<String, Object>> iterator() {
			synchronized (this.monitor) {
				List<Tuple2<String, Object>> headers = new ArrayList<Tuple2<String, Object>>(this.headers.size());
				for (Map.Entry<String, Object> header : this.headers.entrySet()) {
					headers.add(Tuple.of(header.getKey(), header.getValue()));
				}
				return Collections.unmodifiableList(headers).iterator();
			}
		}

		@Override
		public String toString() {
			return headers.toString();
		}

		private void copyHeaders(Map<String, Object> source, Map<String, Object> target) {
			if (source != null) {
				for (Map.Entry<String, Object> entry : source.entrySet()) {
					setHeader(entry.getKey(), entry.getValue(), target);
				}
			}
		}

		private void setHeader(String name, Object value, Map<String, Object> target) {
			if (value == null) {
				target.remove(name);
			} else {
				target.put(name, value);
			}
		}
	}

}

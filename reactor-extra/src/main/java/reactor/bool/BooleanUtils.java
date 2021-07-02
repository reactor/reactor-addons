/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.bool;

import reactor.core.publisher.Mono;

/**
 * Functions to help compose results with {@link Mono}-wrapped {@literal boolean} values.
 *
 * @author Greg Turnquist
 */
public final class BooleanUtils {

	private BooleanUtils() {
	}

	/**
	 * A boolean, wrapped in a {@link Mono}, inverted by the NOT operator.
	 *
	 * @param value boolean
	 * @return {@literal !value} wrapped in a {@link Mono}
	 */
	public static Mono<Boolean> not(Mono<Boolean> value) {
		return value.map(bool -> !bool);
	}

	/**
	 * Two booleans, wrapped in {@link Mono}, combined with the AND operator.
	 *
	 * @param value1 first boolean
	 * @param value2 second boolean
	 * @return {@literal value1 && value2} wrapped in a {@link Mono}
	 */
	public static Mono<Boolean> and(Mono<Boolean> value1, Mono<Boolean> value2) {
		return Mono.zip(value1, value2, (bool1, bool2) -> bool1 && bool2);
	}


	/**
	 * Two booleans, wrapped in {@link Mono}, combined with the exclusive-OR operator.
	 *
	 * @param value1 first boolean
	 * @param value2 second boolean
	 * @return {@literal value1 XOR value2} wrapped in a {@link Mono}
	 */
	public static Mono<Boolean> xor(Mono<Boolean> value1, Mono<Boolean> value2) {
		return Mono.zip(value1, value2, (bool1, bool2) -> (bool1 != bool2) && (bool1 || bool2));
	}
	
	/**
	 * Two booleans, wrapped in {@link Mono}, combined with the OR operator.
	 *
	 * @param value1 first boolean
	 * @param value2 second boolean
	 * @return {@literal value1 || value2} wrapped in a {@link Mono}
	 */
	public static Mono<Boolean> or(Mono<Boolean> value1, Mono<Boolean> value2) {
		return Mono.zip(value1, value2, (bool1, bool2) -> bool1 || bool2);
	}

	/**
	 * Two booleans, wrapped in {@link Mono}, combined with the NOT-AND (NAND) operator.
	 *
	 * @param value1 first boolean
	 * @param value2 second boolean
	 * @return {@literal !(value1 && value2)} wrapped in a {@link Mono}
	 */
	public static Mono<Boolean> nand(Mono<Boolean> value1, Mono<Boolean> value2) {
		return not(and(value1, value2));
	}

	/**
	 * Two booleans, wrapped in {@link Mono}, combined with the NOT-OR (NOR) operator.
	 *
	 * @param value1 first boolean
	 * @param value2 second boolean
	 * @return {@literal !(value1 || value2)} wrapped in a {@link Mono}
	 */
	public static Mono<Boolean> nor(Mono<Boolean> value1, Mono<Boolean> value2) {
		return not(or(value1, value2));
	}
}

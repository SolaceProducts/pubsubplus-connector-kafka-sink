package com.solace.connector.kafka.connect.sink.it.util;

import java.util.function.Function;

public interface ThrowingFunction<T, R> extends Function<T, R> {
	@Override
	default R apply(T t) {
		try {
			return acceptThrows(t);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	R acceptThrows(T t) throws Exception;
}

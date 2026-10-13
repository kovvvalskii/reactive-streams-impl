package de.kovalski.reactivestreams;

import java.util.function.Function;

import org.reactivestreams.Publisher;

public interface Flow<T> extends Publisher<T> {

    static <K> Flow<K> fromArray(K... arr) {
        return new ArrayPublisher<>(arr);
    }

    default <K> Flow<K> map(Function<? super T, ? extends K> mapper) {
        return new MapOperator.MapPublisher<>(mapper, this); 
    }
}

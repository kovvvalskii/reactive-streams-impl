package de.kovalski.reactivestreams;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;


public class ArrayPublisher<T> implements Flow<T> {

    private T[] data;

    public ArrayPublisher(T[] data) {
        this.data = data;
    } 

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        subscriber.onSubscribe(new Subscription() {

            int index = 0;
            AtomicLong requested = new AtomicLong();
            AtomicBoolean cancelled = new AtomicBoolean(false);
            boolean completed = false;

            @Override
            public void request(long n) {

                if (n <= 0 && !cancelled.get()) {
                    cancel();
                    subscriber.onError(new IllegalArgumentException("3.9 violated"));
                    return;
                }

                // --------- Work in progress pattern ------------
                long initialRequested;
                do {
                    initialRequested = requested.get();
                    if (initialRequested == Long.MAX_VALUE) {
                        return;
                    }

                    n = initialRequested + n;
                    if (n <= 0) {
                        n = Long.MAX_VALUE;
                    }
                } while (!requested.compareAndSet(initialRequested, n));

                if (initialRequested != 0) {
                    return;
                }

                // -----------------------------------------------

                int sent = 0;
                while (true) {
                    for (; sent < requested.get() && index < data.length; sent++, index++) {
                        if(cancelled.get()) {
                            return;
                        }
                        T element = data[index];
                        if (element == null) {
                            subscriber.onError(new NullPointerException(String.format("Element %d is null", index)));
                            return;
                        }
                        subscriber.onNext(element);
                    }

                    if (cancelled.get()) {
                        return;
                    }
                    if (index == data.length && !completed) {
                        subscriber.onComplete();
                        completed = true;
                        return;
                    }

                    long currentRequested = requested.addAndGet(-sent);
                    if (currentRequested == 0) {
                        return;
                    }
                    sent = 0;
                }
            }

            @Override
            public void cancel() {
                this.cancelled.set(true);;
            }
            
        });
    }
    
}

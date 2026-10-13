package de.kovalski.reactivestreams;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class MapOperator {
    static class MapPublisher<IN, OUT> implements Flow<OUT> {

        final Function<? super IN, ? extends OUT> mapper;
        final Publisher<? extends IN> source;

        public MapPublisher(Function<? super IN, ? extends OUT> mapper, Publisher< ? extends IN> source) {
            this.mapper = mapper;
            this.source = source;
        }

        @Override
        public void subscribe(Subscriber<? super OUT> s) {
            source.subscribe(new MapSubscriber<>(mapper, s));
        }

    }

    static class MapSubscriber<IN, OUT> implements 
        Subscriber<IN>, 
        Subscription // For performance stuff (creating less objects) and also for subscription control
    {

        final Function<? super IN, ? extends OUT> mapper;
        final Subscriber<? super OUT> next;

        boolean terminated;

        Subscription upstreamSubscription;


        public MapSubscriber(Function<? super IN, ? extends OUT> mapper, Subscriber<? super OUT> next) {
            this.mapper = mapper;
            this.next = next;
        }

        @Override
        public void onSubscribe(Subscription s) {
            upstreamSubscription = s;
            next.onSubscribe(this);
        }

        @Override
        public void onNext(IN t) {
            if (terminated) {
                return;
            }

            OUT element;
            try {
                element = mapper.apply(t);   
            }
            catch (Throwable e) {
                terminated = true;
                cancel();
                next.onError(e);
                return;
            }
            next.onNext(element);
        }

        @Override
        public void onError(Throwable t) {
            if (terminated) {
                return;
            }
            terminated = true;
            next.onError(t);
        }

        @Override
        public void onComplete() {
            terminated = true;
            next.onComplete();
        }

        // Come from subscription
        @Override
        public void request(long n) {
            upstreamSubscription.request(n);
        }

        @Override
        public void cancel() {
            upstreamSubscription.cancel();
        }

    }

}

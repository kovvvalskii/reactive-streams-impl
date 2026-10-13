package de.kovalski.reactivestreams;


import org.assertj.core.api.Assertions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import de.kovalski.reactivestreams.MapOperator.MapPublisher;


public class MapOperatorVerification extends PublisherVerification<Long> {

    public MapOperatorVerification() {
        super(new TestEnvironment());
    }

    @Override
    public Publisher<Long> createPublisher(long elements) {
        return Flow.fromArray(ArrayPublisherTest.generate(elements))
            .map(l -> l.toString())
            .map(s -> Long.parseLong(s));
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        return null;
    }

    @Test 
    public void shouldWorkCorrectlyInCaseOfErrorInMapFunc() {
        ArrayPublisher<Long> arrayPublisher = new ArrayPublisher<>(ArrayPublisherTest.generate(10));
        MapPublisher<Long, String> mapPublisher = new MapPublisher<>(
            l -> {
                throw new RuntimeException("just a test");
            }, 
            arrayPublisher
        );

        mapPublisher.subscribe(
            new Subscriber<String>() {

                boolean done = false;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(100);
                }

                @Override
                public void onNext(String t) {
                    Assertions.fail("should not be called ever");
                }

                @Override
                public void onError(Throwable t) {
                    Assertions.assertThat(t)
                        .isExactlyInstanceOf(RuntimeException.class)
                        .hasMessage("just a test");

                    Assertions.assertThat(done).isFalse();
                    done = true;
                }

                @Override
                public void onComplete() {
                    Assertions.fail("should not be called ever");
                }
                
            }
        );
    }
}
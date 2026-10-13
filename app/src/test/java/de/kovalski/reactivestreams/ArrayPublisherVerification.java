package de.kovalski.reactivestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;


@Test
public class ArrayPublisherVerification extends PublisherVerification<Long> {

    public ArrayPublisherVerification() {
        super(new TestEnvironment());
    }

    @Override
    public Publisher<Long> createPublisher(long elements) {
        return new ArrayPublisher<>(ArrayPublisherTest.generate(elements));
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        return null;
    }
}

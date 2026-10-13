package de.kovalski.reactivestreams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.assertj.core.api.Assertions;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.Assert;
import org.testng.annotations.Test;

import reactor.core.publisher.Flux;

public class ArrayPublisherTest {


    // Rule 1: onSubscribe() -> onNext() 1...n -> onComplete() || onCancel()
    @Test 
    public void subscriberMethodsOrderTest() throws InterruptedException {
        ArrayPublisher<Long> publisher = new ArrayPublisher<>(generate(5));
        List<String> callSequence = new LinkedList<>();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        publisher.subscribe(new Subscriber<Long>() {

            @Override
            public void onSubscribe(Subscription s) {
                callSequence.add("onSubscribe()");
                s.request(5);
            }

            @Override
            public void onNext(Long t) {
                callSequence.add(String.format("onNext(%d)", t));

            }

            @Override
            public void onComplete() {
                callSequence.add("onComplete()");          
                countDownLatch.countDown();      
            }

            @Override
            public void onError(Throwable t) {
                // pass
            }
        });
        Assert.assertTrue(countDownLatch.await(1000, TimeUnit.MILLISECONDS));
        Assert.assertEquals(callSequence, List.of(
            "onSubscribe()",
            "onNext(0)",
            "onNext(1)",
            "onNext(2)",
            "onNext(3)",
            "onNext(4)",
            "onComplete()"
        ));
    }

    // Rule 2
    @Test 
    public void backpressure() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ArrayList<Long> collected = new ArrayList<>();
        long toRequest = 5L;
        Long[] arr = generate(toRequest);
        ArrayPublisher<Long> publisher = new ArrayPublisher<>(arr);
        Subscription[] subscriptions = new Subscription[1];

        publisher.subscribe(new Subscriber<Long>() {

            @Override
            public void onSubscribe(Subscription s) {
                subscriptions[0] = s;
            }

            @Override
            public void onNext(Long t) {
                collected.add(t);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
        });

        subscriptions[0].request(1);
        Assert.assertEquals(collected, List.of(
            0L
        ));
        subscriptions[0].request(1);
        Assert.assertEquals(collected, List.of(
            0L, 1L
        ));

        subscriptions[0].request(2);
        Assert.assertEquals(collected, List.of(
            0L, 1L, 2L, 3L
        ));

        subscriptions[0].request(20);
        Assert.assertEquals(collected, List.of(
            0L, 1L, 2L, 3L, 4L
        ));
        Assert.assertTrue(countDownLatch.await(1000, TimeUnit.MILLISECONDS));


        // Unconcidered: must NOT print onComplete()
        // subscriptions[0].request(20);

        // subscriptions[0].request(20);

        // subscriptions[0].request(20);
    }

    // Rule: cannot just throw an exception from publisher, need to send a sygnal instead
    @Test 
    public void mustSendOnErrorNpe() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Long[] array = new Long[]{null};
        AtomicReference<Throwable> thReference = new AtomicReference<>();
        ArrayPublisher<Long> arrayPublisher = new ArrayPublisher<>(array);
        arrayPublisher.subscribe(new Subscriber<Long>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.request(5);
            }

            @Override
            public void onNext(Long t) {
            }

            @Override
            public void onError(Throwable t) {
                thReference.set(t);
            }

            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
            
        });

        countDownLatch.await(1000, TimeUnit.MILLISECONDS);
        Assertions.assertThat(thReference.get()).isInstanceOf(NullPointerException.class);

    }


    // Rule 3: must not die because of StackOveflowException
    @Test 
    public void mustNotThrowStackOverflow() throws InterruptedException {
        Long toRequest = 1_000_000L;
        Long[] array = generate(toRequest);
        ArrayPublisher<Long> publisher = new ArrayPublisher<>(array);
        List<Long> collected = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        publisher.subscribe(new Subscriber<Long>() {

            @Override
            public void onSubscribe(Subscription s) {
                subscriptionRef.set(s);
                s.request(1);
            }

            @Override
            public void onNext(Long t) {
                collected.add(t);
                subscriptionRef.get().request(1);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {
                countDownLatch.countDown();;
            }
        });
        Assertions.assertThat(countDownLatch.await(120, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(collected).isEqualTo(new ArrayList<>(Arrays.stream(array).collect(Collectors.toList())));
    }

    @Test
    public void testCancel() throws InterruptedException {
        Long[] arr = generate(5);
        List<Long> collected = new LinkedList<>();
        ArrayPublisher<Long> publisher = new ArrayPublisher<>(arr);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        publisher.subscribe(new Subscriber<Long>() {

            @Override
            public void onSubscribe(Subscription s) {
                s.cancel();
                s.request(5);
            }

            @Override
            public void onNext(Long t) {
                collected.add(t);
            }

            @Override
            public void onError(Throwable t) {
                // pass
            }

            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
        });

        Assertions.assertThat(countDownLatch.await(1, TimeUnit.SECONDS)).isFalse();
        Assertions.assertThat(collected).isEmpty();
    }

    @Test
    public void multithreadingTests() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<Long> collected = new ArrayList<>();
        int n = 5000;
        Long[] array = generate(n);
        ArrayPublisher<Long> publisher = new ArrayPublisher<>(array);
        publisher.subscribe(new Subscriber<Long>() {

            private Subscription s;

            @Override
            public void onSubscribe(Subscription s) {
                this.s = s;
                for (int i = 0; i < n; i++) {
                    ForkJoinPool.commonPool().execute(() -> s.request(1));
                }
            }

            @Override
            public void onNext(Long t) {
                collected.add(t);
            }

            @Override
            public void onError(Throwable t) {
                // pass
            }

            @Override
            public void onComplete() {
                countDownLatch.countDown();
            }
        });

        Assertions.assertThat(countDownLatch.await(100, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(collected)
            .hasSize(array.length)
            .containsExactly(array);
        
    }

    static Long[] generate(long num) {
        if (num >= Integer.MAX_VALUE) {
            num = 1_000_000L;
        }
        return LongStream.range(0, num).boxed().toArray(Long[]::new);
    }
}

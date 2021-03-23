package org.demo;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class Demo1MillionThreads {

    private static final int TOTAL_THREADS = 1_000_000;
    private static boolean SLEEP = false;
    private static boolean VIRTUAL = true;

    public static void main(String[] args) throws Exception {
        System.out.printf("My pid is: %d. About to start %d %s threads . Press enter to continue.\n", 
                          ProcessHandle.current().pid(), 
                          TOTAL_THREADS, 
                          (VIRTUAL ? "Virtual" :  "Native"));
        System.in.read();

        System.out.printf("Starting %d threads...\n", TOTAL_THREADS);
        var counter = new AtomicInteger();
        var init = System.currentTimeMillis();

        var threads = IntStream.range(0, TOTAL_THREADS)
                               .mapToObj(i -> {
                                   var thread = VIRTUAL ? Thread.ofVirtual() : Thread.ofPlatform();
                                   return thread.unstarted(() -> {
                                       if (SLEEP) {
                                           sleep(10000);
                                       }
                                       counter.getAndIncrement();
                                   });
                               })
                               .peek(Thread::start)
                               .toList();
        System.out.printf("Started %d threads in %d milliseconds.\n", TOTAL_THREADS, (System.currentTimeMillis() - init));
        init = System.currentTimeMillis();
        threads.forEach(t -> join(t));
        System.out.printf("Finished the execution of %d threads in %d ms.",
                          counter.get(),
                          System.currentTimeMillis() - init);
    }

    private static void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
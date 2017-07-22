package com.colm.expiremap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author colm_mchugh
 */
public class ExpireMapMulitThreadedTest {

    private final static int NUM_CLIENTS = 10;

    class PutRunner implements Runnable {

        private final int num;
        private final ExpireMap<String, String> myMap;

        public PutRunner(int num, ExpireMap<String, String> myMap) {
            this.num = num;
            this.myMap = myMap;
        }

        @Override
        public void run() {
            this.myMap.put("Key" + num, "Val" + num, 5000);
        }

    }

    @Test
    public void testMultiplePut() {
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CLIENTS);
        for (int i = 0; i < NUM_CLIENTS; i++) {
            Runnable putter = new PutRunner(i, em);
            executor.execute(putter);
        }
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.err.println(e.getLocalizedMessage());
        } finally {
            executor.shutdownNow();
        }
        
        for (int i = 0; i < NUM_CLIENTS; i++) {
            assertEquals(em.get("Key" + i), "Val" + i);
        }
        assertEquals(em.size(), 10);
    }

    @Test
    public void testExpiringPut() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CLIENTS);
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        for (int i = 0; i < NUM_CLIENTS; i++) {
            Runnable putter = new PutRunner(i, em);
            executor.execute(putter);
        }
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            Thread.sleep(1000 * 10);
        } catch (InterruptedException e) {
            System.err.println(e.getLocalizedMessage());
        } finally {
            executor.shutdownNow();
        }
        
        for (int i = 0; i < NUM_CLIENTS; i++) {
            assertNull(em.get("Key" + i));
        }
        assertEquals(em.size(), 0);
    }
    

    class RemoveRunner implements Runnable {

        private final int num;
        private final ExpireMap<String, String> myMap;

        public RemoveRunner(int num, ExpireMap<String, String> myMap) {
            this.num = num;
            this.myMap = myMap;
        }

        @Override
        public void run() {
            this.myMap.remove("Key" + num);
        }

    }
    
    @Test
    public void testMultiplePutRemove() {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CLIENTS);
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        for (int i = 0; i < NUM_CLIENTS; i++) {
            em.put("Key" + i, "Val" + i, 5000);
        }
        for (int i = 0; i < NUM_CLIENTS; i++) {
            Runnable putter = new RemoveRunner(i, em);
            executor.execute(putter);
        }
        try {
            System.out.println("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.err.println(e.getLocalizedMessage());
        } finally {
            executor.shutdownNow();
        }
        
        for (int i = 0; i < NUM_CLIENTS; i++) {
            assertNull(em.get("Key" + i));
        }
        assertEquals(em.size(), 0);
    }
}

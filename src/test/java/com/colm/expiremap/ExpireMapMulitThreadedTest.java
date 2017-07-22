package com.colm.expiremap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test that ExpireMap is consistent - gives expected results - when its API
 * is used by concurrent clients.
 * 
 * @author colm_mchugh
 */
public class ExpireMapMulitThreadedTest {

    private final static int NUM_CLIENTS = 10;

    /**
     * PutRunner invokes put on an ExpireMap
     */
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
        
        // Checks that given N put operations put(k1, v1); put(k2, v2);...;put(kN, VN)
        // on an empty ExpireMap with no defined order on their execution(*), 
        // after they have completed execution there should be exactly N key value
        // pairs with the expected key-value pairs.
        // 
        // (*) there is no guaranteed order on their execution; depending on the
        // environment this code is run in, they may be executed serially, or a 
        // subset of them may be executed in parallel.       
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
        
        // Checks that given N put operations put(k1, v1); put(k2, v2);...;put(kN, VN)
        // on an empty ExpireMap with no defined order on their execution(*), 
        // followed by a wait period longer than the longest expiration time in
        // the puts, then the map should be empty.
        // 
        // (*) there is no guaranteed order on their execution; depending on the
        // environment this code is run in, they may be executed serially, or a 
        // subset of them may be executed in parallel.       
        
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
        
        // Checks that given N serial put operations on an initially empty map,
        // followed by N concurrent removes, followed by waiting for all the 
        // removes to complete, then the map should be empty.
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

package com.colm.expiremap;

import static org.junit.Assert.*;

/**
 *
 * @author colm_mchugh
 */
public class ExpireMapFunctionalTest {
    
    @org.junit.Test
    public void expireTest() throws InterruptedException {
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        String expected = "value1";
        String key = "k1";
        
        em.put(key, "foo", 0); // zero delay => not retrievable
        assertNull(em.get(key));
        
        em.put(key, expected, 2);
        Thread.sleep(1);
        assertEquals(em.get(key), expected);
        
        em.put(key, expected, 2);
        Thread.sleep(3);
        assertNull(em.get(key));
    }

    @org.junit.Test
    public void overwriteTest() throws InterruptedException {
        String v1 = "one";
        String v2 = "two";
        String key = "k1";
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        
        em.put(key, v1, 10);
        Thread.sleep(1);
        assertEquals(em.get(key), v1);
        
        em.put(key, v2, 8);
        Thread.sleep(1);
        assertEquals(em.get(key), v2);
    }
    
    @org.junit.Test
    public void orderTest() throws InterruptedException {
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        em.put("k1", "one", 3000);
        em.put("k2", "two", 1500);
        em.put("k3", "three", 500);
        
        Thread.sleep(1000);
        assertEquals(em.get("k1"), "one");
        assertNull(em.get("k3"));
    }
    
    @org.junit.Test
    public void removeTest() throws InterruptedException {
        String v = "toBeRemoved";
        String key = "k1";
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        
        em.put(key, v, 10);
        Thread.sleep(3);
        em.remove(key);
        assertNull(em.get(key));
    }
    

    
    
}

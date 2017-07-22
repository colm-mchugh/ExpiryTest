package com.colm.expiremap;

import static org.junit.Assert.*;

/**
 * ExpireMapFunctionalTest - test that ExpireMap functionality is working as
 * expected.
 * 
 * @author colm_mchugh
 */
public class ExpireMapFunctionalTest {
    
    @org.junit.Test
    public void expireTest() throws InterruptedException {
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        String expected = "value1";
        String key = "k1";
        
        // This test checks three things:
        
        // 1) A key,value pair with 0 timeout is not retrievable 
        em.put(key, "foo", 0); 
        assertNull(em.get(key));
        
        // 2) A key,value pair that is accessed within its timeout is retrievable
        em.put(key, expected, 2);
        Thread.sleep(1);
        assertEquals(em.get(key), expected);
        
        // 3) A key,value pair accessed after its timeout is not retrievable
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
        
        // This test checks that given put(k1, v1, t1), wait(t2), put(k1, v2, t3)
        // where t2 < t1, get(k1) will return v2
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
        
        // This test checks that a series of puts followed by waiting for 
        // a time greater than the last put should result in the last put
        // no longer being present due to being expired
        Thread.sleep(2000);
        assertEquals(em.get("k1"), "one");
        assertNull(em.get("k3"));
    }
    
    @org.junit.Test
    public void removeTest() throws InterruptedException {
        String v = "toBeRemoved";
        String key = "k1";
        ExpireMap<String, String> em = new ExpireMapImpl<>();
        
        // Check that given put(k1, v1, t1); wait(t2); remove(k1)
        // where t2 < t1, get(k1) will return null, i.e. k1 is no longer present
        em.put(key, v, 10);
        Thread.sleep(3);
        em.remove(key);
        assertNull(em.get(key));
    }
    

    
    
}

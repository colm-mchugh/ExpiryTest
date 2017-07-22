package com.colm.expiremap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

/**
 * ExpireMapImpl implements ExpireMap.
 *
 * Structurally, it consists of a concurrent hash map for providing a
 * thread-safe map to keep key,value pairs; a map for associating each key with
 * its ExpirableKey; a DelayQueue for ordering ExpirableKeys so that expiring
 * keys (if any) can be accessed in constant time. This takes 3 x O(N) space,
 * given N keys.
 *
 * @author colm_mchugh
 */
public class ExpireMapImpl<K, V> implements ExpireMap<K, V> {

    private final Map<K, V> map;
    private final Map<K, ExpirableKey<K>> expirableKeys;
    private final DelayQueue<ExpirableKey<K>> expiryQueue;

    /**
     * Initialize data structures to be all initially empty.
     */
    public ExpireMapImpl() {
        this.map = new ConcurrentHashMap<>();
        this.expirableKeys = new ConcurrentHashMap<>();
        this.expiryQueue = new DelayQueue<>();
    }

    /**
     * Accept the given key,value pair with given timeout.
     * Please see comments in-line for special case treatment.
     * 
     * @param key
     * @param value
     * @param timeoutMs
     */
    @Override
    public void put(K key, V value, long timeoutMs) {
        if (timeoutMs <= 0) {
            // No point in dealing with this key, value pair
            return;
        }
        // Start with fresh data structures (no expired keys hanging around)
        this.clearOldKeysFromExpiryQueue();
        // Create an expirable key for the given key
        ExpirableKey<K> expirableKey = new ExpirableKey<>(key, timeoutMs);
        
        if (this.expirableKeys.containsKey(key)) {
            // The given key is present; expire it, clear it from the expiry
            // queue, and put the new expirable key in the expirable key map.
            ExpirableKey<K> prevKey = this.expirableKeys.get(key);
            prevKey.die();
            this.clearOldKeysFromExpiryQueue();
            this.expirableKeys.put(key, expirableKey);
        }
        this.expiryQueue.offer(expirableKey);
        this.map.put(key, value);
    }

    /**
     * Simply return the value associated with the given key, or null if no 
     * such key. Clears out the expiry queue first so the key,value map is as
     * fresh as possible.
     * 
     * @param key
     * @return 
     */
    @Override
    public V get(K key) {
        this.clearOldKeysFromExpiryQueue();
        return this.map.get(key);
    }

    /**
     * Remove the given key from the key,value map. Also, if the key is present
     * in the expirable keys map, then remove it, force it to die, and invoke
     * expiration queue clear out.
     *
     * @param key
     */
    @Override
    public void remove(K key) {
        map.remove(key);
        ExpirableKey<K> keyToKill = expirableKeys.remove(key);
        if (keyToKill != null) {
            keyToKill.die();
            this.clearOldKeysFromExpiryQueue();
        }
    }

    /**
     * Return the number of key,value pairs in the key,value map.
     *
     * @return
     */
    @Override
    public int size() {
        return this.map.size();
    }

    /**
     * For every expired key in the expiryQueue, remove it from the key,value
     * map and from the expirable keys map. This is an internal API which is
     * invoked on demand; it is essentially a reaper function that clears out
     * old keys.
     */
    private void clearOldKeysFromExpiryQueue() {
        for (ExpirableKey<K> expired = this.expiryQueue.poll();
                expired != null; expired = this.expiryQueue.poll()) {
            this.map.remove(expired.Key());
            this.expirableKeys.remove(expired.Key());
        }
    }
}

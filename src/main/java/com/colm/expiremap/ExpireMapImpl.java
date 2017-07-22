package com.colm.expiremap;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.WeakHashMap;
/**
 *
 * @author colm_mchugh
 */
public class ExpireMapImpl<K, V> implements ExpireMap<K, V> {

    private final Map<K, V> map;
    private final Map<K, ExpirableKey<K>> expirableKeys;
    private final DelayQueue<ExpirableKey<K>> expiryQueue;
    private final boolean doLogging;
    private final PrintStream logger;
    
    public ExpireMapImpl() {
        this.map = new ConcurrentHashMap<>();
        this.expirableKeys = new WeakHashMap<>();
        this.expiryQueue = new DelayQueue<>();
        this.doLogging = true;
        this.logger = System.out;
    }
    
    
    @Override
    public void put(K key, V value, long timeoutMs) {
        if (timeoutMs < 0) {
            return;
        }
        this.clearOldKeysFromExpiryQueue();
        ExpirableKey<K> expirableKey = new ExpirableKey<>(key, timeoutMs);
        ExpirableKey<K> prevKey = this.expirableKeys.put(key, expirableKey);
        if (prevKey != null) {
            prevKey.die();
            this.clearOldKeysFromExpiryQueue();
            this.expirableKeys.put(key, expirableKey);
        }
        this.expiryQueue.offer(expirableKey);
        this.map.put(key, value);
    }

    @Override
    public V get(K key) {
        this.clearOldKeysFromExpiryQueue();
        return this.map.get(key);
    }

    @Override
    public void remove(K key) {
        map.remove(key);
        ExpirableKey<K> keyToKill = expirableKeys.remove(key);
        if (keyToKill != null) {
            keyToKill.die();
            this.clearOldKeysFromExpiryQueue();
        }
    }
    
    @Override
    public int size() {
        return this.map.size();
    }
    
    private void clearOldKeysFromExpiryQueue() {
        for (ExpirableKey<K>  expired = this.expiryQueue.poll();
                expired != null; expired = this.expiryQueue.poll()) {
            this.map.remove(expired.Key());
            this.expirableKeys.remove(expired.Key());
            if (this.doLogging) {
                this.logger.println("Expired key: " + expired.Key());
            }
        }
    }
}

package com.colm.expiremap;

/**
 * ExpireMap - variant of a map where a key,value pair is removed after a 
 * designated amount of time (specified when the pair is put in the map) if
 * not already explicitly removed.
 * 
 * @author colm_mchugh
 */
public interface ExpireMap<K, V> {

    /**
     * Put the given value in the map under the given key.
     * 
     * If there is no entry with the key in the map, add the key/value pair as 
     * a new entry. 
     * If there is an existing entry with the key, the current entry will be 
     * replaced with the new key/value pair. 
     * If the newly added entry is not removed after timeoutMs since it's added 
     * to the map, remove it.
     * 
     * @param key
     * @param value
     * @param timeoutMs 
     */
    void put(K key, V value, long timeoutMs);
  
    /**
     * Get the value associated with the key if present; otherwise, return null.
     * @param key
     * @return 
     */
    V get(K key);

    /**
     * Remove the entry associated with key, if any.
     * @param key 
     */
    void remove(K key);

      
    /**
     * Return the number of key-value mappings.
     * 
     * @return 
     */
    int size();
}

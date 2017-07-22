package com.colm.expiremap;

import java.util.Objects;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 *
 * Note that the API of Delayed requires a getDelay() method, it is more useful 
 * in the context of this application to think of the delay as the remaining 
 * life that the key has left.
 * 
 * @author colm_mchugh
 */
public class ExpirableKey<K> implements Delayed {

    private long birth = System.currentTimeMillis();
    private final long lifeSpan;
    private final K key;

    public ExpirableKey(K key, long lifeSpan) {
        this.key = key;
        this.lifeSpan = lifeSpan;
    }
    
    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(remainingLife(), TimeUnit.MILLISECONDS);
    }
    
    /**
     * Kill this key! Used to forcibly expunge a key, e.g. when putting a key,
     * value pair of an already existing key, or when removing a key that has
     * not yet lived out its lifespan.
     */
    public void die() {
        birth = System.currentTimeMillis() - this.lifeSpan - 1;
    }

    /**
     * ExpirableKeys are ordered on their delay, or remaining life, hence a comparison
     * of two ExpirableKey instances boils down to comparing their remaining life
     * values. If this has less life (smaller delay) than the given parameter,
     * a negative value will be returned; it will be ordered less than (or before).
     * 
     * @param o
     * @return 
     */
    @Override
    public int compareTo(Delayed o) {
        if (o == null) {
            throw new IllegalArgumentException("null operand");
        }
        Long myDelay = this.getDelay(TimeUnit.MILLISECONDS);
        Long itsDelay = this.getDelay(TimeUnit.MILLISECONDS);
        return Long.compare(myDelay, itsDelay);
    }
    
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 11 * hash + Objects.hashCode(this.key);
        return hash;
    }

    /**
     * Implementing equals and hashCode means that ExpirableKey instances can 
     * be put and retrieved from Sets and maps efficiently.
     * 
     * @param obj
     * @return 
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ExpirableKey<?> other = (ExpirableKey<?>) obj;
        return Objects.equals(this.key, other.key);
    }
    
    /**
     * Return how much time, in milliseconds, that this key has left to live.
     * @return 
     */
    public long remainingLife() {
        return (birth + lifeSpan) - System.currentTimeMillis();
    }
    
    K Key() {
        return this.key;
    }
}

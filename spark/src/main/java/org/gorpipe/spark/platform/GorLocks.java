package org.gorpipe.spark.platform;

import org.gorpipe.exceptions.GorSystemException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class GorLocks {

    private Map<GorTaskBase, GorKeyLock> locksHeld = new ConcurrentHashMap<>();
    private long lockReservationMs;

    protected GorLocks(long lockReservation) {
        this.lockReservationMs = lockReservation;
    }

    @SuppressWarnings("resource")
    synchronized boolean lock(GorTaskBase task, String key, long timeoutMs)
            throws InterruptedException {
        if (locksHeld.containsKey(task)) {
            throw new GorSystemException("Only one lock per task", null);
        }
        GorKeyLock lock = onCreateKeyLock(key, lockReservationMs);
        if (lock.lock(timeoutMs)) {
            locksHeld.put(task, lock);
            return true;
        }
        return false;
    }

    protected abstract GorKeyLock onCreateKeyLock(String key, long lockReservation);

    public void unlock(GorTaskBase task) {
        GorKeyLock lock = locksHeld.remove(task);
        if (lock != null) {
            lock.unlock();
        }
    }

    public void refreshAll() {
        for (GorKeyLock lock : locksHeld.values()) {
            lock.refreshLock();
        }
    }

    public void close() {
        for (GorKeyLock lock : locksHeld.values()) {
            lock.unlock();
        }
        locksHeld.clear();
    }

    public boolean hasLock(GorTaskBase task) {
        GorKeyLock lock = locksHeld.get(task);
        return (lock != null) && lock.isLocked();
    }
}

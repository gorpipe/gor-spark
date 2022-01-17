package org.gorpipe.spark.platform;

public abstract class GorKeyLock {
    private String key;

    protected GorKeyLock(String key) {
        this.key = key;
    }

    public abstract boolean lock(long timeoutMs) throws InterruptedException;
    public abstract boolean refreshLock();
    public abstract long getReserveToTime();
    public abstract void unlock();
    public abstract boolean isLocked();

    /**
     * Get key used for lock
     *
     * @return key
     */
    public String getKey() {
        return key;
    }

    /**
     * NB: Assumes that keys are using same redis server
     */
    @Override
    public boolean equals(Object obj) {
        return obj instanceof GorKeyLock && key.equals(((GorKeyLock) obj).getKey());
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

}

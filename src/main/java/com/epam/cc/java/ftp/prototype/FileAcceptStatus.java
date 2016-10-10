package com.epam.cc.java.ftp.prototype;

/**
 * POJO that describes file processing status.
 * <p>
 * Created by Maksym Bruner.
 */
public class FileAcceptStatus {

    public static final int IN_PROGRESS = 0;

    public static final int DONE = 1;

    public static final int REJECTED = 2;

    private int status = IN_PROGRESS;

    private int tries = 0;

    private long lastTryTimestamp = 0L;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getTries() {
        return tries;
    }

    public void setTries(int tries) {
        this.tries = tries;
    }

    public long getLastTryTimestamp() {
        return lastTryTimestamp;
    }

    public void setLastTryTimestamp(long lastTryTimestamp) {
        this.lastTryTimestamp = lastTryTimestamp;
    }
}


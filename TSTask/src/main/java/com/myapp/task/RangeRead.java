package com.myapp.task;

/**
 * Hold a range position with included start position and excluded end position
 */
public class RangeRead {
    /**
     *
     */
    private long includeStartPos;
    /**
     *
     */
    private long excludeEndPos;

    public RangeRead(long includeStartPos, long excludeEndPos) {
        this.includeStartPos = includeStartPos;
        this.excludeEndPos = excludeEndPos;
    }

    public long getIncludeStartPos() {
        return includeStartPos;
    }

    public long getExcludeEndPos() {
        return excludeEndPos;
    }
}

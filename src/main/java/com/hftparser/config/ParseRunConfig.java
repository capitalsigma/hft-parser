package com.hftparser.config;

import com.hftparser.containers.Backoff;
import com.hftparser.containers.Backoffable;
import com.hftparser.containers.NoOpBackoff;
import org.json.JSONObject;

/**
 * Created by patrick on 7/31/14.
 */
public class ParseRunConfig {
    private final int line_queue_size;
    private final int point_queue_size;
    private final boolean backoff;
    private final int min_backoff_s;
    private final int max_backoff_s;
    private final int min_backoff_d;
    private final int max_backoff_d;

    ParseRunConfig(JSONObject json) {
        line_queue_size = json.getInt("line_queue_size");
        point_queue_size = json.getInt("point_queue_size");
        backoff = json.getBoolean("backoff");
        min_backoff_s = json.getInt("min_backoff_s");
        max_backoff_s = json.getInt("max_backoff_s");
        min_backoff_d = json.getInt("min_backoff_d");
        max_backoff_d = json.getInt("max_backoff_d");
    }

    public ParseRunConfig(int line_queue_size,
                          int point_queue_size,
                          boolean backoff,
                          int min_backoff_s,
                          int max_backoff_s,
                          int min_backoff_d,
                          int max_backoff_d) {
        this.line_queue_size = line_queue_size;
        this.point_queue_size = point_queue_size;
        this.backoff = backoff;
        this.min_backoff_s = min_backoff_s;
        this.max_backoff_s = max_backoff_s;
        this.min_backoff_d = min_backoff_d;
        this.max_backoff_d = max_backoff_d;
    }

    public int getLine_queue_size() {
        return line_queue_size;
    }

    public int getPoint_queue_size() {
        return point_queue_size;
    }

    public boolean isBackoff() {
        return backoff;
    }

    public int getMin_backoff_s() {
        return min_backoff_s;
    }

    public int getMax_backoff_s() {
        return max_backoff_s;
    }

    public int getMin_backoff_d() {
        return min_backoff_d;
    }

    public int getMax_backoff_d() {
        return max_backoff_d;
    }

    public Backoffable makeBackoffFor(BackoffType ty) {
        if (!backoff) {
            return new NoOpBackoff();
        }

        if (ty == BackoffType.String) {
            return new Backoff(min_backoff_s, max_backoff_s);
        } else {
            return new Backoff(min_backoff_d, max_backoff_d);
        }

    }

    @Override
    public int hashCode() {
        int result = line_queue_size;
        result = 31 * result + point_queue_size;
        result = 31 * result + (backoff ? 1 : 0);
        result = 31 * result + min_backoff_s;
        result = 31 * result + max_backoff_s;
        result = 31 * result + min_backoff_d;
        result = 31 * result + max_backoff_d;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ParseRunConfig that = (ParseRunConfig) o;

        if (backoff != that.backoff) {
            return false;
        }
        return line_queue_size == that.line_queue_size && max_backoff_d == that.max_backoff_d &&
                max_backoff_s == that.max_backoff_s && min_backoff_d == that.min_backoff_d &&
                min_backoff_s == that.min_backoff_s && point_queue_size == that.point_queue_size;

    }

    public static enum BackoffType {
        String,
        DataPoint
    }
}

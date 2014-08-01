package com.hftparser.config;

import org.json.JSONObject;

/**
 * Created by patrick on 7/31/14.
 */
public class ArcaParserConfig {
    private final int initial_order_history_size;
    private final int output_progress_every;

    public int getInitial_order_history_size() {
        return initial_order_history_size;
    }

    public int getOutput_progress_every() {
        return output_progress_every;
    }

    ArcaParserConfig(JSONObject json) {
        initial_order_history_size = json.getInt("initial_order_history_size");
        output_progress_every = json.getInt("output_progress_every");
    }

    public ArcaParserConfig(int initial_order_history_size, int output_progress_every) {
        this.initial_order_history_size = initial_order_history_size;
        this.output_progress_every = output_progress_every;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ArcaParserConfig that = (ArcaParserConfig) o;

        return initial_order_history_size == that.initial_order_history_size && output_progress_every == that
                .output_progress_every;

    }

    @Override
    public int hashCode() {
        int result = initial_order_history_size;
        result = 31 * result + output_progress_every;
        return result;
    }

    @Override
    public String toString() {
        return "ArcaParserConfig{" +
                "initial_order_history_size=" + initial_order_history_size +
                ", output_progress_every=" + output_progress_every +
                '}';
    }

    public static ArcaParserConfig getDefault() {
        return new ArcaParserConfig(500000, 5000000);
    }
}

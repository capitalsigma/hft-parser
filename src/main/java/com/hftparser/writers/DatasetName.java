package com.hftparser.writers;

/**
 * Created by patrick on 7/28/14.
 */
class DatasetName {
    private final String group;

    public String getGroup() {
        return group;
    }

    public String getDataSet() {
        return dataSet;
    }

    private final String dataSet;

    public DatasetName(String _group, String _dataSet) {
        group = _group;
        dataSet = _dataSet;
    }

    public String getFullPath() {
        return "/" + group + "/" + dataSet;
    }
}

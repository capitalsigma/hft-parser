package com.hftparser.data;

import ch.systemsx.cisd.hdf5.HDF5LinkInformation;

/**
 * Created by patrick on 7/28/14.
 */
public class DataSetName {
    private final String group;

    public String getGroup() {
        return group;
    }

    public String getDataSet() {
        return dataSet;
    }

    private final String dataSet;

    public DataSetName(String _group, String _dataSet) {
        group = _group;
        dataSet = _dataSet;
    }

    public static DataSetName fromHDF5LinkInformation(HDF5LinkInformation linkInformation) {
        String thisGroup = linkInformation.getParentPath();
        String thisDataset = linkInformation.getName();

        return new DataSetName(thisGroup, thisDataset);
    }

    public String getFullPath() {
        return "/" + group + "/" + dataSet;
    }
}

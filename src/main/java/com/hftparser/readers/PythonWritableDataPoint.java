package com.hftparser.readers;

import ch.systemsx.cisd.hdf5.CompoundElement;

/**
 * Created by patrick on 8/4/14.
 */
public class PythonWritableDataPoint extends WritableDataPoint {

    @CompoundElement(memberName = "timestamp_s", dimensions = {16})
    protected String timestamp_s;

    PythonWritableDataPoint() {
//        need default constructor for library
    }

}


package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5LinkInformation;

/**
 * Created by patrick on 8/4/14.
 */
public class DiffElement {
    HDF5LinkInformation expected;
    HDF5LinkInformation actual;
    int index;

    public DiffElement(HDF5LinkInformation expected, HDF5LinkInformation actual) {
        this(expected, actual, -1);
    }

    public DiffElement(HDF5LinkInformation expected, HDF5LinkInformation actual, int index) {
        this.expected = expected;
        this.actual = actual;
        this.index = index;
    }

    @Override
    public String toString() {
        return "DiffElement{" +
                "expected=" + (expected != null ? expected.toString() : null) +
                ", actual=" + (actual != null ? actual.toString() : null) +
                ", index=" + index +
                '}';
    }

    public HDF5LinkInformation getExpected() {
        return expected;
    }

    public HDF5LinkInformation getActual() {
        return actual;
    }
}

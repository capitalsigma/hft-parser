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

    public String deepToString() {
        return "DiffElement{" +
                "expected=" + (expected != null ? expected.getPath() : null) +
                ", actual=" + (actual != null ? actual.getPath() : null) +
                ", index=" + index +
                '}';
    }

    public HDF5LinkInformation getExpected() {
        return expected;
    }

    public HDF5LinkInformation getActual() {
        return actual;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DiffElement that = (DiffElement) o;

        if (index != that.index) {
            return false;
        }
        //        we compare on paths
        //        if (actual != null ? !actual.equals(that.actual) : that.actual != null) {
        //            return false;
        //        }
        //        if (expected != null ? !expected.equals(that.expected) : that.expected != null) {
        //            return false;
        //        }

        return compareLinkInformation(this.expected, that.expected) && compareLinkInformation(this.actual, that.actual);
    }


    private static boolean compareLinkInformation(HDF5LinkInformation expected, HDF5LinkInformation actual) {
        if ((expected == null) && (actual == null)) {
            return true;
        } else if ((expected == null) || (actual == null)) {
            return false;
        } else {
            return (expected.getType() == actual.getType()) && (expected.getPath().equals(actual.getPath()));
        }
    }

    @Override
    public int hashCode() {
        int result = expected != null ? expected.hashCode() : 0;
        result = 31 * result + (actual != null ? actual.hashCode() : 0);
        result = 31 * result + index;
        return result;
    }
}

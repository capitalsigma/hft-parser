package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.hftparser.writers.DatasetName;
import com.hftparser.writers.HDF5CompoundDSBridgeBuilder;
import com.hftparser.writers.HDF5CompoundDSReadOnlyBridge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by patrick on 8/4/14.
 */
public class HDF5CompoundVerifier<T> {
    private final IHDF5Reader expectedReader;
    private final IHDF5Reader actualReader;
    private final boolean READ_LINK_TARGETS = false;
    private final HDF5CompoundDSBridgeBuilder<T> expectedBridgeBuilder;
    private final HDF5CompoundDSBridgeBuilder<T> actualBridgeBuilder;

    public HDF5CompoundVerifier(HDF5CompoundDSBridgeBuilder<T> actualBridgeBuilder, HDF5CompoundDSBridgeBuilder<T>
            expectedBridgeBuilder, IHDF5Reader actualReader, IHDF5Reader expectedReader) {
        this.actualBridgeBuilder = actualBridgeBuilder;
        this.expectedBridgeBuilder = expectedBridgeBuilder;
        this.actualReader = actualReader;
        this.expectedReader = expectedReader;
    }

    public boolean compare() {
        return diff() == null;
    }

    public DiffElement diff() {
        List<HDF5LinkInformation> expectedInfo;
        List<HDF5LinkInformation> actualInfo;


        return new DiffElement(null, null);
    }

    private DiffElement compareLinkInfos(List<HDF5LinkInformation> expectedInfo, List<HDF5LinkInformation> actualInfo) {
        DiffElement toRet;

        if ((toRet = findExtraElement(expectedInfo, actualInfo)) != null) {
            return toRet;
        } else {
            for (HDF5LinkInformation expectedLink : expectedInfo) {
                String path = expectedLink.getPath();
                HDF5LinkInformation actualLink = actualReader.getLinkInformation(path);

                switch (expectedLink.getType()) {
                    case GROUP:
                        toRet = compareLinkInfos(expectedReader.object().getGroupMemberInformation(path,
                                READ_LINK_TARGETS), actualReader.object().getGroupMemberInformation(path,
                                READ_LINK_TARGETS));

                        break;

                    case DATASET:
                        toRet = compareDataSets(expectedLink, actualLink);

                    default:
                        throw new UnsupportedOperationException();
                }

                if (toRet != null) {
                    return toRet;
                }

            }
        }

        return null;
    }

    private T[] getArrayForBridgeBuilder(HDF5CompoundDSBridgeBuilder<T> builder, HDF5LinkInformation linkInformation) {
        HDF5CompoundDSReadOnlyBridge<T> bridge = builder.build(DatasetName.fromHDF5LinkInformation(linkInformation));

        return bridge.readArray();
    }

    private DiffElement compareDataSets(HDF5LinkInformation expectedLink, HDF5LinkInformation actualLink) {
        T[] expectedArray = getArrayForBridgeBuilder(expectedBridgeBuilder, expectedLink);
        T[] actualArray = getArrayForBridgeBuilder(actualBridgeBuilder, actualLink);

        if (expectedArray.length != actualArray.length) {
            return new DiffElement(expectedLink, actualLink, Math.min(expectedArray.length, actualArray.length));
        }

        for (int i = 0; i < expectedArray.length; i++) {
            if (!expectedArray[i].equals(actualArray[i])) {
                return new DiffElement(expectedLink, actualLink, i);
            }
        }

        return null;
    }

    private DiffElement findExtraElement(List<HDF5LinkInformation> expectedInfo,
                                            List<HDF5LinkInformation> actualInfo) {
        List<HDF5LinkInformation> expectedNotInActual = new ArrayList<>(expectedInfo);
        expectedNotInActual.removeAll(actualInfo);

        if (!expectedNotInActual.isEmpty()) {
            return new DiffElement(null, expectedNotInActual.get(0));
        }

        List<HDF5LinkInformation> actualNotInExpected = new ArrayList<>(actualInfo);
        actualNotInExpected.removeAll(expectedInfo);

        if (!actualNotInExpected.isEmpty()) {
            return new DiffElement(actualNotInExpected.get(0), null);
        }

        return null;
    }

    public void closeReaders() {
        expectedReader.close();
        actualReader.close();
    }
}

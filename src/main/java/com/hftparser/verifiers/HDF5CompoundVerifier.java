package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
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
    private final String ROOT;

    public HDF5CompoundVerifier(IHDF5Reader expectedReader, IHDF5Reader actualReader,
                                HDF5CompoundDSBridgeBuilder<T> expectedBridgeBuilder,
                                HDF5CompoundDSBridgeBuilder<T> actualBridgeBuilder, String root) {
        this.actualBridgeBuilder = actualBridgeBuilder;
        this.expectedBridgeBuilder = expectedBridgeBuilder;
        this.actualReader = actualReader;
        this.expectedReader = expectedReader;
        ROOT = root;
    }

    public HDF5CompoundVerifier(IHDF5Writer expectedWriter, IHDF5Writer actualWriter, Class<T> typeForBridge,
                                String root) {
        actualBridgeBuilder = new HDF5CompoundDSBridgeBuilder<>(actualWriter);
        expectedBridgeBuilder = new HDF5CompoundDSBridgeBuilder<>(expectedWriter);
        actualBridgeBuilder.setTypeFromInferred(typeForBridge);
        expectedBridgeBuilder.setTypeFromInferred(typeForBridge);

        actualReader = actualWriter;
        expectedReader = expectedWriter;
        ROOT = root;
    }

    public HDF5CompoundVerifier(IHDF5Writer expectedWriter, IHDF5Writer actualWriter, Class<T> typeForBridge) {
        this(expectedWriter, actualWriter, typeForBridge, "/");
    }

    public HDF5CompoundVerifier(IHDF5Reader expectedReader, IHDF5Reader actualReader,
                               HDF5CompoundDSBridgeBuilder<T> expectedBridgeBuilder,
                               HDF5CompoundDSBridgeBuilder<T> actualBridgeBuilder) {
        this(expectedReader, actualReader, expectedBridgeBuilder, actualBridgeBuilder, "/");
    }

    public boolean compare() {
        return diff() == null;
    }

    public DiffElement diff(String rootGroup) {
        List<HDF5LinkInformation> expectedInfos = getInformationForGroup(expectedReader, rootGroup);
        List<HDF5LinkInformation> actualInfos = getInformationForGroup(actualReader, rootGroup);


        return compareLinkInfos(expectedInfos, actualInfos);
    }

    public DiffElement diff() {
        return diff(ROOT);
    }

    private List<HDF5LinkInformation> getInformationForGroup(IHDF5Reader reader, String path) {
        return reader.object().getGroupMemberInformation(path, READ_LINK_TARGETS);
    }

    private DiffElement compareLinkInfos(List<HDF5LinkInformation> expectedInfos,
                                         List<HDF5LinkInformation> actualInfos) {
        DiffElement toRet;

        System.out.println("Testing for expected infos: " + Arrays.deepToString(expectedInfos.toArray()));
        System.out.println("And actual infos: " + Arrays.deepToString(actualInfos.toArray()));

        if ((toRet = findExtraElement(expectedInfos, actualInfos)) != null) {
            return toRet;
        } else {
            for (HDF5LinkInformation expectedLink : expectedInfos) {
                String path = expectedLink.getPath();
                HDF5LinkInformation actualLink = actualReader.getLinkInformation(path);

                switch (expectedLink.getType()) {
                    case GROUP:
                        toRet = compareLinkInfos(getInformationForGroup(expectedReader, path),
                                                 getInformationForGroup(actualReader, path));

                        break;

                    case DATASET:
                        toRet = compareDataSets(expectedLink, actualLink);
                        break;

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

        System.out.println("got arrays of length: " + expectedArray.length + ", " + actualArray.length);

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
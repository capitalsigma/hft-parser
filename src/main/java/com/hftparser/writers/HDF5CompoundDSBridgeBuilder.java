package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.*;

/**
 * Created by patrick on 7/28/14.
 */
public class HDF5CompoundDSBridgeBuilder<T> {
    private HDF5CompoundType<T> type;
    private final IHDF5CompoundWriter writer;
    private long startSize;
    private int chunkSize;
    private final HDF5CompoundDSBridgeConfig bridgeConfig;
    private AbstractExecutorService executor;
    private int corePoolSize = 3;
    private int maxPoolSize = 3;
    private long keepAliveSec = 30;
    private int queueSize = 500;
    private boolean async;
    private boolean parallelFlush;
    private ElementCacheFactory<T> cacheFactory;

    public HDF5CompoundType<T> getType() {
        return type;
    }

    public void setTypeFromInferred(Class<T> typeToInferClass) {
        type = writer.getInferredType(typeToInferClass);
    }

    public void setTypeForDSPath(String dataSetPath, Class<T> typeToInferClass) {
        type = writer.getInferredType(dataSetPath, typeToInferClass);
    }

    public void setAnonTypeFromInferred(Class<T> typeToInferClass) {
        type = writer.getInferredAnonType(typeToInferClass);
    }

    public long getStartSize() {
        return startSize;
    }

    public void setStartSize(long startSize) {
        this.startSize = startSize;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public void setCutoff(boolean cutoff) {
        cacheFactory.setCutoff(cutoff);
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public void setKeepAliveSec(long keepAliveSec) {
        this.keepAliveSec = keepAliveSec;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public HDF5CompoundDSBridgeBuilder(IHDF5Writer writer) {
        this(writer, HDF5CompoundDSBridgeConfig.getDefault());
    }

    HDF5CompoundDSBridgeBuilder(IHDF5Writer writer, HDF5CompoundDSBridgeConfig bridgeConfig) {
        this.bridgeConfig = bridgeConfig;
        this.writer = writer.compound();
        this.cacheFactory = new ElementCacheFactory<>(bridgeConfig.getCache_size(), bridgeConfig.isCutoff());

        corePoolSize = bridgeConfig.getCore_pool_size();
        maxPoolSize = bridgeConfig.getMax_pool_size();
        keepAliveSec = bridgeConfig.getKeep_alive_sec();
        queueSize = bridgeConfig.getQueue_size();
        async = bridgeConfig.isAsync();
        parallelFlush = bridgeConfig.isParallel_write();
    }

    public HDF5CompoundDSBridge<T> build(
            @NotNull
            DatasetName name) throws HDF5FormatNotFoundException {
        if (type == null || writer == null) {
            throw new HDF5FormatNotFoundException();
        } else {
//            System.out.println("Building. Parallel flush? " + parallelFlush);
            if (parallelFlush) {
                return buildParallelFlush(name);
            } else if (async) {
                return buildAsync(name);
            } else if (bridgeConfig.getCache_size() > 0) {
                //                System.out.println("Building caching");

                return buildCaching(name);
            } else {
                //                System.out.println("Building regular");
                return new HDF5CompoundDSBridge<>(name, type, writer, startSize, chunkSize, bridgeConfig);
            }
        }
    }

    public HDF5CompoundDSCachingBridge<T> buildCaching(
            @NotNull
            DatasetName name) throws HDF5FormatNotFoundException {
        return new HDF5CompoundDSCachingBridge<>(name, type, writer, startSize, chunkSize, bridgeConfig, cacheFactory);
    }

    public HDF5CompoundDSReadOnlyBridge<T> buildReadOnly(
            @NotNull
            DatasetName name) throws HDF5FormatNotFoundException {
        if (type == null || writer == null) {
            throw new HDF5FormatNotFoundException();
        }

        return new HDF5CompoundDSReadOnlyBridge<>(name, type, writer);
    }

    @Override
    public String toString() {
        return "HDF5CompoundDSBridgeBuilder{" +
                "type=" + type +
                ", writer=" + writer +
                ", startSize=" + startSize +
                ", chunkSize=" + chunkSize +
                ", bridgeConfig=" + bridgeConfig +
                ", executor=" + executor +
                ", corePoolSize=" + corePoolSize +
                ", maxPoolSize=" + maxPoolSize +
                ", keepAliveSec=" + keepAliveSec +
                ", queueSize=" + queueSize +
                ", async=" + async +
                ", parallelFlush=" + parallelFlush +
                ", cacheFactory=" + cacheFactory +
                '}';
    }

    private void initExecutor() {
        executor = new ThreadPoolExecutor(corePoolSize,
                                          maxPoolSize,
                                          keepAliveSec,
                                          TimeUnit.SECONDS,
                                          new LinkedBlockingQueue<Runnable>());
    }

    public HDF5CompoundDSAsyncBridge<T> buildAsync(
            @NotNull
            DatasetName name) throws HDF5FormatNotFoundException {
        if (executor == null) {
            initExecutor();
        }

        return new HDF5CompoundDSAsyncBridge<>(name,
                                               type,
                                               writer,
                                               startSize,
                                               chunkSize,
                                               bridgeConfig,
                                               cacheFactory,
                                               executor);

    }


    public HDF5CompoundDSParallelFlushBridge<T> buildParallelFlush(
            @NotNull
            DatasetName name) {
        if (executor == null) {
            initExecutor();
        }
//
//        System.out.println("Building parallel flush bridge");

        return new HDF5CompoundDSParallelFlushBridge<>(name,
                                                       type,
                                                       writer,
                                                       startSize,
                                                       chunkSize,
                                                       bridgeConfig,
                                                       cacheFactory,
                                                       executor);
    }
}

package org.apache.paimon.flink.lookup;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.paimon.flink.FlinkRowWrapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncLookupFunction extends org.apache.flink.table.functions.AsyncLookupFunction {

    private static final long serialVersionUID = 1L;

    private final FileStoreLookupFunction lookupFunction;

    /**
     * Since FileStoreLookupFunction is not a thread safe class, we need to make sure that only one thread.
     */
    private transient ExecutorService asyncThreadPool;

    public AsyncLookupFunction(FileStoreLookupFunction lookupFunction) {
        this.lookupFunction = lookupFunction;
    }


    public void open(FunctionContext context) throws Exception {
        this.asyncThreadPool = Executors.newSingleThreadExecutor(new ExecutorThreadFactory("paimon-async-lookup-worker"));
        this.lookupFunction.open(context);
    }

    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        // TODO introduce retry logic.
                        return lookupFunction.lookup(keyRow);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                asyncThreadPool);
    }

    public Collection<RowData> lookup(RowData keyRow) {
        return null;
    }

    public void close() throws IOException {
        this.lookupFunction.close();
    }
}

package org.apache.paimon.flink.lookup;

import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;

public interface MixedLookupFunctionProvider extends AsyncLookupFunctionProvider, LookupFunctionProvider {
    static MixedLookupFunctionProvider of(AsyncLookupFunction asyncLookupFunction, LookupFunction lookupFunction) {
        return new MixedLookupFunctionProvider() {
            @Override
            public AsyncLookupFunction createAsyncLookupFunction() {
                return asyncLookupFunction;
            }

            @Override
            public LookupFunction createLookupFunction() {
                return lookupFunction;
            }
        };
    }
}
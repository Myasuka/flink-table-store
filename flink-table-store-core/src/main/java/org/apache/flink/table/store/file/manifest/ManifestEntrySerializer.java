/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;
import org.apache.flink.table.store.file.utils.VersionedObjectSerializer;
import org.apache.flink.table.types.logical.RowType;

/** Serializer for {@link ManifestEntry}. */
public class ManifestEntrySerializer extends VersionedObjectSerializer<ManifestEntry> {

    private static final long serialVersionUID = 1L;

    private final RowDataSerializer partitionSerializer;
    private final DataFileMetaSerializer dataFileMetaSerializer;

    public ManifestEntrySerializer(RowType partitionType, RowType keyType, RowType valueType) {
        super(ManifestEntry.schema(partitionType, keyType, valueType));
        this.partitionSerializer = new RowDataSerializer(partitionType);
        this.dataFileMetaSerializer = new DataFileMetaSerializer(keyType, valueType);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public RowData convertTo(ManifestEntry entry) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, entry.kind().toByteValue());
        row.setField(1, entry.partition());
        row.setField(2, entry.bucket());
        row.setField(3, entry.totalBuckets());
        row.setField(4, dataFileMetaSerializer.toRow(entry.file()));
        return row;
    }

    @Override
    public ManifestEntry convertFrom(int version, RowData row) {
        if (version != 1) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        return new ManifestEntry(
                ValueKind.fromByteValue(row.getByte(0)),
                partitionSerializer
                        .toBinaryRow(row.getRow(1, partitionSerializer.getArity()))
                        .copy(),
                row.getInt(2),
                row.getInt(3),
                dataFileMetaSerializer.fromRow(row.getRow(4, dataFileMetaSerializer.numFields())));
    }
}

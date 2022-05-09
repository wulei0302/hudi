/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This only use by reader returning.
 */
public class HoodieIndexRecord extends HoodieRecord<IndexedRecord> {

  public HoodieIndexRecord(IndexedRecord data) {
    super(null, data);
  }

  public HoodieIndexRecord(HoodieKey key, IndexedRecord data) {
    super(key, data);
  }

  public HoodieIndexRecord(HoodieKey key, IndexedRecord data, HoodieOperation operation) {
    super(key, data, operation);
  }

  public HoodieIndexRecord(HoodieRecord<IndexedRecord> record) {
    super(record);
  }

  public HoodieIndexRecord() {
  }

  @Override
  public HoodieRecord<IndexedRecord> newInstance() {
    return null;
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    return keyGeneratorOpt.isPresent() ? keyGeneratorOpt.get().getRecordKey((GenericRecord) data) : ((GenericRecord) data).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
  }

  @Override
  public HoodieRecord<IndexedRecord> preCombine(HoodieRecord<IndexedRecord> previousRecord) {
    return null;
  }

  @Override
  public Option<HoodieRecord<IndexedRecord>> combineAndGetUpdateValue(HoodieRecord previousRecord, Schema schema, Properties props) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord mergeWith(HoodieRecord other, Schema readerSchema, Schema writerSchema) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema, TypedProperties props) throws IOException {
    GenericRecord avroPayloadInNewSchema =
        HoodieAvroUtils.rewriteRecord((GenericRecord) data, targetSchema);
    return new HoodieIndexRecord(avroPayloadInNewSchema);
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields) throws IOException {
    GenericRecord rewriteRecord = schemaOnReadEnabled ? HoodieAvroUtils.rewriteRecordWithNewSchema(data, writeSchemaWithMetaFields, new HashMap<>())
        : HoodieAvroUtils.rewriteRecord((GenericRecord) data, writeSchemaWithMetaFields);
    return new HoodieIndexRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithMetadata(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields, String fileName) throws IOException {
    GenericRecord rewriteRecord = schemaOnReadEnabled ? HoodieAvroUtils.rewriteEvolutionRecordWithMetadata((GenericRecord) data, writeSchemaWithMetaFields, fileName)
        : HoodieAvroUtils.rewriteRecordWithMetadata((GenericRecord) data, writeSchemaWithMetaFields, fileName);
    return new HoodieIndexRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema, Map<String, String> renameCols) throws IOException {
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(data, newSchema, renameCols);
    return new HoodieIndexRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema) throws IOException {
    GenericRecord oldRecord = (GenericRecord) data;
    GenericRecord rewriteRecord = HoodieAvroUtils.rewriteRecord(oldRecord, newSchema);
    return new HoodieIndexRecord(rewriteRecord);
  }

  @Override
  public HoodieRecord addMetadataValues(Map<HoodieMetadataField, String> metadataValues) throws IOException {
    // NOTE: RewriteAvroPayload is expected here
    Arrays.stream(HoodieMetadataField.values()).forEach(metadataField -> {
      String value = metadataValues.get(metadataField);
      if (value != null) {
        ((GenericRecord) data).put(metadataField.getFieldName(), metadataValues.get(metadataField));
      }
    });

    return new HoodieIndexRecord(data);
  }

  @Override
  public HoodieRecord overrideMetadataValue(HoodieMetadataField metadataField, String value) throws IOException {
    // NOTE: RewriteAvroPayload is expected here

    data.put(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(metadataField.getFieldName()), value);
    return new HoodieAvroRecord(getKey(), new RewriteAvroPayload((GenericRecord) data), getOperation());
  }

  @Override
  public boolean isIgnoredRecord(Schema schema, Properties prop) throws IOException {
    return false;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return null;
  }

  @Override
  public boolean canBeIgnored() {
    return false;
  }
}

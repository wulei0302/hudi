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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieRecordFileWriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.TypeUtils.unsafeCast;

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
  public void writeWithMetadata(HoodieFileWriter writer, Schema schema, Properties props) throws IOException {
    HoodieRecordFileWriter<IndexedRecord> avroWriter = unsafeCast(writer);
    IndexedRecord avroPayload = getData();

    avroWriter.writeWithMetadata(avroPayload, this);
  }

  @Override
  public void write(HoodieFileWriter writer, Schema schema, Properties props) throws IOException {
    HoodieRecordFileWriter<IndexedRecord> avroWriter = unsafeCast(writer);
    IndexedRecord avroPayload = getData();

    avroWriter.write(getRecordKey(), avroPayload);
  }

  @Override
  public HoodieRecord<IndexedRecord> preCombine(HoodieRecord<IndexedRecord> previousRecord) {
    return null;
  }

  @Override
  public Option<HoodieRecord<IndexedRecord>> combineAndGetUpdateValue(HoodieRecord<IndexedRecord> previousRecord, Schema schema, Properties props) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord mergeWith(HoodieRecord other, Schema readerSchema, Schema writerSchema) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema, TypedProperties props) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord addMetadataValues(Map<HoodieMetadataField, String> metadataValues) throws IOException {
    return null;
  }

  @Override
  public HoodieRecord overrideMetadataValue(HoodieMetadataField metadataField, String value) throws IOException {
    return null;
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

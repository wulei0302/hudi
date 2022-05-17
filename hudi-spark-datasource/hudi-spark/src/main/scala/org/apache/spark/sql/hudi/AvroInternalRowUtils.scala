/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.spark.sql.avro.{AvroDeserializer, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow}
import org.apache.spark.sql.types.{DataType, StructType}

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.util.control.Breaks._

object AvroInternalRowUtils {

  @volatile var longestSchema: Schema = null
  private val schema2AvroDeserializer = new ConcurrentHashMap[Schema, AvroDeserializer]()
  private val schema2AvroSerializer = new ConcurrentHashMap[Schema, AvroSerializer]()
  private val fieldName2DataType = new ConcurrentHashMap[String, DataType]
  private val fieldName2Serializer = new ConcurrentHashMap[String, AvroSerializer]
  private val fieldName2AvroDeserializer = new ConcurrentHashMap[String, AvroDeserializer]
  // GenericRecord -> GenericInternalRow,in user perspective，it is serialize
  @throws[IOException]
  def serialize(record: GenericRecord): GenericInternalRow = makeNewAvroDeserializer(record.getSchema).deserialize(record).asInstanceOf[SpecificInternalRow].copy

  def deserialize(dataRow: InternalRow, schema: Schema): IndexedRecord = schema2AvroSerializer.get(schema).serialize(dataRow).asInstanceOf[IndexedRecord]

  @throws[IOException]
  def makeNewAvroDeserializer(schema: Schema): AvroDeserializer = {
    if (schema == null) throw new IOException("Schema should not be null!")
    if (schema2AvroDeserializer.containsKey(schema)) {
      schema2AvroDeserializer.get(schema)
    }
    else {
      initWithNewSchema(schema)
    }
  }

  def initWithNewSchema(schema: Schema): AvroDeserializer = synchronized {
    if ((longestSchema == null) || (longestSchema != null && schema.getFields.size() > longestSchema.getFields.size())) {
      longestSchema = schema
    }
    initToAvroSerializer(schema)
    initFieldName2DataType(schema)
    fieldName2Serializer.clear()
    initFieldAvroDeSerializer(schema)
    initToAvroDeserializer(schema)
  }

  def getFieldValue(dataRow: GenericInternalRow, fieldName: String): Any = if (dataRow == null) null
  else getFieldSerializer(fieldName).serialize(dataRow.get(longestSchema.getField(fieldName).pos, fieldName2DataType.get(fieldName)))

  def getInternalRowFieldValue(ob: Any, fieldName: String): Any = if (ob == null) null
  else fieldName2AvroDeserializer.get(fieldName).deserialize(ob)

  private def getFieldSerializer(fieldName: String) = if (fieldName2Serializer.containsKey(fieldName)) fieldName2Serializer.get(fieldName)
  else {
    val serializer = new AvroSerializer(fieldName2DataType.get(fieldName), longestSchema.getField(fieldName).schema, true)
    fieldName2Serializer.put(fieldName, serializer)
    serializer
  }

  private def initFieldName2DataType(schema: Schema): Unit = {
    val typeValue = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
    for (field <- typeValue.fields) {
      fieldName2DataType.put(field.name, field.dataType)
    }
  }

  private def initToAvroDeserializer(schema: Schema): AvroDeserializer = {
    val avroDeserializer: AvroDeserializer = new AvroDeserializer(schema, SchemaConverters.toSqlType(schema).dataType)
    schema2AvroDeserializer.put(schema, avroDeserializer)
    avroDeserializer
  }

  private def initToAvroSerializer(schema: Schema): Unit = {
    val toAvroSerializer = new AvroSerializer(SchemaConverters.toSqlType(schema).dataType, schema, true)
    schema2AvroSerializer.put(schema, toAvroSerializer)
  }

  private def initFieldAvroDeSerializer(schema: Schema): Unit = {
    for(f <- schema.getFields ){
      var newSchema = f.schema()
      if (newSchema.getType == Type.UNION) {
        newSchema = getNotNullSchema(f.schema())
      }
      val avroDeserializer: AvroDeserializer = new AvroDeserializer(newSchema, SchemaConverters.toSqlType(newSchema).dataType)
      fieldName2AvroDeserializer.put(f.name(), avroDeserializer)
    }
  }

  private def getNotNullSchema(schemas: Schema): Schema = {
    var retSchema = schemas
    breakable {
      for (schema <- schemas.getTypes) {
        if (!(schema.getType == Type.NULL)) {
          retSchema = schema
          break
        }
      }
    }
    retSchema
  }
}


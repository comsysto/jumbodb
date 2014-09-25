/**
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

package org.jumbodb.connector.hadoop.index.strategy.common.partition;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.jumbodb.connector.hadoop.configuration.IndexField;

/**
 * An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text..
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractJsonFieldInputFormat<K> extends FileInputFormat<K, NullWritable> {

    @Override
    public RecordReader<K, NullWritable>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        return new JsonFieldLineRecordReader<K>(recordDelimiterBytes) {
            @Override
            public K getIndexableValue(IndexField indexField, JsonNode input) {
                return AbstractJsonFieldInputFormat.this.getIndexableValue(indexField, input);
            }
        };
    }

    protected abstract K getIndexableValue(IndexField indexField, JsonNode input);

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }

    public JsonNode getNodeFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
            jsonNode = jsonNode.path(s);
        }
        return jsonNode;
    }

}

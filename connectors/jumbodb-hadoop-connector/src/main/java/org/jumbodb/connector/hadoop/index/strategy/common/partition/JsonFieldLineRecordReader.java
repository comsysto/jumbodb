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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;

/**
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public abstract class JsonFieldLineRecordReader<K> extends RecordReader<K, NullWritable> {
    private static final Log LOG = LogFactory.getLog(JsonFieldLineRecordReader.class);
    public static final String MAX_LINE_LENGTH =
            "mapreduce.input.linerecordreader.line.maxlength";

    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private K key;
    private Text line = new Text();
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;
    private ObjectMapper objectMapper = new ObjectMapper();
    private IndexField indexField;

    public JsonFieldLineRecordReader() {
    }

    public JsonFieldLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);

        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
        if (null != codec) {
            isCompressedInput = true;
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream cIn =
                        ((SplittableCompressionCodec) codec).createInputStream(
                                fileIn, decompressor, start, end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                in = new CompressedSplitLineReader(cIn, job,
                        this.recordDelimiterBytes);
                start = cIn.getAdjustedStart();
                end = cIn.getAdjustedEnd();
                filePosition = cIn;
            } else {
                in = new SplitLineReader(codec.createInputStream(fileIn,
                        decompressor), job, this.recordDelimiterBytes);
                filePosition = fileIn;
            }
        } else {
            fileIn.seek(start);
            in = new SplitLineReader(fileIn, job, this.recordDelimiterBytes);
            filePosition = fileIn;
        }
        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.pos = start;
    }


    private int maxBytesToConsume(long pos) {
        return isCompressedInput
                ? Integer.MAX_VALUE
                : (int) Math.min(Integer.MAX_VALUE, end - pos);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }

    public boolean nextKeyValue() throws IOException {
//        if (key == null) {
//            key = newKeyInstance();
//        }
        int newSize = 0;
        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
            newSize = in.readLine(line, maxLineLength,
                    Math.max(maxBytesToConsume(pos), maxLineLength));
            JsonNode jsonNode = objectMapper.readValue(line.getBytes(), JsonNode.class);
            key = getIndexableValue(indexField, jsonNode);
            pos += newSize;
            if ((newSize < maxLineLength && key != null) || newSize == 0) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " +
                    (pos - newSize));
        }
        if (newSize == 0 || key == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public K getCurrentKey() {
        return key;
    }

    @Override
    public NullWritable getCurrentValue() {
        return NullWritable.get();
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }

    public abstract K getIndexableValue(IndexField indexField, JsonNode input);
}

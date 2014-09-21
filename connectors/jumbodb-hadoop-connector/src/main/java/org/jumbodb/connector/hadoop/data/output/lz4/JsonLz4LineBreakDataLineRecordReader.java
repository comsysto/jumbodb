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

package org.jumbodb.connector.hadoop.data.output.lz4;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.jumbodb.data.common.lz4.LZ4BlockInputStream;
import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;

/**
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class JsonLz4LineBreakDataLineRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(JsonLz4LineBreakDataLineRecordReader.class);
    public static final String MAX_LINE_LENGTH =
            "mapreduce.input.linerecordreader.line.maxlength";

    private long start;
    private long uncompressedPos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private byte[] recordDelimiterBytes;
    private FSDataInputStream fileIn;
    private LZ4BlockInputStream lz4BlockInputStream;

    public JsonLz4LineBreakDataLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        lz4BlockInputStream = new LZ4BlockInputStream(fileIn);

        if (null == this.recordDelimiterBytes) {
            in = new LineReader(lz4BlockInputStream, job);
        } else {
            in = new LineReader(lz4BlockInputStream, job, this.recordDelimiterBytes);
        }

        if (start != 0) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.uncompressedPos = start;
    }


    private int maxBytesToConsume(long pos) {
        return (int) (end - pos);
    }

    private long getFilePosition() throws IOException {
        return fileIn.getPos();
    }

    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(uncompressedPos);
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        while (getFilePosition() <= end) {
            newSize = in.readLine(value, maxLineLength,
                    Math.max(maxBytesToConsume(uncompressedPos), maxLineLength));
            uncompressedPos += newSize;
            if (newSize < maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at uncompressedPos " +
                    (uncompressedPos - newSize));
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        }
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
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
        if (in != null) {
            in.close();
        }
        if (lz4BlockInputStream != null) {
            lz4BlockInputStream.close();
        }
    }
}

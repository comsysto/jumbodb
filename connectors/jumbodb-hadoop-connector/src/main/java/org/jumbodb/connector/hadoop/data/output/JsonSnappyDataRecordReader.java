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

package org.jumbodb.connector.hadoop.data.output;

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
import org.xerial.snappy.SnappyInputStream;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Treats keys as offset in file and value as line.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce", "Pig"})
@InterfaceStability.Evolving
public class JsonSnappyDataRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(JsonSnappyDataRecordReader.class);

    private long start;
    private long uncompressedPos;
    private long end;
    private LongWritable key;
    private Text value;
    private SnappyInputStream snappyInputStream;
    private DataInputStream dataInputStream;
    private FSDataInputStream fileIn;
    private byte[] data = new byte[0];

    public JsonSnappyDataRecordReader() {
    }

    @Override
    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        snappyInputStream = new SnappyInputStream(fileIn);
        dataInputStream = new DataInputStream(snappyInputStream);
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
        int length = dataInputStream.readInt();
        if(length == -1) {
            return false; // end reached
        }
        if(data.length < length) {
            data = new byte[length];
        }
        dataInputStream.read(data, 0, length);
        uncompressedPos += length + 4;
        value.set(data, 0, length);
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
        if (dataInputStream != null) {
            dataInputStream.close();
        }
        if (snappyInputStream != null) {
            snappyInputStream.close();
        }
    }
}

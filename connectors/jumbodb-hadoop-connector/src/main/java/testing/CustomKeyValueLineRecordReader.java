package testing;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.jumbodb.connector.hadoop.index.strategy.common.hashcode32.GenericJsonHashCode32IndexMapper;

/**
 * This class treats a line in the input as a key/value pair separated by a 
 * separator character. The separator can be specified in config file 
 * under the attribute name mapreduce.input.keyvaluelinerecordreader.key.value.separator. The default
 * separator is the tab character ('\t').
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CustomKeyValueLineRecordReader extends RecordReader<IntWritable, NullWritable> {
  public static final String KEY_VALUE_SEPERATOR = 
    "mapreduce.input.keyvaluelinerecordreader.key.value.separator";
  
  private final LineRecordReader lineRecordReader;

  private byte separator = (byte) '\t';

  private Text innerValue;

  private IntWritable key;
  
  private NullWritable value;
  
  public Class getKeyClass() { return IntWritable.class; }
  
  public CustomKeyValueLineRecordReader(Configuration conf)
    throws IOException {
    
    lineRecordReader = new LineRecordReader();
    String sepStr = conf.get(KEY_VALUE_SEPERATOR, "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  public void initialize(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    lineRecordReader.initialize(genericSplit, context);
  }
  
  public static int findSeparator(byte[] utf, int start, int length, 
      byte sep) {
    for (int i = start; i < (start + length); i++) {
      if (utf[i] == sep) {
        return i;
      }
    }
    return -1;
  }

  public static void setKeyValue(IntWritable key, NullWritable value, byte[] line,
      int lineLen, int pos) {
    if (pos == -1) {
      key.set(Integer.valueOf(new String(line, 0, lineLen)));
    } else {
      key.set(Integer.valueOf(new String(line, 0, pos)));
    }
  }
  /** Read key/value pair in a line. */
  public synchronized boolean nextKeyValue()
    throws IOException {
    byte[] line = null;
    int lineLen = -1;
    if (lineRecordReader.nextKeyValue()) {
      innerValue = lineRecordReader.getCurrentValue();
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      return false;
    }
    if (line == null)
      return false;
    if (key == null) {
      key = new IntWritable();
    }
    if (value == null) {
      value =  NullWritable.get();
    }
    int pos = findSeparator(line, 0, lineLen, this.separator);
    setKeyValue(key, value, line, lineLen, pos);
    return true;
  }
  
  public IntWritable getCurrentKey() {
    return key;
  }

  public NullWritable getCurrentValue() {
    return value;
  }

  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }
  
  public synchronized void close() throws IOException { 
    lineRecordReader.close();
  }
}
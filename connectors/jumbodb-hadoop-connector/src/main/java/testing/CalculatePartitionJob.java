package testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.integer.IntegerSamplingInputFormat;

import java.util.Arrays;


/**
 * Created by Carsten on 22.09.2014.
 */
public class CalculatePartitionJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new CalculatePartitionJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Configuration conf = getConf();
        Job instance = Job.getInstance(conf);
        instance.setNumReduceTasks(32);
        instance.setMapOutputKeyClass(IntWritable.class);
        instance.setInputFormatClass(IntegerSamplingInputFormat.class);
        instance.setOutputKeyClass(NullWritable.class);
        instance.getConfiguration().set(JumboConfigurationUtil.JUMBO_INDEX_JSON_CONF, mapper.writeValueAsString(new IndexField("blub", Arrays.asList("user.followers_count"), "egal", 4)));

        IntegerSamplingInputFormat.addInputPath(instance, new Path("file:///c:/Development/data/twitter/input_big/"));
        InputSampler.Sampler<Text, Text> sampler =
                new InputSampler.RandomSampler<Text, Text>(0.2, 1000000);
        Path partitionFile = new Path("file:///c:/Development/data/partition_small/partition.txt");
        // CATCH ArrayOutOf exception and use old partitioner
        TotalOrderPartitioner.setPartitionFile(instance.getConfiguration(), partitionFile);
//        partitionFile.getFileSystem(conf).create(partitionFile);
        InputSampler.writePartitionFile(instance, sampler);
        return 0;
    }
}

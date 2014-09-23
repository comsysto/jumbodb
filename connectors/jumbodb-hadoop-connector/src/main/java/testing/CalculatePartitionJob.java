package testing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


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
        Configuration conf = getConf();
        Job instance = Job.getInstance(conf);
        instance.setNumReduceTasks(64);
        instance.setMapOutputKeyClass(IntWritable.class);
        instance.setInputFormatClass(TestFileInputFormat.class);
        instance.setOutputKeyClass(IntWritable.class);
        KeyValueTextInputFormat.addInputPath(instance, new Path("file:///c:/Development/data/twitter_followers_count_big/"));
        InputSampler.Sampler<Text, Text> sampler =
                new InputSampler.RandomSampler<Text, Text>(0.1, 1000000);
        Path partitionFile = new Path("file:///c:/Development/data/partition_small/partition.txt");
        TotalOrderPartitioner.setPartitionFile(instance.getConfiguration(), partitionFile);
//        partitionFile.getFileSystem(conf).create(partitionFile);
        InputSampler.writePartitionFile(instance, sampler);
        return 0;
    }
}

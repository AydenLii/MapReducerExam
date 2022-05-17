package top.ayden.four;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class TestDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("hadoop.home.dir", "D:/Program Files/hadoop-2.7.6");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Mytest");
        job.setJarByClass(TestDriver.class);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("D:/Programming/Hadoop/input"));
        FileOutputFormat.setOutputPath(job, new Path("D:/Programming/Hadoop/output"));
        job.waitForCompletion(true);
    }
}

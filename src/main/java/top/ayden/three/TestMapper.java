package top.ayden.three;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text bigData, Context context) throws IOException, InterruptedException {
        String line = bigData.toString();
        String userName = line.split(",")[0];
        String date = line.split("date='")[1].split(":")[0];
        context.write(new Text(date), new Text(userName));
    }
}
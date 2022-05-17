package top.ayden.two;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text bigData, Context context) throws IOException, InterruptedException {

        String line = bigData.toString();
        String urlList = line.split(",")[1];
        String urlLast = urlList.substring(urlList.lastIndexOf('/') + 1);

        if (urlList.contains("苹果") && urlList.contains("search")) {

            context.write(new Text(urlLast), new IntWritable(1));

        }
    }

}
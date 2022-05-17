package top.ayden.six;

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
        String urlLast = urlList.substring(urlList.lastIndexOf('/'));

        if (urlList.contains("item")) {

            context.write(new Text("itemPage"), new IntWritable(1));

        }
    }

}
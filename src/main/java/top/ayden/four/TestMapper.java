package top.ayden.four;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;

public class TestMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text bigData, Context context) throws IOException, InterruptedException {

        String line = bigData.toString();
        String dev = line.split(",")[2].split("br=")[1].toLowerCase();
        String user = line.split(",")[0];

        if (dev.contains("android") || dev.contains("ios")) {

            context.write(new Text(dev), new Text(user));

        }
    }
}
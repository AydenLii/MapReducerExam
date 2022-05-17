package top.ayden.five;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text bigData, Context context) throws IOException, InterruptedException {
        String line = bigData.toString();
        String userName = line.split(",")[0];
        String allDateToSecond = line.split(",")[5].split("date=")[1].replace("\'", "");
        context.write(new Text(userName), new IntWritable(dateToSecond(allDateToSecond)));
    }


    public static int dateToSecond(String date) {
        int hour = Integer.parseInt(date.split(":")[1]) * 60 * 60;
        int minute = Integer.parseInt(date.split(":")[2]) * 60;
        int second = Integer.parseInt(date.split(":")[3]);
        return hour + minute + second;
    }
}

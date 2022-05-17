package top.ayden.seven;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text bigData, Context context) throws IOException, InterruptedException {
        String line = bigData.toString()``;
        String[] data=line.split(",");
        String[] urlList = data[1].split("/");
        String s = urlList[urlList.length-1];
        if (data[1].contains("苹果") && data[1].contains("item")){
            context.write(new Text("item-苹果"),new Text(s));
        }
        if (data[1].contains("可乐") && data[1].contains("item")){
            context.write(new Text("item-可乐"),new Text(s));
        }
        if (data[1].contains("维生素") && data[1].contains("item")){
            context.write(new Text("item-维生素"),new Text(s));
        }
    }
}
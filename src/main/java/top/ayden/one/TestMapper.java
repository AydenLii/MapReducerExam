package top.ayden.one;

<<<<<<< HEAD
import org.apache.hadoop.io.IntWritable;
=======
>>>>>>> 1fecd79168fe6abeb43f38c76af3aed2b2b4e473
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

<<<<<<< HEAD
public class TestMapper extends Mapper<LongWritable,Text, Text,Text> {
    @Override
    protected void map(LongWritable key, Text bigDate, Context context) throws IOException, InterruptedException {
        String line = bigDate.toString();
        String user = line.split(",")[0];
        String date = line.split("date='")[1].split(":")[0];
        context.write(new Text(date), new Text(user));
=======
public class TestMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //传入完整数据刘，转换为字符串形式
        String line = value.toString();
        //以","来切分字符串
        String[] words = line.split(",");
        //找到第5个单词并以=切分
        String[] FowInfo = words[5].split("=");
        //找到第二个单词以日期分割
        String[] Hello = FowInfo[1].split(":");
        //截取第一个字符串-第11个字符
        String k = Hello[0].substring(1, 11);
        //传入k=data，传入words[0]=user
        context.write(new Text(k), new Text(words[0]));
>>>>>>> 1fecd79168fe6abeb43f38c76af3aed2b2b4e473
    }
}
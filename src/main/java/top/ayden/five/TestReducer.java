package top.ayden.five;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;


public class TestReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text users, Iterable<IntWritable> allDateToSecond, Context context) throws IOException, InterruptedException {
        //定义一个列表,将所有的秒数传入并排序
        ArrayList<Integer> allSecondList = new ArrayList<>();
        for (IntWritable value : allDateToSecond) {
            allSecondList.add(value.get());
        }

        Collections.sort(allSecondList);

        int secondAvg = 0;
        for (Integer i = 0; i < allSecondList.size() - 1; i++) {
            secondAvg += allSecondList.get(i + 1) - allSecondList.get(i);
        }
        secondAvg = (secondAvg + 30) / allSecondList.size();

        context.write(new Text("<"+users+","), new Text(secondAvg+">"));

    }
}
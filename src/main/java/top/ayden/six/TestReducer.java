package top.ayden.six;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


public class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text objName, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // objName=itemPage, values=[1,1,1,1,1,1,1]
        int count=0;
        for (IntWritable value : values) {
            count += value.get();
        }

        context.write(new Text(objName), new IntWritable(count));
    }

}

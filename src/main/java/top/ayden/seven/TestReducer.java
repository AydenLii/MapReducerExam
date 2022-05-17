package top.ayden.seven;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class TestReducer extends Reducer<Text,Text, Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashSet<Text> a = new HashSet<>();

        for (Text value:values){

            a.add(value);

        }

        context.write(key, new Text(Integer.toString(a.size())));
    }
}

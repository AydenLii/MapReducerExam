package top.ayden.three;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


public class TestReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text date, Iterable<Text> users, Context context) throws IOException, InterruptedException {

        HashSet<String> uvList = new HashSet<>();

        int pvCount = 0;

        for (Text user : users) {
            pvCount++;
            uvList.add(user.toString());
        }

        context.write(new Text("<" + date + ","), new Text("pv-" + pvCount + "," + ",uv-" + uvList.size() + ">"));

    }

}
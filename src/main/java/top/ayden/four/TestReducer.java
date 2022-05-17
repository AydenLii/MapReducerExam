package top.ayden.four;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


public class TestReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text devKey, Iterable<Text> users, Context context) throws IOException, InterruptedException {

        HashSet<Text> userCount = new HashSet<>();

        for (Text user : users) {

            userCount.add(user);

        }

        context.write(new Text("<" + devKey + ","), new Text(userCount.size()+">"));

    }
}
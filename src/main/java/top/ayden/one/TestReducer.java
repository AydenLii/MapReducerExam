package top.ayden.one;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


<<<<<<< HEAD
public class TestReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text Date, Iterable<Text> users, Context context) throws IOException, InterruptedException {
        HashSet<String> hashSet = new HashSet<>();

        for (Text userValue : users) {
            hashSet.add(userValue.toString());

        }
        context.write(new Text(Date+"-"), new Text(String.valueOf(hashSet.size())));

=======
public class TestReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Text> hashSet=new HashSet<Text>();
        System.out.println(values);
        System.out.println(key);
        //循环遍历遍历名称为value的values数组
        for (Text value : values) {
            hashSet.add(value);
            System.out.println(value);
        }
        context.write(key,new Text(String.valueOf(hashSet.size())));
>>>>>>> 1fecd79168fe6abeb43f38c76af3aed2b2b4e473
    }
}

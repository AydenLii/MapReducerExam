package top.ayden.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class TestDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
<<<<<<< HEAD
        System.setProperty("hadoop.home.dir", "D:/Program Files/hadoop-2.7.6");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Mytest");
=======
        System.setProperty("hadoop.home.dir","D:/Program Files/hadoop-2.7.6");
        Configuration configuration= new Configuration();
        Job job = Job.getInstance(configuration,"Mytest");
>>>>>>> 1fecd79168fe6abeb43f38c76af3aed2b2b4e473
        job.setJarByClass(TestDriver.class);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
<<<<<<< HEAD
        FileInputFormat.setInputPaths(job, new Path("D:/Programming/Hadoop/input"));
        FileOutputFormat.setOutputPath(job, new Path("D:/Programming/Hadoop/output"));
        job.waitForCompletion(true);
    }
}
=======
        FileInputFormat.setInputPaths(job,new Path("D:/Programming/Hadoop/input"));
        FileOutputFormat.setOutputPath(job,new Path("D:/Programming/Hadoop/output"));
        job.waitForCompletion(true);
    }
}
>>>>>>> 1fecd79168fe6abeb43f38c76af3aed2b2b4e473

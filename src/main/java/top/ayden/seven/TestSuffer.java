package top.ayden.seven;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public  class TestSuffer extends Partitioner<Text,Text> {

    @Override
    public int getPartition(Text text, Text text2, int i) {

        if (text.toString().contains("苹果")){
            return 1;
        }
        if (text.toString().contains("可乐")){
            return 2;
        }
        if (text.toString().contains("维生素")){
            return 3;
        }
        else {
            return 0;
        }
    }
}

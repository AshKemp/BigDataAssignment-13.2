package mapreduce.advanced.demo.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, IntPair, Text> {
    private IntPair compositeKey = new IntPair();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

      String[] lineArray = line.split(",");
      String[] dateArray = lineArray[0].split("-");
      
      compositeKey.set(Integer.parseInt(dateArray[2]), Integer.parseInt(lineArray[2]));
      context.write(compositeKey, value);
  }
}
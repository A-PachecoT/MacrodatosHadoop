package SalesCountry;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int totalHogares = 0;
        while (values.hasNext()) {
            IntWritable value = (IntWritable) values.next();
            totalHogares += value.get();
        }
        output.collect(t_key, new IntWritable(totalHogares));
    }
}
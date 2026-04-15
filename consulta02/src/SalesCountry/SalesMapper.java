package SalesCountry;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;

        String[] data = value.toString().trim().split(";");
        if (data.length < 15) return;

        String departamento = data[1].trim().length() >= 2 ? data[1].trim().substring(0, 2) : "";
        if (departamento.isEmpty()) return;

        int pension65 = 0;
        int contigo   = 0;

        try { if (!data[11].trim().isEmpty()) pension65 = Integer.parseInt(data[11].trim()); } catch (NumberFormatException e) {}
        try { if (!data[14].trim().isEmpty()) contigo   = Integer.parseInt(data[14].trim()); } catch (NumberFormatException e) {}

        output.collect(new Text(departamento), new IntWritable(pension65 + contigo));
    }
}
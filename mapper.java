package ryosuke;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split("\\s");
		for (int i=0; i<words.length; i++)
		{
			String word = words[i];
			if (word.length() > 0) {
				context.write(new Text(words[i]), new IntWritable(1));
			}
		}
	}
}
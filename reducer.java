package ryosuke;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		Integer count = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext())
		{
			count++;
			iterator.next();
		}
		context.write(key, new IntWritable(count));
		
		// クラスター係数の計算
		String nodeinfo = key.toString();
		//HashMap a;
		context.write(key, new IntWritable(count));
		
	}
}
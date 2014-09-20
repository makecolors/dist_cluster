package ryosuke;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class cluster extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int runner = ToolRunner.run(new cluster(), args);
		System.exit(runner);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage:%s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		String inputDir = args[0];
		String outputDir = args[1];

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());

		// delete hdfs output files
		FileSystem fs = FileSystem.get(URI.create(outputDir), conf);
		fs.delete(new Path(outputDir), true);

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(mapper.class);
		//  job.setCombinerClass(reducer.class);
		job.setReducerClass(reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//  job.setNumMapTasks(1);
		job.setNumReduceTasks(1);

		//  job.setPartitionerClass(HashPartitioner.class);

		// hadoopの処理
		boolean result = job.waitForCompletion(true);

		return result ? 0 : 1;
	}
}
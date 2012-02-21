package simjoin.spatial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionToText extends Configured implements Tool {
	
	@SuppressWarnings("unchecked")
	public int dump(String path) throws IOException {
		Configuration conf = getConf();
		Path file = new Path(path);
		FileSystem fs = file.getFileSystem(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
		Class<? extends Writable> keyClass = (Class<? extends Writable>) reader.getKeyClass();
		
		Writable key = ReflectionUtils.newInstance(keyClass, conf);
		while (reader.next(key))
			System.out.println(key);
		
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length == 1)
			return dump(args[0]);
		
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(0);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PartitionToText(), args);
	}
}

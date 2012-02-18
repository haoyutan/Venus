package simjoin.spatial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simjoin.core.PartitionItems;
import simjoin.core.SimJoin;

public class SpatialJoinTest extends Configured implements Tool {
	
	private Configuration createSimJoinConf() {
		Configuration conf = new Configuration(getConf());
		SimJoin.setItemClass(conf, RegionItemWritable.class);
		SimJoin.setHandlerClass(conf, GridIndexJoinHandler.class);
		SimJoin.setHasSignature(conf, true);
		SimJoin.setOutputPayload(conf, true);
		return conf;
	}

	private Job createPartitionJob(Configuration conf, Path inputPath,
			Path outputPath) throws IOException {
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName("Step-1-Partition");
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		job.setNumReduceTasks(1);
		PartitionItems.configureJob(job);
		FileOutputFormat.setOutputPath(job, outputPath);

		GridIndexJoinHandler.setGridIndexFile(job.getConfiguration(), new Path(
				inputPath, GridIndexJoinHandler.DEFAULT_INDEX_DIRNAME));
		
		return job;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = createSimJoinConf();
		Job job = createPartitionJob(conf, new Path(args[0]), new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SpatialJoinTest(), args);
	}
}

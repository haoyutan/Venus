package simjoin.spatial;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simjoin.core.PartitionItems;
import simjoin.core.SimJoin;
import simjoin.core.SimJoinContext;

public class SpatialJoinTest extends Configured implements Tool {

	private Job createGridPartitionJob(Path inputPath, Path outputPath)
			throws IOException {
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setJobName("Step-1-Partition");

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		SimJoin.setItemClass(job, RegionItemWritable.class);
		SimJoin.setItemBuildHandlerClass(job, TextRegionItemBuildHandler.class);
		SimJoin.setHasSignature(job, true);
		SimJoin.setOutputPayload(job, true);
		SimJoin.setItemPartitionHandlerClass(job,
				GridRegionItemPartitionHandler.class);
		GridRegionItemPartitionHandler.setGridIndexFile(job.getConfiguration(),
				new Path(inputPath,
						GridRegionItemPartitionHandler.DEFAULT_INDEX_DIRNAME));

		PartitionItems.configureJob(job);
		
		return job;
	}

	public int run1(String[] args) throws Exception {
		Job job = createGridPartitionJob(new Path(args[0]), new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Job virtualJob = new Job(getConf());
		virtualJob.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(virtualJob, inputPath);
		virtualJob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(virtualJob, outputPath);
		
		SimJoinContext sjctx = new SimJoinContext(virtualJob);
		sjctx.setSimJoinName("Spatial Join");
		sjctx.setItemClass(RegionItemWritable.class);
		sjctx.setItemBuildHandlerClass(TextRegionItemBuildHandler.class, true);
		sjctx.setItemPartitionHandlerClass(GridRegionItemPartitionHandler.class);
		GridRegionItemPartitionHandler.setGridIndexFile(sjctx.getConf(),
				new Path(inputPath,
						GridRegionItemPartitionHandler.DEFAULT_INDEX_DIRNAME));
		sjctx.initialize();
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SpatialJoinTest(), args);
	}
}

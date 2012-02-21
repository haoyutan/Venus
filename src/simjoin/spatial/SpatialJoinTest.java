package simjoin.spatial;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simjoin.core.SimJoin;
import simjoin.core.SimJoinContext;

public class SpatialJoinTest extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Job virtualJob = new Job(getConf());
		virtualJob.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(virtualJob, inputPath);
		virtualJob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(virtualJob, outputPath);
		
		SimJoinContext sjCtx = new SimJoinContext(virtualJob);
		sjCtx.setSimJoinName("Spatial Join");
		sjCtx.setSimJoinWorkDir(outputPath);
		sjCtx.setItemClass(RegionItemWritable.class);
		sjCtx.setItemBuildHandlerClass(TextRegionItemBuildHandler.class, true);
		sjCtx.setItemPartitionHandlerClass(GridRegionItemPartitionHandler.class);
		GridRegionItemPartitionHandler.setGridIndexFile(sjCtx.getConf(),
				new Path(inputPath,
						GridRegionItemPartitionHandler.DEFAULT_INDEX_DIRNAME));
		
		SimJoin simjoin = new SimJoin(sjCtx);
		simjoin.run();
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SpatialJoinTest(), args);
	}
}

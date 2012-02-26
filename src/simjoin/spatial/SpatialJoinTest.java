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
import simjoin.core.SimJoinConf;

public class SpatialJoinTest extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Job dummyJob = new Job(getConf());
		dummyJob.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(dummyJob, inputPath);
		dummyJob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(dummyJob, outputPath);
		
		SimJoinConf sjConf = new SimJoinConf(dummyJob.getConfiguration());
		sjConf.setSimJoinName("Spatial Join");
		sjConf.setWorkDir(new Path(outputPath, "_simjoin"));
		sjConf.setItemClass(RegionItemWritable.class);
		sjConf.setItemBuildHandlerClass(TextRegionItemBuildHandler.class, true);
		sjConf.setItemPartitionHandlerClass(GridRegionItemPartitionHandler.class);
		GridRegionItemPartitionHandler.setGridIndexFile(sjConf,
				new Path(inputPath,
						GridRegionItemPartitionHandler.DEFAULT_INDEX_DIRNAME));
		
		sjConf.setSimJoinAlgorithm(SimJoinConf.CV_ALGO_CLONE);
		sjConf.setClusterTaskSlots(3);
		ToolRunner.run(new SimJoin(sjConf), args);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SpatialJoinTest(), args);
	}
}

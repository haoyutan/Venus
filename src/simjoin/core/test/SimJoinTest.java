package simjoin.core.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simjoin.core.PartitionItemsMapper;
import simjoin.spatial.RegionItemWritable;
import simjoin.spatial.VirtualTextRegionInputFormat;

public class SimJoinTest extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("simjoin.core.simjoinhandler.class",
				"simjoin.spatial.TextInputRegionIntersectJoinHandler");
		conf.setBoolean("simjoin.core.has_signature", true);
		conf.setBoolean("simjoin.core.output_payload", true);
		
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());
		
		job.setInputFormatClass(VirtualTextRegionInputFormat.class);
		job.setMapperClass(PartitionItemsMapper.class);
		job.setMapOutputKeyClass(RegionItemWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SimJoinTest(), args);
	}
}

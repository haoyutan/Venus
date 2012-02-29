package simjoin.core.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import simjoin.core.SimJoinConf;

public class DeduplicateItemPairs extends BaseTask {
	
	private static final Log LOG = LogFactory.getLog(DeduplicateItemPairs.class);
	
	public DeduplicateItemPairs(Configuration conf) {
		super(conf);
	}
	
	@Override
	protected int runTask(String[] args) throws Exception {
		LOG.info("FIXME");
		Configuration conf = getConf();

		// remove output directory
		FileSystem fs = taskOutputPath.getFileSystem(conf);
		fs.delete(taskOutputPath, true);

		// execute job
		Job job = new Job(conf);
		String simJoinName = SimJoinConf.getSimJoinName(conf);
		job.setJobName(simJoinName + "-" + getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, taskInputPath);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, taskOutputPath);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}

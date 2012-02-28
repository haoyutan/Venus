package simjoin.core.exec;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;
import simjoin.core.SimJoinUtils;
import simjoin.core.handler.ItemJoinHandler;
import simjoin.core.partition.VirtualPartition;
import simjoin.core.partition.VirtualPartitionID;
import simjoin.core.partition.VirtualPartitionInfo;
import simjoin.core.partition.VirtualPartitionInputFormat;
import simjoin.core.partition.VirtualPartitionReader;

public class PartitionJoin extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(PartitionJoin.class);
	
	public static final String CK_TASKSCHEDULE_DIR = "simjoin.core.partition.task_schedule_dir";
	
	@SuppressWarnings("rawtypes")
	public static class PartitionJoinMapper
			extends
			Mapper<VirtualPartition, VirtualPartition, ItemWritable, ItemWritable> {
		
		private ItemJoinHandler<?> itemJoinHandler;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			itemJoinHandler = SimJoinUtils.createItemJoinHandler(conf);
			itemJoinHandler.setup(context);
			itemJoinHandler.setItemOutputMask(ItemWritable.MASK_ID);
		}

		@Override
		protected void map(VirtualPartition key, VirtualPartition value,
				Context context) throws IOException, InterruptedException {
			context.setStatus(key.getVirtualPartitionInfo() + ","
					+ value.getVirtualPartitionInfo());
			itemJoinHandler.joinItem(key, value);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			itemJoinHandler.cleanup(context);
			super.cleanup(context);
		}
	}
	
	private Path workDir;
	
	private Path taskScheduleDir;
	
	public PartitionJoin(Configuration conf) {
		setConf(conf);
		workDir = SimJoinConf.getWorkDir(conf);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		// remove output directory
		workDir = SimJoinConf.getWorkDir(conf);
		FileSystem fs = workDir.getFileSystem(conf);
		fs.delete(workDir, true);
		
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setInputFormatClass(VirtualPartitionInputFormat.class);
		VirtualPartitionInputFormat.setInputDir(job,
				new Path(conf.get(CK_TASKSCHEDULE_DIR)));
		job.setMapperClass(PartitionJoinMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, workDir);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	// FIXME: This method is for debug purpose and can be safely removed.
	@SuppressWarnings({ "rawtypes", "unused" })
	private void testVirtualPartitionReader() throws Exception {
		Configuration conf = getConf();
		taskScheduleDir = new Path(conf.get(CK_TASKSCHEDULE_DIR));
		LOG.info("Task schedule dir: " + taskScheduleDir);

		Map<VirtualPartitionID, VirtualPartitionInfo> vpInfoMap = VirtualPartitionInfo
				.readVirtualPartitionInfo(conf, taskScheduleDir, true);

		long totalCount = 0;
		for (VirtualPartitionID id : vpInfoMap.keySet()) {
			if (id.getMainId() != 0)
				continue;
			VirtualPartitionInfo info = vpInfoMap.get(id);
			VirtualPartitionReader reader = new VirtualPartitionReader(conf, info);
			long count = 0;
			while (reader.nextItem()) {
				++count;
				ItemWritable item = reader.getCurrentItem();
				LOG.info(item);
			}
			LOG.info(id.toString() + ": " + count);
			totalCount += count;
		}
		LOG.info("Total: " + totalCount);
	}
}

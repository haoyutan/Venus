package simjoin.core.exec;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;
import simjoin.core.SimJoinUtils;
import simjoin.core.handler.ItemJoinHandler;
import simjoin.core.partition.VirtualPartition;
import simjoin.core.partition.VirtualPartitionID;
import simjoin.core.partition.VirtualPartitionInfo;
import simjoin.core.partition.VirtualPartitionInputFormat;
import simjoin.core.partition.VirtualPartitionReader;

public class PartitionJoin extends BaseTask {
	
	private static final Log LOG = LogFactory.getLog(PartitionJoin.class);
	
	public static final String CK_JOIN_TYPE = "simjoin.core.exec.partitionjoin.join_type";
	public static final int CV_JOIN_TYPE_SIG_PAYLOAD = 0;
	public static final int CV_JOIN_TYPE_SIG = 1;
	public static final int CV_JOIN_TYPE_PAYLOAD = 2;
	
	@SuppressWarnings("rawtypes")
	public static class PartitionJoinMapper
			extends
			Mapper<VirtualPartition, VirtualPartition, ItemWritable, ItemWritable> {
		
		private ItemJoinHandler<?> itemJoinHandler;
		
		private int joinType;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			joinType = conf.getInt(CK_JOIN_TYPE, CV_JOIN_TYPE_SIG_PAYLOAD);
			LOG.info("Join type: " + joinType);
			itemJoinHandler = SimJoinUtils.createItemJoinHandler(conf);
			itemJoinHandler.setup(context);
			itemJoinHandler.setItemOutputMask(ItemWritable.MASK_ID);
		}

		@Override
		protected void map(VirtualPartition key, VirtualPartition value,
				Context context) throws IOException, InterruptedException {
			context.setStatus(key.getVirtualPartitionInfo() + ","
					+ value.getVirtualPartitionInfo());
			switch (joinType) {
			case CV_JOIN_TYPE_SIG_PAYLOAD:
				itemJoinHandler.joinItem(key, value);
				break;
			case CV_JOIN_TYPE_SIG:
				itemJoinHandler.joinSignature(key, value);
				break;
			case CV_JOIN_TYPE_PAYLOAD:
				itemJoinHandler.joinPayload(key, value);
				break;
			default:
				throw new RuntimeException("Wrong join type: " + joinType);
			}
			LOG.info("Finished partition pair: "
					+ key.getVirtualPartitionInfo().getId() + ","
					+ value.getVirtualPartitionInfo().getId());
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			itemJoinHandler.cleanup(context);
			super.cleanup(context);
		}
	}
	
	public PartitionJoin(Configuration conf) {
		super(conf);
	}
	
	@Override
	protected int runTask(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = taskOutputPath.getFileSystem(conf);
		fs.delete(taskOutputPath, true);
		return runJob();
	}
	
	@SuppressWarnings("rawtypes")
	private int runJob() throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		String simJoinName = SimJoinConf.getSimJoinName(conf);
		job.setJobName(simJoinName + "-" + getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(VirtualPartitionInputFormat.class);
		VirtualPartitionInputFormat.setInputDir(job, taskInputPath);
		job.setMapperClass(PartitionJoinMapper.class);
		job.setNumReduceTasks(0);
		
		Class<? extends ItemWritable> itemClass = SimJoinConf
				.getItemClass(conf);
		job.setOutputKeyClass(itemClass);
		job.setOutputValueClass(itemClass);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, taskOutputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job,
				CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job,
				SnappyCodec.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	// FIXME: This method is for debug purpose and can be safely removed.
	@SuppressWarnings({ "rawtypes", "unused" })
	private void testVirtualPartitionReader() throws Exception {
		Configuration conf = getConf();
		LOG.info("Task input dir: " + taskInputPath);

		Map<VirtualPartitionID, VirtualPartitionInfo> vpInfoMap = VirtualPartitionInfo
				.readVirtualPartitionInfo(conf, taskInputPath, true);

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

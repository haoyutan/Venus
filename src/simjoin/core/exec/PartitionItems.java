package simjoin.core.exec;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;
import simjoin.core.SimJoinUtils;
import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;
import simjoin.core.partition.PartitionID;
import simjoin.core.partition.VirtualPartitionID;
import simjoin.core.partition.VirtualPartitionInfo;

public class PartitionItems extends BaseTask {
	
	private static final Log LOG = LogFactory.getLog(PartitionItems.class);

	public static final String CK_OUTPUT_PAYLOAD = "simjoin.core.exec.partitionitems.output_payload";
	
	private static final String PARTITION__FILENAME_PREFIX = "P-";
	
	@SuppressWarnings({ "rawtypes" })
	private void configureJob(Job job) {
		Configuration conf = job.getConfiguration();
		Class<? extends ItemWritable> itemClass = SimJoinConf.getItemClass(conf);
		if (itemClass == null)
			throw new RuntimeException("Must specify item class.");

		job.setJarByClass(getClass());
		
		job.setMapperClass(PartitionItemsMapper.class);
		job.setMapOutputKeyClass(PartitionID.class);
		job.setMapOutputValueClass(itemClass);
		
		job.setReducerClass(PartitionItemsReducer.class);
		job.setNumReduceTasks(SimJoinConf.getClusterTaskSlots(conf) * 2);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(itemClass);
		job.setOutputValueClass(NullWritable.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job,
				CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job,
				SnappyCodec.class);
		MultipleOutputs.addNamedOutput(job, "StatNumRecords",
				SequenceFileOutputFormat.class, PartitionID.class,
				LongWritable.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static class PartitionItemsMapper<KEYIN, VALUEIN> extends
			Mapper<KEYIN, VALUEIN, PartitionID, ItemWritable> {

		private boolean hasSignature;

		private boolean outputPayload;

		private int mask;

		private ItemBuildHandler<KEYIN, VALUEIN, ?> itemBuildHandler;
		
		private ItemPartitionHandler<?> itemPartitionHandler;
		
		private ItemWritable item;

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
			Configuration conf = context.getConfiguration();
			
			hasSignature = SimJoinConf.hasSignature(conf);
			outputPayload = true;
			if (hasSignature)
				outputPayload = conf.getBoolean(CK_OUTPUT_PAYLOAD, false);

			mask = ItemWritable.MASK_ID;
			if (outputPayload)
				mask |= ItemWritable.MASK_PLD;
			if (hasSignature)
				mask |= ItemWritable.MASK_SIG;
			
			itemBuildHandler = SimJoinUtils.createItemBuildHandler(conf);
			itemBuildHandler.setup(context);
			item = itemBuildHandler.createItem();
			
			itemPartitionHandler = SimJoinUtils.createItemPartitionHandler(conf);
			itemPartitionHandler.setup(context);
		}

		@Override
		protected void map(KEYIN key, VALUEIN value, Context context)
				throws IOException, InterruptedException {
			itemBuildHandler.resetItem(item, key, value);
			item.setMask(mask);
			
			List<PartitionID> pids = itemPartitionHandler.getPartitions(item);
			for (PartitionID pid : pids)
				context.write(pid, item);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			itemPartitionHandler.cleanup(context);
			itemBuildHandler.cleanup(context);
			super.cleanup(context);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class PartitionItemsReducer<VALUEIN extends ItemWritable, VALUEOUT extends ItemWritable>
			extends Reducer<PartitionID, VALUEIN, VALUEOUT, NullWritable> {
		
		MultipleOutputs mos;

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			mos = new MultipleOutputs(context);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void reduce(PartitionID key, Iterable<VALUEIN> values,
				Context context) throws IOException, InterruptedException {
			String partitionName = PARTITION__FILENAME_PREFIX + key.toString();
			long count = 0;
			for (VALUEIN value : values) {
				mos.write(value, NullWritable.get(), partitionName);
				++count;
			}
			mos.write("StatNumRecords", key, new LongWritable(count));
			context.getCounter("PartitionItems Count", "ITEM_COUNT").increment(count);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
		}
	}
	
	public PartitionItems(Configuration conf) {
		super(conf);
	}

	@Override
	protected int runTask(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = taskOutputPath.getFileSystem(conf);
		fs.delete(taskOutputPath, true);
		int ret = runJob();
		if (ret == 0)
			createPartitionSummary();
		return ret;
	}
	
	private int runJob() throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		String simJoinName = SimJoinConf.getSimJoinName(conf);
		job.setJobName(simJoinName + "-" + getClass().getSimpleName());
		FileOutputFormat.setOutputPath(job, taskOutputPath);
		configureJob(job);
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	private void createPartitionSummary() throws IOException {
		LOG.info("Creating summary file...");
		Configuration conf = getConf();
		FileSystem fs = taskOutputPath.getFileSystem(conf);

		Map<VirtualPartitionID, VirtualPartitionInfo> vpInfoMap = 
				new HashMap<VirtualPartitionID, VirtualPartitionInfo>();
		
		// get path and size of each partition file
		FileStatus[] partitionFileStatus = fs.listStatus(taskOutputPath,
				new PrefixPathFilter(PARTITION__FILENAME_PREFIX));
		int prefixLength = PARTITION__FILENAME_PREFIX.length();
		for (FileStatus status : partitionFileStatus) {
			String filename = status.getPath().getName();
			VirtualPartitionID vpId = VirtualPartitionID
					.createFromString(filename.substring(prefixLength));
			VirtualPartitionInfo vpInfo = new VirtualPartitionInfo(vpId);
			vpInfo.setLength(status.getLen());
			vpInfo.setPartitionFile(new Path(status.getPath().getName()));
			vpInfoMap.put(vpId, vpInfo);
		}
		LOG.info("  Partition file path and size loaded.");

		// get the number of records in each partition
		FileStatus[] metaFileStatus = fs.listStatus(taskOutputPath,
				new PrefixPathFilter("StatNumRecords-"));
		for (FileStatus status : metaFileStatus) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs,
					status.getPath(), conf);
			PartitionID key = new PartitionID();
			LongWritable value = new LongWritable();
			while (reader.next(key, value))
				vpInfoMap.get(key).setNumRecords(value.get());
			reader.close();
		}
		LOG.info("  Number of records loaded.");

		VirtualPartitionInfo.writeVirtualPartitionInfo(conf, taskOutputPath,
				vpInfoMap.values(), true);
		LOG.info("  Summary file written.");
		
		// remove useless files (StatNumRecords-* and part-r-*)
		for (FileStatus status : metaFileStatus)
			fs.delete(status.getPath(), true);
		FileStatus[] partFileStatus = fs.listStatus(taskOutputPath,
				new PrefixPathFilter("part-r-"));
		for (FileStatus status : partFileStatus)
			fs.delete(status.getPath(), true);
		LOG.info("  Useless files removed.");
		
		LOG.info("Creating summary file... Done.");
	}
	
	private static class PrefixPathFilter implements PathFilter {
		
		private String prefix;
		
		public PrefixPathFilter(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith(prefix);
		}
	}
}

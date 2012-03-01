package simjoin.core.exec;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;
import simjoin.core.SimJoinUtils;
import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemJoinHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class RefinementJoin extends BaseTask {

	@SuppressWarnings("rawtypes")
	public static class RefinementJoinOriginalItemsMapper<KEYIN, VALUEIN> extends
			Mapper<KEYIN, VALUEIN, ItemWritable, ItemWritable> {

		private ItemBuildHandler<KEYIN, VALUEIN, ?> itemBuildHandler;

		private ItemPartitionHandler<?> itemPartitionHandler;

		private ItemWritable item, itemIdOnly;

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);
			Configuration conf = context.getConfiguration();

			itemBuildHandler = SimJoinUtils.createItemBuildHandler(conf);
			itemBuildHandler.setup(context);
			item = itemBuildHandler.createItem();
			itemIdOnly = itemBuildHandler.createItem();

			itemPartitionHandler = SimJoinUtils
					.createItemPartitionHandler(conf);
			itemPartitionHandler.setup(context);
		}

		@Override
		protected void map(KEYIN key, VALUEIN value, Context context)
				throws IOException, InterruptedException {
			itemBuildHandler.resetItem(item, key, value);
			ItemWritable.copy(context.getConfiguration(), item, itemIdOnly,
					ItemWritable.MASK_ID);
			itemIdOnly.setTag(true);
			context.write(itemIdOnly, item);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			itemPartitionHandler.cleanup(context);
			itemBuildHandler.cleanup(context);
			super.cleanup(context);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class RefinementJoinPhaseAReducer extends
			Reducer<ItemWritable, ItemWritable, ItemWritable, ItemWritable> {
		
		@Override
		protected void reduce(ItemWritable key, Iterable<ItemWritable> values,
				Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Iterator<ItemWritable> itor = values.iterator();
			ItemWritable origItem = ItemWritable.clone(conf, itor.next());
			while (itor.hasNext())
				context.write(itor.next(), origItem);
//			for (ItemWritable value : values)
//				context.write(key, value);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class RefinementJoinPhaseBReducer extends
			Reducer<ItemWritable, ItemWritable, ItemWritable, ItemWritable> {
		
		private ItemJoinHandler<?> itemJoinHandler;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			itemJoinHandler = SimJoinUtils.createItemJoinHandler(context
					.getConfiguration());
			itemJoinHandler.setup(context);
			itemJoinHandler.setItemOutputMask(ItemWritable.MASK_ID);
		}

		@Override
		protected void reduce(ItemWritable key, Iterable<ItemWritable> values,
				Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Iterator<ItemWritable> itor = values.iterator();
			ItemWritable left = ItemWritable.clone(conf, itor.next());
			while (itor.hasNext()) {
				ItemWritable right = itor.next();
				if (itemJoinHandler.acceptPayloadPair(left, right))
					context.write(left, right);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			itemJoinHandler.cleanup(context);
			super.cleanup(context);
		}
	}
	
	private Path phaseAOutputPath, phaseBOutputPath;
	
	public RefinementJoin(Configuration conf) {
		super(conf);
	}
	
	@SuppressWarnings("rawtypes")
	private Job createPhaseAJob() throws IOException, ClassNotFoundException {
		Configuration conf = getConf();
		String simJoinName = SimJoinConf.getSimJoinName(conf);
		Job job = new Job(conf);
		job.setJobName(simJoinName + "-" + getClass().getSimpleName()
				+ "-Phase-A");
		job.setJarByClass(getClass());

		Path origInputPath = FileInputFormat.getInputPaths(job)[0];
		MultipleInputs.addInputPath(job, origInputPath,
				job.getInputFormatClass(),
				RefinementJoinOriginalItemsMapper.class);
		MultipleInputs.addInputPath(job, taskInputPath,
				SequenceFileInputFormat.class, Mapper.class);

		Class<? extends ItemWritable> itemClass = SimJoinConf
				.getItemClass(conf);
		job.setMapOutputKeyClass(itemClass);
		job.setMapOutputKeyClass(itemClass);
		job.setSortComparatorClass(ItemWritable.ItemIDTagComparator.class);

		job.setReducerClass(RefinementJoinPhaseAReducer.class);
		job.setNumReduceTasks(SimJoinConf.getClusterTaskSlots(conf) * 2);
		job.setNumReduceTasks(1);
		job.setGroupingComparatorClass(ItemWritable.ItemIDComparator.class);
		job.setOutputKeyClass(itemClass);
		job.setOutputValueClass(itemClass);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, phaseAOutputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job,
				CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job,
				SnappyCodec.class);
		
		return job;
	}
	
	@SuppressWarnings("rawtypes")
	private Job createPhaseBJob() throws IOException, ClassNotFoundException {
		Configuration conf = getConf();
		String simJoinName = SimJoinConf.getSimJoinName(conf);
		Job job = new Job(conf);
		job.setJobName(simJoinName + "-" + getClass().getSimpleName()
				+ "-Phase-B");
		job.setJarByClass(getClass());

		Path origInputPath = FileInputFormat.getInputPaths(job)[0];
		MultipleInputs.addInputPath(job, origInputPath,
				job.getInputFormatClass(),
				RefinementJoinOriginalItemsMapper.class);
		MultipleInputs.addInputPath(job, phaseAOutputPath,
				SequenceFileInputFormat.class, Mapper.class);

		Class<? extends ItemWritable> itemClass = SimJoinConf
				.getItemClass(conf);
		job.setMapOutputKeyClass(itemClass);
		job.setMapOutputKeyClass(itemClass);
		job.setSortComparatorClass(ItemWritable.ItemIDTagComparator.class);

		job.setReducerClass(RefinementJoinPhaseBReducer.class);
		job.setNumReduceTasks(SimJoinConf.getClusterTaskSlots(conf) * 2);
		job.setGroupingComparatorClass(ItemWritable.ItemIDComparator.class);
		job.setOutputKeyClass(itemClass);
		job.setOutputValueClass(itemClass);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, phaseBOutputPath);
		
		return job;
	}
	
	@Override
	protected int runTask(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = taskOutputPath.getFileSystem(conf);
		fs.delete(taskOutputPath, true);
		
		phaseAOutputPath = new Path(taskOutputPath, "Phase-A");
		phaseBOutputPath = new Path(taskOutputPath, "Phase-B");
		
		int ret = -1;
		
		Job phaseAJob = createPhaseAJob();
		ret = phaseAJob.waitForCompletion(true) ? 0 : 1;
		if (ret != 0)
			return ret;
		
		Job phaseBJob = createPhaseBJob();
		ret = phaseBJob.waitForCompletion(true) ? 0 : 1;
		if (ret != 0)
			return ret;
		
		return 0;
	}
}

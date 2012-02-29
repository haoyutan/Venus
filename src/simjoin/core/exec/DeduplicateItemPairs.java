package simjoin.core.exec;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;

public class DeduplicateItemPairs extends BaseTask {

	@SuppressWarnings("rawtypes")
	public static class DeduplicateItemPairsReducer extends
			Reducer<ItemWritable, ItemWritable, ItemWritable, ItemWritable> {
		
		@SuppressWarnings("unused")
		private static class ItemIDCompWrapper {
			
			private ItemWritable item;

			public ItemIDCompWrapper(ItemWritable item) {
				this.item = item;
			}
			
			public ItemWritable getItem() {
				return item;
			}

			public void setItem(ItemWritable item) {
				this.item = item;
			}

			@Override
			public int hashCode() {
				return item.getId().hashCode();
			}

			@Override
			public boolean equals(Object obj) {
				return item.getId().equals(
						((ItemIDCompWrapper) obj).getItem().getId());
			}
		}

		@Override
		protected void reduce(ItemWritable key, Iterable<ItemWritable> values,
				Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Set<ItemIDCompWrapper> set = new HashSet<ItemIDCompWrapper>();
			for (ItemWritable value : values)
				set.add(new ItemIDCompWrapper(WritableUtils.clone(value, conf)));
			for (ItemIDCompWrapper wrapper : set)
				context.write(key, wrapper.getItem());
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class DeduplicateItemPairsPartitioner extends Partitioner<ItemWritable, ItemWritable> {

		@Override
		public int getPartition(ItemWritable key, ItemWritable value,
				int numPartitions) {
			return Math.abs(key.getId().hashCode() * 997) % numPartitions;
		}
	}
	
	public static class ItemIDComparator extends WritableComparator {

		public ItemIDComparator() {
			super(ItemWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	public DeduplicateItemPairs(Configuration conf) {
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
		Class<? extends ItemWritable> itemClass = SimJoinConf
				.getItemClass(conf);
		Job job = new Job(conf);
		String simJoinName = SimJoinConf.getSimJoinName(conf);
		job.setJobName(simJoinName + "-" + getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, taskInputPath);
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(itemClass);
		job.setMapOutputValueClass(itemClass);
		job.setPartitionerClass(DeduplicateItemPairsPartitioner.class);
		job.setSortComparatorClass(ItemIDComparator.class);

		job.setReducerClass(DeduplicateItemPairsReducer.class);
		job.setGroupingComparatorClass(ItemIDComparator.class);
		job.setNumReduceTasks(SimJoinConf.getClusterTaskSlots(conf) * 2);
		job.setOutputKeyClass(itemClass);
		job.setOutputValueClass(itemClass);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, taskOutputPath);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}

package simjoin.core.exec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;

public class DeduplicateItemPairs extends BaseTask {

	private static final Log LOG = LogFactory
			.getLog(DeduplicateItemPairs.class);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static class ItemPairWritable implements WritableComparable {
		
		private ItemWritable first;
		
		private ItemWritable second;

		public ItemWritable getFirst() {
			return first;
		}

		public void setFirst(ItemWritable first) {
			this.first = first;
		}

		public ItemWritable getSecond() {
			return second;
		}

		public void setSecond(ItemWritable second) {
			this.second = second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}

		@Override
		public int compareTo(Object o) {
			ItemPairWritable other = (ItemPairWritable) o;
			int cmp = this.getFirst().getId()
					.compareTo(other.getFirst().getId());
			if (cmp == 0)
				cmp = this.getSecond().getId()
						.compareTo(other.getSecond().getId());
			return cmp;
		}

		@Override
		public int hashCode() {
			return first.getId().hashCode() * 163 + second.getId().hashCode();
		}

		@Override
		public String toString() {
			return first.toString() + ",," + second.toString();
		}
	}

	@SuppressWarnings("rawtypes")
	public static class DeduplicateItemPairsMapper extends
			Mapper<ItemWritable, ItemWritable, ItemPairWritable, NullWritable> {
		
		private ItemPairWritable itemPair = new ItemPairWritable();

		@Override
		protected void map(ItemWritable key, ItemWritable value, Context context)
				throws IOException, InterruptedException {
			itemPair.setFirst(key);
			itemPair.setFirst(value);
			context.write(itemPair, NullWritable.get());
		}
	}

	@SuppressWarnings("rawtypes")
	public static class DeduplicateItemPairsReducer extends
			Reducer<ItemPairWritable, NullWritable, ItemWritable, ItemWritable> {

		@Override
		protected void reduce(ItemPairWritable key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key.getFirst(), key.getSecond());
		}
	}

	public DeduplicateItemPairs(Configuration conf) {
		super(conf);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected int runTask(String[] args) throws Exception {
		LOG.info("FIXME AGAIN");
		Configuration conf = getConf();

		// remove output directory
		FileSystem fs = taskOutputPath.getFileSystem(conf);
		fs.delete(taskOutputPath, true);

		Class<? extends ItemWritable> itemClass = SimJoinConf
				.getItemClass(conf);

		// execute job
		Job job = new Job(conf);
		String simJoinName = SimJoinConf.getSimJoinName(conf);
		job.setJobName(simJoinName + "-" + getClass().getSimpleName());
		job.setJarByClass(getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, taskInputPath);
		job.setMapperClass(DeduplicateItemPairsMapper.class);
		job.setMapOutputKeyClass(ItemPairWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(DeduplicateItemPairsReducer.class);
		job.setNumReduceTasks(SimJoinConf.getClusterTaskSlots(conf) * 2);
		job.setOutputKeyClass(itemClass);
		job.setOutputValueClass(itemClass);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, taskOutputPath);

		int ret = (job.waitForCompletion(true) ? 0 : 1);
		ret = 1;
		return ret;
	}
}

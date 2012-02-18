package simjoin.core;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import simjoin.spatial.RegionItemWritable;

public class PartitionItems {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void configureJob(Job job) {
		Configuration conf = job.getConfiguration();
		Class<? extends ItemWritable> itemClass = (Class<? extends ItemWritable>) conf
				.getClass(SimJoin.CONF_ITEM_CLASS, null);
		if (itemClass == null)
			throw new RuntimeException("Must specify item class");
		
		job.setMapperClass(PartitionItemsMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(itemClass);
		
		job.setReducerClass(PartitionItemsReducer.class);
		MultipleOutputs.addNamedOutput(job, "dummy",
				TextOutputFormat.class, RegionItemWritable.class,
				NullWritable.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static class PartitionItemsMapper<KEYIN, VALUEIN> extends
			Mapper<KEYIN, VALUEIN, IntWritable, ItemWritable> {

		private boolean hasSignature;

		private boolean outputPayload;

		private int mask;

		private SimJoinHandler<KEYIN, VALUEIN, ?> handler;

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
			Configuration conf = context.getConfiguration();
			
			hasSignature = conf.getBoolean(SimJoin.CONF_HAS_SIG, false);
			outputPayload = true;
			if (hasSignature)
				outputPayload = conf.getBoolean(SimJoin.CONF_OUTPUT_PLD, false);

			mask = ItemWritable.MASK_ID;
			if (outputPayload)
				mask |= ItemWritable.MASK_PLD;
			if (hasSignature)
				mask |= ItemWritable.MASK_SIG;
			
			handler = SimJoin.createHandler(conf);
			handler.setupBuildItem(conf);
			handler.setupGetPartitions(conf);
		}

		@Override
		protected void map(KEYIN key, VALUEIN value, Context context)
				throws IOException, InterruptedException {
			ItemWritable item = handler.buildItem(key, value);
			item.setMask(mask);
			
			List<Integer> pids = handler.getPartitions(item);
			for (Integer pid : pids)
				context.write(new IntWritable(pid), item);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			handler.cleanupBuildItem(conf);
			handler.cleanupGetPartitions(conf);
			super.cleanup(context);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static class PartitionItemsReducer<VALUEIN extends ItemWritable, VALUEOUT extends ItemWritable>
			extends Reducer<IntWritable, VALUEIN, VALUEOUT, NullWritable> {
		
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
		protected void reduce(IntWritable key, Iterable<VALUEIN> values,
				Context context) throws IOException, InterruptedException {
			for (VALUEIN value : values)
				mos.write(value, NullWritable.get(), "P-" + key.get());
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
		}
	}
}

package simjoin.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

@SuppressWarnings("rawtypes")
public class PartitionItemsMapper<KEYIN, VALUEIN> extends
		Mapper<KEYIN, VALUEIN, ItemWritable, NullWritable> {

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
		hasSignature = conf.getBoolean("simjoin.core.has_signature", false);
		outputPayload = true;
		if (hasSignature)
			outputPayload = conf.getBoolean("simjoin.core.output_payload",
					false);

		mask = ItemWritable.MASK_ID;
		if (outputPayload)
			mask |= ItemWritable.MASK_PLD;
		if (hasSignature)
			mask |= ItemWritable.MASK_SIG;

		Class<?> handlerClass = conf.getClass(
				"simjoin.core.simjoinhandler.class", null);
		try {
			handler = (SimJoinHandler<KEYIN, VALUEIN, ?>) handlerClass
					.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void map(KEYIN key, VALUEIN value, Context context)
			throws IOException, InterruptedException {
		ItemWritable item = handler.buildItem(key, value);
		item.setMask(mask);
		context.write(item, NullWritable.get());
	}
}
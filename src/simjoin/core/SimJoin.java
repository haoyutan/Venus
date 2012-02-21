package simjoin.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class SimJoin {
	
	public static final String CONF_HANDLER_ITEMBUILD_CLASS = "simjoin.core.handler.itembuild.class";
	public static final String CONF_HANDLER_ITEMPARTITION_CLASS = "simjoin.core.handler.itempartition.class";
	public static final String CONF_ITEM_CLASS = "simjoin.core.item.class";
	public static final String CONF_HAS_SIG = "simjoin.core.has_signature";
	public static final String CONF_OUTPUT_PLD = "simjoin.core.output_payload";

	@SuppressWarnings("rawtypes")
	public static void setItemClass(Job job,
			Class<? extends ItemWritable> itemClass) {
		job.getConfiguration().setClass(CONF_ITEM_CLASS, itemClass,
				ItemWritable.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static void setItemBuildHandlerClass(Job job,
			Class<? extends ItemBuildHandler> handlerClass) {
		job.getConfiguration().setClass(CONF_HANDLER_ITEMBUILD_CLASS,
				handlerClass, ItemBuildHandler.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static void setItemPartitionHandlerClass(Job job,
			Class<? extends ItemPartitionHandler> handlerClass) {
		job.getConfiguration().setClass(CONF_HANDLER_ITEMPARTITION_CLASS,
				handlerClass, ItemPartitionHandler.class);
	}
	
	public static void setHasSignature(Job job, boolean hasSignature) {
		job.getConfiguration().setBoolean(CONF_HAS_SIG, hasSignature);
	}
	
	public static void setOutputPayload(Job job, boolean outputPayload) {
		job.getConfiguration().setBoolean(CONF_OUTPUT_PLD, outputPayload);
	}
	
	@SuppressWarnings("rawtypes")
	public static ItemBuildHandler createItemBuildHandler(Configuration conf) {
		ItemBuildHandler handler;
		Class<?> handlerClass = conf.getClass(
				SimJoin.CONF_HANDLER_ITEMBUILD_CLASS, null);
		try {
			handler = (ItemBuildHandler) ReflectionUtils.newInstance(
					handlerClass, conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return handler;
	}
	
	@SuppressWarnings("rawtypes")
	public static ItemPartitionHandler createItemPartitionHandler(Configuration conf) {
		ItemPartitionHandler handler;
		Class<?> handlerClass = conf.getClass(
				SimJoin.CONF_HANDLER_ITEMPARTITION_CLASS, null);
		try {
			handler = (ItemPartitionHandler) ReflectionUtils.newInstance(
					handlerClass, conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return handler;
	}
}

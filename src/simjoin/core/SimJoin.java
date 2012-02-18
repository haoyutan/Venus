package simjoin.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class SimJoin {
	
	public static final String CONF_HANDLER_CLASS = "simjoin.core.simjoinhandler.class";
	public static final String CONF_ITEM_CLASS = "simjoin.core.item.class";
	public static final String CONF_HAS_SIG = "simjoin.core.has_signature";
	public static final String CONF_OUTPUT_PLD = "simjoin.core.output_payload";

	@SuppressWarnings("rawtypes")
	public static void setItemClass(Configuration conf,
			Class<? extends ItemWritable> itemClass) {
		conf.setClass(CONF_ITEM_CLASS, itemClass, ItemWritable.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static void setHandlerClass(Configuration conf,
			Class<? extends SimJoinHandler> handlerClass) {
		conf.setClass(CONF_HANDLER_CLASS, handlerClass, SimJoinHandler.class);
	}
	
	public static void setHasSignature(Configuration conf, boolean hasSignature) {
		conf.setBoolean(CONF_HAS_SIG, hasSignature);
	}
	
	public static void setOutputPayload(Configuration conf, boolean outputPayload) {
		conf.setBoolean(CONF_OUTPUT_PLD, outputPayload);
	}
	
	@SuppressWarnings({ "rawtypes" })
	public static SimJoinHandler createHandler(Configuration conf) {
		SimJoinHandler handler;
		Class<?> handlerClass = conf.getClass(SimJoin.CONF_HANDLER_CLASS,
				null);
		try {
			handler = (SimJoinHandler) ReflectionUtils.newInstance(
					handlerClass, conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return handler;
	}
}

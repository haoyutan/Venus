package simjoin.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemJoinHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class SimJoinUtils {

	@SuppressWarnings("rawtypes")
	public static ItemBuildHandler createItemBuildHandler(Configuration conf) {
		Class<?> handlerClass = SimJoinConf.getItemBuildHandlerClass(conf);
		return (ItemBuildHandler) createInstance(handlerClass, conf);
	}

	@SuppressWarnings("rawtypes")
	public static ItemPartitionHandler createItemPartitionHandler(
			Configuration conf) {
		Class<?> handlerClass = SimJoinConf.getItemPartitionHandlerClass(conf);
		return (ItemPartitionHandler) createInstance(handlerClass, conf);
	}
	
	@SuppressWarnings("rawtypes")
	public static ItemJoinHandler createItemJoinHandler(
			Configuration conf) {
		Class<?> handlerClass = SimJoinConf.getItemJoinHandlerClass(conf);
		return (ItemJoinHandler) createInstance(handlerClass, conf);
	}
	
	private static Object createInstance(Class<?> theClass, Configuration conf) {
		Object instance;
		try {
			instance = ReflectionUtils.newInstance(theClass, conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
}

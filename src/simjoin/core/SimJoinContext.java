package simjoin.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class SimJoinContext extends Configured {
	
	public static final String CK_NAME = "simjoin.name";
	
	public static final String CK_WORKDIR = "simjoin.workdir";
	
	public static final String CK_ALGO = "simjoin.core.algorithm";
	public static final String CV_ALGO_AUTO = "auto";
	public static final String CV_ALGO_CLONE = "clone";
	public static final String CV_ALGO_SHADOW = "shadow";
	public static final String CV_ALGO_SHADOWRANDOM = "shadowrandom";
	
	public static final String CK_ITEM_CLASS = "simjoin.core.item.class";
	
	public static final String CK_HANDLER_ITEMBUILD_CLASS = "simjoin.core.handler.itembuild.class";
	public static final String CK_HAS_SIG = "simjoin.core.has_signature";
	
	public static final String CK_HANDLER_ITEMPARTITION_CLASS = "simjoin.core.handler.itempartition.class";
	
	public static void setSimJoinName(Configuration conf, String simJoinName) {
		conf.set(CK_NAME, simJoinName);
	}
	
	public void setSimJoinName(String simJoinName) {
		setSimJoinName(getConf(), simJoinName);
	}
	
	public static String getSimJoinName(Configuration conf) {
		return conf.get(CK_NAME);
	}
	
	public String getSimJoinName() {
		return getSimJoinName(getConf());
	}
	
	public static void setSimJoinWorkDir(Configuration conf, Path workDir) {
		workDir = new Path(workDir, "_simjoin");
		try {
			workDir = workDir.getFileSystem(conf).makeQualified(workDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	    String dirStr = StringUtils.escapeString(workDir.toString());
		conf.set(CK_WORKDIR, dirStr);
	}
	
	public void setSimJoinWorkDir(Path workDir) {
		setSimJoinWorkDir(getConf(), workDir);
	}
	
	public static Path getSimJoinWorkDir(Configuration conf) {
		Path workDir = new Path(conf.get(CK_WORKDIR));
		return workDir;
	}
	
	public Path getSimJoinWorkDir() {
		return getSimJoinWorkDir(getConf());
	}
	
	public static void setSimJoinAlgorithm(Configuration conf, String simJoinAlgorithm) {
		conf.set(CK_ALGO, simJoinAlgorithm);
	}
	
	public void setSimJoinAlgorithm(String simJoinAlgorithm) {
		setSimJoinAlgorithm(getConf(), simJoinAlgorithm);
	}
	
	public static String getSimJoinAlgorithm(Configuration conf) {
		return conf.get(CK_ALGO);
	}
	
	public String getSimJoinAlgorithm() {
		return getSimJoinAlgorithm(getConf());
	}
	
	@SuppressWarnings("rawtypes")
	public static void setItemClass(Configuration conf,
			Class<? extends ItemWritable> itemClass) {
		conf.setClass(CK_ITEM_CLASS, itemClass, ItemWritable.class);
	}
	
	@SuppressWarnings("rawtypes")
	public void setItemClass(Class<? extends ItemWritable> itemClass) {
		setItemClass(getConf(), itemClass);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Class<? extends ItemWritable> getItemClass(Configuration conf) {
		return (Class<? extends ItemWritable>) conf.getClass(CK_ITEM_CLASS,
				null);
	}
	
	@SuppressWarnings("rawtypes")
	public Class<? extends ItemWritable> getItemClass() {
		return getItemClass(getConf());
	}
	
	@SuppressWarnings("rawtypes")
	public static void setItemBuildHandlerClass(Configuration conf,
			Class<? extends ItemBuildHandler> handlerClass, boolean hasSignature) {
		conf.setClass(CK_HANDLER_ITEMBUILD_CLASS, handlerClass,
				ItemBuildHandler.class);
		conf.setBoolean(CK_HAS_SIG, hasSignature);
	}

	@SuppressWarnings("rawtypes")
	public void setItemBuildHandlerClass(
			Class<? extends ItemBuildHandler> handlerClass, boolean hasSignature) {
		setItemBuildHandlerClass(getConf(), handlerClass, hasSignature);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Class<? extends ItemBuildHandler> getItemBuildHandlerClass(
			Configuration conf) {
		return (Class<? extends ItemBuildHandler>) conf.getClass(
				CK_HANDLER_ITEMBUILD_CLASS, null);
	}
	
	@SuppressWarnings("rawtypes")
	public Class<? extends ItemBuildHandler> getItemBuildHandlerClass() {
		return getItemBuildHandlerClass(getConf());
	}
	
	public static boolean hasSignature(Configuration conf) {
		return conf.getBoolean(CK_HAS_SIG, false);
	}
	
	public boolean hasSignature() {
		return hasSignature(getConf());
	}
	
	@SuppressWarnings("rawtypes")
	public static void setItemPartitionHandlerClass(Configuration conf,
			Class<? extends ItemPartitionHandler> handlerClass) {
		conf.setClass(CK_HANDLER_ITEMPARTITION_CLASS,
				handlerClass, ItemPartitionHandler.class);
	}
	
	@SuppressWarnings("rawtypes")
	public void setItemPartitionHandlerClass(
			Class<? extends ItemPartitionHandler> handlerClass) {
		setItemPartitionHandlerClass(getConf(), handlerClass);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Class<? extends ItemPartitionHandler> getItemPartitionHandlerClass(
			Configuration conf) {
		return (Class<? extends ItemPartitionHandler>) conf.getClass(
				CK_HANDLER_ITEMPARTITION_CLASS, null);
	}
	
	@SuppressWarnings("rawtypes")
	public Class<? extends ItemPartitionHandler> getItemPartitionHandlerClass() {
		return getItemPartitionHandlerClass(getConf());
	}
	
	@SuppressWarnings("rawtypes")
	public static ItemBuildHandler createItemBuildHandler(Configuration conf) {
		Class<?> handlerClass = getItemBuildHandlerClass(conf);
		return (ItemBuildHandler) createInstance(handlerClass, conf);
	}
	
	@SuppressWarnings("rawtypes")
	public ItemBuildHandler createItemBuildHandler() {
		return createItemBuildHandler(getConf());
	}
	
	@SuppressWarnings("rawtypes")
	public static ItemPartitionHandler createItemPartitionHandler(Configuration conf) {
		Class<?> handlerClass = getItemPartitionHandlerClass(conf);
		return (ItemPartitionHandler) createInstance(handlerClass, conf);
	}
	
	@SuppressWarnings("rawtypes")
	public ItemPartitionHandler createItemPartitionHandler() {
		return createItemPartitionHandler(getConf());
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
	
	public SimJoinContext(Configuration conf) {
		super(new Configuration(conf));
	}
	
	public SimJoinContext(Job virtualJob) {
		this(virtualJob.getConfiguration());
	}
	
	public SimJoinContext(SimJoinContext context) {
		this(context.getConf());
	}
}

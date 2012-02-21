package simjoin.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;

import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class SimJoinContext extends Configured {

	private static final Log LOG = LogFactory.getLog(SimJoinContext.class);
	
	public static final String CK_NAME = "simjoin.name";
	
	public static final String CK_ALGO = "simjoin.core.algorithm";
	public static final String CV_ALGO_AUTO = "auto";
	public static final String CV_ALGO_CLONE = "clone";
	public static final String CV_ALGO_SHADOW = "shadow";
	public static final String CV_ALGO_SHADOWRANDOM = "shadowrandom";
	
	public static final String CK_ITEM_CLASS = "simjoin.core.item.class";
	
	public static final String CK_HANDLER_ITEMBUILD_CLASS = "simjoin.core.handler.itembuild.class";
	public static final String CK_HAS_SIG = "simjoin.core.has_signature";
	
	public static final String CK_HANDLER_ITEMPARTITION_CLASS = "simjoin.core.handler.itempartition.class";
	
	private Job virtualJob;
	
	private boolean initialized = false;
	
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
		setItemBuildHandlerClass(getConf(), ItemBuildHandler.class,
				hasSignature);
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

	public SimJoinContext(Job virtualJob) {
		super(new Configuration(virtualJob.getConfiguration()));
		this.virtualJob = virtualJob;
	}

	public boolean isInitialized() {
		return initialized;
	}

	public void setInitialized(boolean initialized) {
		this.initialized = initialized;
	}

	public void initialize() {
		LOG.info("initialize: Begin...");
		if (!isInitialized()) {
			checkVirtualJob();
			checkMandatoryArguments();
			setDefaultValues();
			setInitialized(true);
			LOG.info("initialize: Done.");
		} else
			LOG.info("initialize: Already initialized before.");
	}
	
	private void checkVirtualJob() {
		try {
			LOG.info("InputFormat: " + virtualJob.getInputFormatClass().getName());
			LOG.info("OutputFormat: " + virtualJob.getOutputFormatClass().getName());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void checkMandatoryArguments() {
		checkClass(CK_ITEM_CLASS, ItemWritable.class);
		checkClass(CK_HANDLER_ITEMBUILD_CLASS, ItemBuildHandler.class);
		checkValue(CK_HAS_SIG);
		checkClass(CK_HANDLER_ITEMPARTITION_CLASS, ItemPartitionHandler.class);
	}
	
	private void checkClass(String confKey, Class<?> superClass) {
		Configuration conf  = getConf();
		Class<?> theClass = conf.getClass(confKey, null);
		if (theClass == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		if (!superClass.isAssignableFrom(theClass))
			throw new RuntimeException("" + theClass + " is not a subclass of "
					+ superClass + ".");
		LOG.info(confKey + ": " + theClass.getName());
	}
	
	private void checkValue(String confKey) {
		Configuration conf  = getConf();
		String value = conf.get(confKey, null);
		if (value == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		LOG.info(confKey + ": " + value);
	}
	
	private void setDefaultValues() {
		setIfNotSpecified(CK_ALGO, CV_ALGO_AUTO);
	}
	
	private void setIfNotSpecified(String confKey, String defaultValue) {
		Configuration conf  = getConf();
		String value = conf.get(confKey, null);
		if (value == null) {
			LOG.info(confKey + " is not specified. Use the default value.");
			conf.set(confKey, defaultValue);
		}
		LOG.info(confKey + ": " + conf.get(confKey, null));
	}
}

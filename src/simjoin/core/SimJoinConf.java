package simjoin.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.util.StringUtils;

import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class SimJoinConf extends Configuration {

	public static final String CK_NAME = "simjoin.name";

	public static final String CK_WORKDIR = "simjoin.workdir";

	public static final String CK_INPUTFORMAT_CLASS = "simjoin.core.inputformat.class";

	public static final String CK_ALGO = "simjoin.core.algorithm";
	public static final String CV_ALGO_AUTO = "auto";
	public static final String CV_ALGO_CLONE = "clone";
	public static final String CV_ALGO_SHADOW = "shadow";
	public static final String CV_ALGO_SHADOWRANDOM = "shadowrandom";

	public static final String CK_ITEM_CLASS = "simjoin.core.item.class";

	public static final String CK_HANDLER_ITEMBUILD_CLASS = "simjoin.core.handler.itembuild.class";
	public static final String CK_HAS_SIG = "simjoin.core.has_signature";

	public static final String CK_HANDLER_ITEMPARTITION_CLASS = "simjoin.core.handler.itempartition.class";
	
	public static final String CK_CLUSTER_TASK_SLOTS = "simjoin.core.cluster.task.slots";

	public SimJoinConf() {
		super(new Configuration());
	}

	public SimJoinConf(Configuration conf) {
		super(conf);
	}
	
	// SequenceFile split should be larger than io.seqfile.compress.blocksize
	public static long getSequenceFileCompressionBlockSize(Configuration conf) {
		return conf.getInt("io.seqfile.compress.blocksize", Integer.MAX_VALUE);
	}
	
	public long getSequenceFileCompressionBlockSize() {
		return getSequenceFileCompressionBlockSize(this);
	}
	
	// setPath
	public static void setPath(Configuration conf, String key, Path path)
			throws IOException {
		path = path.getFileSystem(conf).makeQualified(path);
		String dirStr = StringUtils.escapeString(path.toString());
		conf.set(key, dirStr);
	}
	
	public void setPath(String key, Path path) throws IOException {
		setPath(this, key, path);
	}

	// CK_NAME
	public static void setSimJoinName(Configuration conf, String simJoinName) {
		conf.set(CK_NAME, simJoinName);
	}

	public void setSimJoinName(String simJoinName) {
		setSimJoinName(this, simJoinName);
	}

	public static String getSimJoinName(Configuration conf) {
		return conf.get(CK_NAME);
	}

	public String getSimJoinName() {
		return getSimJoinName(this);
	}

	// CK_WORKDIR
	public static void setWorkDir(Configuration conf, Path workDir)
			throws IOException {
		setPath(conf, CK_WORKDIR, workDir);
	}

	public void setWorkDir(Path workDir) throws IOException {
		setWorkDir(this, workDir);
	}

	public static Path getWorkDir(Configuration conf) {
		Path workDir = new Path(conf.get(CK_WORKDIR));
		return workDir;
	}

	public Path getWorkDir() {
		return getWorkDir(this);
	}

	// CK_INPUTFORMAT_CLASS
	@SuppressWarnings("rawtypes")
	public static void setInputFormatClass(Configuration conf,
			Class<? extends InputFormat> inputFormatClass) {
		conf.setClass(CK_INPUTFORMAT_CLASS, inputFormatClass, InputFormat.class);
	}

	@SuppressWarnings("rawtypes")
	public void setInputFormatClass(
			Class<? extends InputFormat> inputFormatClass) {
		setInputFormatClass(this, inputFormatClass);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Class<? extends InputFormat> getInputFormatClass(
			Configuration conf) {
		return (Class<? extends InputFormat>) conf.getClass(
				CK_INPUTFORMAT_CLASS, null);
	}
	
	@SuppressWarnings("rawtypes")
	public Class<? extends InputFormat> getInputFormatClass() {
		return getInputFormatClass(this);
	}

	// CK_ALGO
	public static void setSimJoinAlgorithm(Configuration conf,
			String simJoinAlgorithm) {
		conf.set(CK_ALGO, simJoinAlgorithm);
	}

	public void setSimJoinAlgorithm(String simJoinAlgorithm) {
		setSimJoinAlgorithm(this, simJoinAlgorithm);
	}

	public static String getSimJoinAlgorithm(Configuration conf) {
		return conf.get(CK_ALGO);
	}

	public String getSimJoinAlgorithm() {
		return getSimJoinAlgorithm(this);
	}

	// CK_ITEM_CLASS
	@SuppressWarnings("rawtypes")
	public static void setItemClass(Configuration conf,
			Class<? extends ItemWritable> itemClass) {
		conf.setClass(CK_ITEM_CLASS, itemClass, ItemWritable.class);
	}

	@SuppressWarnings("rawtypes")
	public void setItemClass(Class<? extends ItemWritable> itemClass) {
		setItemClass(this, itemClass);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Class<? extends ItemWritable> getItemClass(Configuration conf) {
		return (Class<? extends ItemWritable>) conf.getClass(CK_ITEM_CLASS,
				null);
	}

	@SuppressWarnings("rawtypes")
	public Class<? extends ItemWritable> getItemClass() {
		return getItemClass(this);
	}

	// CK_HANDLER_ITEMBUILD_CLASS, CK_HAS_SIG
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
		setItemBuildHandlerClass(this, handlerClass, hasSignature);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Class<? extends ItemBuildHandler> getItemBuildHandlerClass(
			Configuration conf) {
		return (Class<? extends ItemBuildHandler>) conf.getClass(
				CK_HANDLER_ITEMBUILD_CLASS, null);
	}

	@SuppressWarnings("rawtypes")
	public Class<? extends ItemBuildHandler> getItemBuildHandlerClass() {
		return getItemBuildHandlerClass(this);
	}

	public static boolean hasSignature(Configuration conf) {
		return conf.getBoolean(CK_HAS_SIG, false);
	}

	public boolean hasSignature() {
		return hasSignature(this);
	}

	// CK_HANDLER_ITEMPARTITION_CLASS
	@SuppressWarnings("rawtypes")
	public static void setItemPartitionHandlerClass(Configuration conf,
			Class<? extends ItemPartitionHandler> handlerClass) {
		conf.setClass(CK_HANDLER_ITEMPARTITION_CLASS, handlerClass,
				ItemPartitionHandler.class);
	}

	@SuppressWarnings("rawtypes")
	public void setItemPartitionHandlerClass(
			Class<? extends ItemPartitionHandler> handlerClass) {
		setItemPartitionHandlerClass(this, handlerClass);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Class<? extends ItemPartitionHandler> getItemPartitionHandlerClass(
			Configuration conf) {
		return (Class<? extends ItemPartitionHandler>) conf.getClass(
				CK_HANDLER_ITEMPARTITION_CLASS, null);
	}

	@SuppressWarnings("rawtypes")
	public Class<? extends ItemPartitionHandler> getItemPartitionHandlerClass() {
		return getItemPartitionHandlerClass(this);
	}
	
	// CK_CLUSTER_TASK_SLOTS
	public static void setClusterTaskSlots(Configuration conf, int numSlots) {
		conf.setInt(CK_CLUSTER_TASK_SLOTS, numSlots);
	}
	
	public void setClusterTaskSlots(int numSlots) {
		setClusterTaskSlots(this, numSlots);
	}
	
	public static int getClusterTaskSlots(Configuration conf) {
		return conf.getInt(CK_CLUSTER_TASK_SLOTS, -1);
	}
	
	public int getClusterTaskSlots() {
		return getClusterTaskSlots(this);
	}
}

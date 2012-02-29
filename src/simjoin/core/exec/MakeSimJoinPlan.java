package simjoin.core.exec;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;
import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;
import simjoin.core.handler.NestedLoopItemJoinHandler;

public class MakeSimJoinPlan extends BaseTask {
	
	private static final Log LOG = LogFactory.getLog(MakeSimJoinPlan.class);
	
	private static final String CK_PLAN_PREFIX = "simjoin.core.plan";
	
	public static final String CK_PLAN_ALGO = CK_PLAN_PREFIX + ".algorithm";

	public MakeSimJoinPlan(Configuration conf) {
		super(conf);
	}
	
	public Configuration getPlan() {
		return new Configuration(getConf());
	}
	
	@Override
	protected int runTask(String[] args) throws Exception {
		checkConfiguration();
		plan();
		printPlan();
		return 0;
	}
	
	private void printPlan() {
		LOG.info("Printing plan...");
		Configuration conf = getConf();
		Map<String, String> map = conf.getValByRegex(CK_PLAN_PREFIX);
		for (Map.Entry<String, String> kv : map.entrySet())
			LOG.info("  " + kv.getKey() + ": " + kv.getValue());
		LOG.info("Printing plan... Done.");
	}
	
	// TODO
	private void plan() {
		LOG.info("Making similarity join plan...");
		Configuration conf = getConf();
		String algorithm = SimJoinConf.getSimJoinAlgorithm(conf);
		
		if (SimJoinConf.CV_ALGO_AUTO.equals(algorithm))
			algorithm = SimJoinConf.CV_ALGO_SHADOW;
		conf.set(CK_PLAN_ALGO, algorithm);
		LOG.info("  Planned algorithm: " + algorithm);
		LOG.info("Making similarity join plan... Done.");
	}

	private void checkConfiguration() {
		LOG.info("Checking configuration...");
		checkInputOutputFormat();
		checkMandatoryArguments();
		setDefaultValues();
		LOG.info("Checking configuration... Done.");
	}
	
	private void checkInputOutputFormat() {
		try {
			Job job = new Job(getConf());
			LOG.info("  InputFormat: " + job.getInputFormatClass().getName());
			LOG.info("  OutputFormat: " + job.getOutputFormatClass().getName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void checkMandatoryArguments() {
		checkClass(SimJoinConf.CK_ITEM_CLASS, ItemWritable.class);
		checkClass(SimJoinConf.CK_HANDLER_ITEMBUILD_CLASS,
				ItemBuildHandler.class);
		checkValue(SimJoinConf.CK_HAS_SIG);
		checkClass(SimJoinConf.CK_HANDLER_ITEMPARTITION_CLASS,
				ItemPartitionHandler.class);
	}
	
	private void checkClass(String confKey, Class<?> superClass) {
		Class<?> theClass = getConf().getClass(confKey, null);
		if (theClass == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		if (!superClass.isAssignableFrom(theClass))
			throw new RuntimeException("" + theClass + " is not a subclass of "
					+ superClass + ".");
		LOG.info("  " + confKey + ": " + theClass.getName());
	}
	
	private void checkValue(String confKey) {
		String value = getConf().get(confKey, null);
		if (value == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		LOG.info("  " + confKey + ": " + value);
	}
	
	private void setDefaultValues() {
		setIfNotSpecified(SimJoinConf.CK_ALGO, SimJoinConf.CV_ALGO_AUTO);
		setIfNotSpecified(SimJoinConf.CK_CLUSTER_TASK_SLOTS, "2");
		setIfNotSpecified(SimJoinConf.CK_HANDLER_ITEMJOIN_CLASS,
				NestedLoopItemJoinHandler.class.getName());
	}
	
	private void setIfNotSpecified(String confKey, String defaultValue) {
		Configuration conf = getConf();
		String value = conf.get(confKey, null);
		if (value == null) {
			conf.set(confKey, defaultValue);
			LOG.info("  " + confKey + ": " + conf.get(confKey, null)
					+ " (default)");
		} else
			LOG.info("  " + confKey + ": " + conf.get(confKey, null));
	}
}

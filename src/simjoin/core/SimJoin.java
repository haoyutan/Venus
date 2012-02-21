package simjoin.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;


public class SimJoin {

	private static final Log LOG = LogFactory.getLog(SimJoin.class);
	
	public static final String CK_PLAN_OUTPUTPAYLOAD = "simjoin.core.plan.output_payload";
	
	private SimJoinContext sjCtx;

	public SimJoin(SimJoinContext simJoinContext) {
		this.sjCtx = new SimJoinContext(simJoinContext);
	}

	protected void initialize() {
		LOG.info("initialize: Begin...");
		checkInputOutputFormat();
		checkMandatoryArguments();
		setDefaultValues();
		LOG.info("initialize: Done.");
	}
	
	protected void planGlobal() {
		String algorithm = sjCtx.getSimJoinAlgorithm();
		boolean outputPayload = false;
		if (SimJoinContext.CV_ALGO_CLONE.equals(algorithm))
			outputPayload = true;
		sjCtx.getConf().setBoolean(CK_PLAN_OUTPUTPAYLOAD, outputPayload);
	}
	
	private int run(int stepBegin, int stepEnd) throws Exception {
		initialize();
		planGlobal();
		
		Job job = new Job(sjCtx.getConf());
		job.setJarByClass(getClass());
		job.setJobName("Step-1-ItemPartition");

		Path itemPartitionPath = new Path(sjCtx.getSimJoinWorkDir(), "ItemPartition");
		FileOutputFormat.setOutputPath(job, itemPartitionPath);
		PartitionItems.configureJob(job);
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public void run() throws Exception {
		run(0, -1);
	}
	
	private void checkInputOutputFormat() {
		try {
			Job job = new Job(sjCtx.getConf());
			LOG.info("InputFormat: " + job.getInputFormatClass().getName());
			LOG.info("OutputFormat: " + job.getOutputFormatClass().getName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void checkMandatoryArguments() {
		checkValue(SimJoinContext.CK_WORKDIR);
		checkClass(SimJoinContext.CK_ITEM_CLASS, ItemWritable.class);
		checkClass(SimJoinContext.CK_HANDLER_ITEMBUILD_CLASS, ItemBuildHandler.class);
		checkValue(SimJoinContext.CK_HAS_SIG);
		checkClass(SimJoinContext.CK_HANDLER_ITEMPARTITION_CLASS, ItemPartitionHandler.class);
	}
	
	private void checkClass(String confKey, Class<?> superClass) {
		Configuration conf  = sjCtx.getConf();
		Class<?> theClass = conf.getClass(confKey, null);
		if (theClass == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		if (!superClass.isAssignableFrom(theClass))
			throw new RuntimeException("" + theClass + " is not a subclass of "
					+ superClass + ".");
		LOG.info(confKey + ": " + theClass.getName());
	}
	
	private void checkValue(String confKey) {
		Configuration conf  = sjCtx.getConf();
		String value = conf.get(confKey, null);
		if (value == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		LOG.info(confKey + ": " + value);
	}
	
	private void setDefaultValues() {
		setIfNotSpecified(SimJoinContext.CK_ALGO, SimJoinContext.CV_ALGO_AUTO);
	}
	
	private void setIfNotSpecified(String confKey, String defaultValue) {
		Configuration conf  = sjCtx.getConf();
		String value = conf.get(confKey, null);
		if (value == null) {
			LOG.info(confKey + " is not specified. Use the default value.");
			conf.set(confKey, defaultValue);
		}
		LOG.info(confKey + ": " + conf.get(confKey, null));
	}
}

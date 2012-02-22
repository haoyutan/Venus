package simjoin.core.tmp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;
import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class SimJoinDeprecated {

	private static final Log LOG = LogFactory.getLog(SimJoinDeprecated.class);
	
	public static final String CK_PLAN_PREFIX = "simjoin.core.plan";
	public static final String CK_PLAN_OUTPUTPAYLOAD = CK_PLAN_PREFIX + ".output_payload";
	
	// CP stands for checkpoint
	private static final String CP_00_INITIALIZE = "Stage-00-Initialize";
	private static final String CP_01_PLANGLOBAL = "Stage-01-PlanGlobal";
	//private static final String CP_02_ITEMPARTITION = "Stage-02-ItemPartition";
	
	private SimJoinConf sjConf;
	
	private Path workDir;
	
	private FileSystem fs;
	
	public SimJoinDeprecated(SimJoinConf simJoinConf) {
		this.sjConf = new SimJoinConf(simJoinConf);
	}

	protected void initialize() throws IOException {
		checkValue(SimJoinConf.CK_WORKDIR);
		workDir = sjConf.getWorkDir();
		fs = workDir.getFileSystem(sjConf);
		
		if (stageFinished(CP_00_INITIALIZE)) {
			LOG.info("Cached stage " + CP_00_INITIALIZE + ": Loading...");
			loadInitializedContext();
			checkInputOutputFormat();
			checkMandatoryArguments();
			LOG.info("Cached stage " + CP_00_INITIALIZE + ": Loaded with success.");
		} else {
			LOG.info(CP_00_INITIALIZE + ": Executing...");
			checkInputOutputFormat();
			checkMandatoryArguments();
			setDefaultValues();
			LOG.info(CP_00_INITIALIZE + ": Saving...");
			dumpInitializedContext();
			LOG.info(CP_00_INITIALIZE + ": Done.");
		}
	}
	
	private void loadInitializedContext() throws IOException {
		Path xmlFile = new Path(new Path(workDir, CP_00_INITIALIZE),
				"simjoin-conf.xml");
		InputStream inputStream = fs.open(xmlFile);
		sjConf.addResource(inputStream);
		sjConf.get("DUMMY_KEY"); // force conf to consume inputStream immediately
		inputStream.close();
	}
	
	private void dumpInitializedContext() throws IOException {
		Path tmpDir = new Path(workDir, "_tmp-" + CP_00_INITIALIZE);
		if (fs.exists(tmpDir))
			fs.delete(tmpDir, true);
		Path xmlFile = new Path(tmpDir, "simjoin-conf.xml");
		OutputStream outputStream = fs.create(xmlFile,	true);
		sjConf.writeXml(outputStream);
		outputStream.close();
		fs.rename(tmpDir, new Path(workDir, CP_00_INITIALIZE));
	}
	
	protected void planGlobal() throws IOException {
		if (stageFinished(CP_01_PLANGLOBAL)) {
			LOG.info("Cached stage " + CP_01_PLANGLOBAL + ": Loading...");
			loadPlanGlobal();
			LOG.info("Cached stage " + CP_01_PLANGLOBAL + ": Loaded with success.");
		} else {
			LOG.info(CP_01_PLANGLOBAL + ": Executing...");
			_planGlobal();
			LOG.info(CP_01_PLANGLOBAL + ": Saving...");
			dumpPlanGlobal();
			LOG.info(CP_01_PLANGLOBAL + ": Done.");
		}
	}
	
	private void loadPlanGlobal() throws IOException {
		Path xmlFile = new Path(new Path(workDir, CP_01_PLANGLOBAL),
				"simjoin-conf-with-plan.xml");
		InputStream inputStream = fs.open(xmlFile);
		sjConf.addResource(inputStream);
		sjConf.get("DUMMY_KEY"); // force conf to consume inputStream immediately
		inputStream.close();
	}
	
	private void dumpPlanGlobal() throws IOException {
		Path tmpDir = new Path(workDir, "_tmp-" + CP_01_PLANGLOBAL);
		if (fs.exists(tmpDir))
			fs.delete(tmpDir, true);
		Path xmlFile = new Path(tmpDir, "simjoin-conf-with-plan.xml");
		OutputStream outputStream = fs.create(xmlFile,	true);
		sjConf.writeXml(outputStream);
		outputStream.close();
		fs.rename(tmpDir, new Path(workDir, CP_01_PLANGLOBAL));
	}
	
	private void _planGlobal() {
		String algorithm = sjConf.getSimJoinAlgorithm();
		boolean outputPayload = false;
		if (SimJoinConf.CV_ALGO_CLONE.equals(algorithm))
			outputPayload = true;
		sjConf.setBoolean(CK_PLAN_OUTPUTPAYLOAD, outputPayload);
	}
	
	public int run() throws Exception {
		initialize();
		planGlobal();
		return 0;
		
//		Job job = new Job(sjCtx.getConf());
//		job.setJarByClass(getClass());
//		job.setJobName(CP_02_ITEMPARTITION);
//
//		Path itemPartitionPath = new Path(sjCtx.getSimJoinWorkDir(),
//				CP_02_ITEMPARTITION);
//		FileOutputFormat.setOutputPath(job, itemPartitionPath);
//		PartitionItems.configureJob(job);
//		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	private boolean stageFinished(String stage) throws IOException {
		Path checkpoint = new Path(workDir, stage);
		if (fs.exists(checkpoint) && fs.getFileStatus(checkpoint).isDir())
			return true;
		return false;
	}
	
	private void checkInputOutputFormat() {
		try {
			Job job = new Job(sjConf);
			LOG.info("InputFormat: " + job.getInputFormatClass().getName());
			LOG.info("OutputFormat: " + job.getOutputFormatClass().getName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void checkMandatoryArguments() {
		checkClass(SimJoinConf.CK_ITEM_CLASS, ItemWritable.class);
		checkClass(SimJoinConf.CK_HANDLER_ITEMBUILD_CLASS, ItemBuildHandler.class);
		checkValue(SimJoinConf.CK_HAS_SIG);
		checkClass(SimJoinConf.CK_HANDLER_ITEMPARTITION_CLASS, ItemPartitionHandler.class);
	}
	
	private void checkClass(String confKey, Class<?> superClass) {
		Class<?> theClass = sjConf.getClass(confKey, null);
		if (theClass == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		if (!superClass.isAssignableFrom(theClass))
			throw new RuntimeException("" + theClass + " is not a subclass of "
					+ superClass + ".");
		LOG.info(confKey + ": " + theClass.getName());
	}
	
	private void checkValue(String confKey) {
		String value = sjConf.get(confKey, null);
		if (value == null)
			throw new RuntimeException("Must specify " + confKey + ".");
		LOG.info(confKey + ": " + value);
	}
	
	private void setDefaultValues() {
		setIfNotSpecified(SimJoinConf.CK_ALGO, SimJoinConf.CV_ALGO_AUTO);
	}
	
	private void setIfNotSpecified(String confKey, String defaultValue) {
		String value = sjConf.get(confKey, null);
		if (value == null) {
			LOG.info(confKey + " is not specified. Use the default value.");
			sjConf.set(confKey, defaultValue);
		}
		LOG.info(confKey + ": " + sjConf.get(confKey, null));
	}
}

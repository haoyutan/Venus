package simjoin.core.exec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import simjoin.core.ItemWritable;
import simjoin.core.SimJoinConf;
import simjoin.core.handler.ItemBuildHandler;
import simjoin.core.handler.ItemPartitionHandler;

public class SimJoinPlan extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(SimJoinPlan.class);
	
	private static final String CK_PLAN_PREFIX = "simjoin.core.plan";
	
	private static final String FILENAME_CONF = "simjoin-conf-with-plan.dat";
	private static final String FILENAME_XML = "simjoin-conf-with-plan.xml";
	
	private Path workDir;

	public SimJoinPlan(Configuration conf) {
		super(conf);
		workDir = SimJoinConf.getWorkDir(conf);
	}
	
	public Configuration getPlan() {
		return new Configuration(getConf());
	}
	
	@Override
	public int run(String[] args) throws Exception {
		boolean recovered = recover();
		checkConfiguration();
		if (!recovered) {
			plan();
			savePlan();
		}
		printPlan();
		return 0;
	}
	
	private boolean recover() {
		Configuration conf = getConf();
		Path confFile = new Path(workDir, FILENAME_CONF);
		FileSystem fs;
		boolean found;
		try {
			fs = confFile.getFileSystem(conf);
			found = fs.exists(confFile);
		} catch (IOException e) {
			return false;
		}
		if (found) {
			LOG.info("Found saved results. Try to recover...");
			Configuration savedConf = new Configuration();
			try {
				DataInputStream in = fs.open(confFile);
				savedConf.readFields(in);
				in.close();
			} catch (IOException e) {
				LOG.info("Failed to recover: " + e.getMessage());
				return false;
			}
			setConf(savedConf);
		} else
			return false;

		LOG.info("Recovered with success.");
		return true;
	}
	
	private void savePlan() throws IOException {
		LOG.info("Saving plan...");
		
		// save binary (used for recovery)
		Configuration conf = getConf();
		Path confFile = new Path(workDir, FILENAME_CONF);
		FileSystem fs = confFile.getFileSystem(conf);
		DataOutputStream out = fs.create(confFile, true);
		conf.write(out);
		out.close();
		
		// save xml (used for logging)
		Path xmlFile = new Path(workDir, FILENAME_XML);
		out = fs.create(xmlFile, true);
		conf.writeXml(out);
		out.close();
		
		LOG.info("Saving plan... Done.");
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
		if (!SimJoinConf.CV_ALGO_CLONE.equals(algorithm)) {
			LOG.warn("  Not support algorithm '" + algorithm
					+ "'. Change to 'clone'.");
			conf.set(CK_PLAN_PREFIX + ".algorithm", SimJoinConf.CV_ALGO_CLONE);
		}
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
		checkClass(SimJoinConf.CK_HANDLER_ITEMBUILD_CLASS, ItemBuildHandler.class);
		checkValue(SimJoinConf.CK_HAS_SIG);
		checkClass(SimJoinConf.CK_HANDLER_ITEMPARTITION_CLASS, ItemPartitionHandler.class);
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
	}
	
	private void setIfNotSpecified(String confKey, String defaultValue) {
		Configuration conf = getConf();
		String value = conf.get(confKey, null);
		if (value == null) {
			LOG.info("  " + confKey + " is not specified. Use the default value.");
			conf.set(confKey, defaultValue);
		}
		LOG.info("  " + confKey + ": " + conf.get(confKey, null));
	}
}

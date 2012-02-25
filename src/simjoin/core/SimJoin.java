package simjoin.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import simjoin.core.exec.PartitionItems;
import simjoin.core.exec.SchedulePartitionPairs;
import simjoin.core.exec.SimJoinPlan;

public class SimJoin extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(SimJoin.class);
	
	private Path workDir;

	public SimJoin(Configuration conf) {
		super(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		int ret = -1;
	
		Configuration conf = new Configuration(getConf());
		workDir = SimJoinConf.getWorkDir(conf);
		
		// Stage 00
		LOG.info("Stage-00-SimJoinPlan: Start...");
		SimJoinConf.setWorkDir(conf, new Path(workDir, "Stage-00-SimJoinPlan"));
		SimJoinPlan simJoinPlan = new SimJoinPlan(conf);
		ret = simJoinPlan.run(null);
		LOG.info("Stage-00-SimJoinPlan: Finished with success.");
		
		Configuration plan = simJoinPlan.getPlan();
		
		// plan
		String algorithm = plan.get(SimJoinPlan.CK_PLAN_ALGO);
		if (SimJoinConf.CV_ALGO_CLONE.equals(algorithm))
			doCloneJoin(plan, args);
		else if (SimJoinConf.CV_ALGO_SHADOW.equals(algorithm))
			doShadowJoin(plan, args);
		else {
			LOG.error("Does not support planned algorithm: " + algorithm);
			ret = -1;
		}
			
		return ret;
	}
	
	private int doCloneJoin(Configuration plan, String[] args) throws Exception {
		int ret = -1;
		final String ALGONAME = "CloneJoin";
		Configuration conf;
		
		// Stage 01
		String stage01Name = "Stage-01-" + ALGONAME + "-PartitionItems";
		conf = new Configuration(plan);
		conf.setBoolean(PartitionItems.CK_OUTPUT_PAYLOAD, true);
		SimJoinConf.setWorkDir(conf, new Path(workDir, stage01Name));

		LOG.info(stage01Name + ": Start...");
		PartitionItems partitionItems = new PartitionItems(conf);
		ret = partitionItems.run(null);
		LOG.info(stage01Name + ": Finished with success.");
		
		// Stage 02
		String stage02Name = "Stage-02-" + ALGONAME + "-SchedulePartitionPairs";
		conf = new Configuration(plan);
		SimJoinConf.setPath(conf, SchedulePartitionPairs.CK_PARTITIONS_DIR,
				new Path(workDir, stage01Name));
		SimJoinConf.setWorkDir(conf, new Path(workDir, stage02Name));
		
		LOG.info(stage02Name + ": Start...");
		SchedulePartitionPairs schedulePartitionPairs = new SchedulePartitionPairs(
				conf);
		ret = schedulePartitionPairs.run(null);
		LOG.info(stage02Name + ": Finished with success.");
		return ret;
	}
	
	private int doShadowJoin(Configuration plan, String[] args) throws Exception {
		int ret = -1;
		final String ALGONAME = "ShadowJoin";
		String stageName;
		Configuration conf;
		
		// Stage 01
		stageName = "Stage-01-" + ALGONAME + "-PartitionItems";
		conf = new Configuration(plan);
		conf.setBoolean(PartitionItems.CK_OUTPUT_PAYLOAD, false);
		LOG.info(stageName + ": Start...");
		SimJoinConf.setWorkDir(conf, new Path(workDir, stageName));
		PartitionItems partitionItems = new PartitionItems(conf);
		ret = partitionItems.run(null);
		LOG.info(stageName + ": Finished with success.");
		
		return ret;
	}
}

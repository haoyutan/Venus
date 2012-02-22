package simjoin.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import simjoin.core.exec.PartitionItems;
import simjoin.core.exec.SimJoinPlan;

public class SimJoin extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(SimJoin.class);

	public SimJoin(Configuration conf) {
		super(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		int ret = -1;
	
		Configuration conf = new Configuration(getConf());
		Path workDir = SimJoinConf.getWorkDir(conf);
		
		LOG.info("Stage-00-SimJoinPlan: Start...");
		SimJoinConf.setWorkDir(conf, new Path(workDir, "Stage-00-SimJoinPlan"));
		SimJoinPlan simJoinPlan = new SimJoinPlan(conf);
		ret = simJoinPlan.run(null);
		LOG.info("Stage-00-SimJoinPlan: Finished with success.");
		
		Configuration plan = simJoinPlan.getPlan();
		
		LOG.info("Stage-01-PartitionItems: Start...");
		conf = new Configuration(plan);
		SimJoinConf.setWorkDir(conf, new Path(workDir, "Stage-01-PartitionItems"));
		PartitionItems partitionItems = new PartitionItems(conf);
		ret = partitionItems.run(null);
		LOG.info("Stage-00-PartitionItems: Finished with success.");
		
		return ret;
	}
}

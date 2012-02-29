package simjoin.core;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import simjoin.core.exec.BaseTask;
import simjoin.core.exec.DeduplicateItemPairs;
import simjoin.core.exec.MakeSimJoinPlan;
import simjoin.core.exec.PartitionItems;
import simjoin.core.exec.PartitionJoin;
import simjoin.core.exec.SchedulePartitionPairs;

public class SimJoin extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(SimJoin.class);
	
	private Path workDir, planDir;

	public SimJoin(Configuration conf) {
		super(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		int ret = -1;
	
		Configuration conf = new Configuration(getConf());
		workDir = SimJoinConf.getWorkDir(conf);
		planDir = new Path(workDir, "00-MakeSimJoinPlan");
		
		// Stage 00
		LOG.info("00-MakeSimJoinPlan: Start...");
		BaseTask.setTaskOutputPath(conf, planDir);
		MakeSimJoinPlan simJoinPlan = new MakeSimJoinPlan(conf);
		ret = simJoinPlan.run(null);
		LOG.info("00-MakeSimJoinPlan: Finished with success.");
		
		Configuration plan = simJoinPlan.getPlan();
		
		// plan
		String algorithm = plan.get(MakeSimJoinPlan.CK_PLAN_ALGO);
		if (SimJoinConf.CV_ALGO_CLONE.equals(algorithm))
			ret = doCloneJoin(plan);
		else if (SimJoinConf.CV_ALGO_SHADOW.equals(algorithm))
			ret = doShadowJoin(plan);
		else {
			LOG.error("Does not support planned algorithm: " + algorithm);
			ret = -1;
		}

		return ret;
	}
	
	private int doCloneJoin(Configuration plan) throws Exception {
		SimJoinTaskChain chain = new SimJoinTaskChain("01-CloneJoin", workDir,
				planDir);
		Configuration conf;
		
		conf = new Configuration(plan);
		conf.setBoolean(PartitionItems.CK_OUTPUT_PAYLOAD, true);
		chain.appendTask(new PartitionItems(conf));
		
		conf = new Configuration(plan);
		chain.appendTask(new SchedulePartitionPairs(conf));
		
		conf = new Configuration(plan);
		conf.setInt(PartitionJoin.CK_JOIN_TYPE,
				PartitionJoin.CV_JOIN_TYPE_SIG_PAYLOAD);
		chain.appendTask(new PartitionJoin(conf));
		
		if (!SimJoinConf.isSkipDeduplication(plan)) {
			conf = new Configuration(plan);
			chain.appendTask(new DeduplicateItemPairs(conf));
		}
		
		return chain.run();
	}
	
	private int doShadowJoin(Configuration plan) throws Exception {
		SimJoinTaskChain chain = new SimJoinTaskChain("01-ShadowJoin", workDir,
				planDir);
		Configuration conf;
		
		conf = new Configuration(plan);
		conf.setBoolean(PartitionItems.CK_OUTPUT_PAYLOAD, false);
		chain.appendTask(new PartitionItems(conf));
		
		conf = new Configuration(plan);
		chain.appendTask(new SchedulePartitionPairs(conf));
		
		conf = new Configuration(plan);
		conf.setInt(PartitionJoin.CK_JOIN_TYPE, PartitionJoin.CV_JOIN_TYPE_SIG);
		chain.appendTask(new PartitionJoin(conf));
		
		if (!SimJoinConf.isSkipDeduplication(plan)) {
			conf = new Configuration(plan);
			chain.appendTask(new DeduplicateItemPairs(conf));
		}
		
		return chain.run();
	}
	
	private static class SimJoinTaskChain {
		
		private String taskChainName;
		
		private ArrayList<BaseTask> taskChain;
		
		private Path workDir, initInputPath;
		
		public SimJoinTaskChain(String name, Path workDir, Path initInputPath) {
			this.taskChainName = name;
			this.taskChain = new ArrayList<BaseTask>();
			this.workDir = workDir;
			this.initInputPath = initInputPath;
		}
		
		public void appendTask(BaseTask task) {
			taskChain.add(task);
		}
		
		public int run() throws Exception {
			if (taskChain.size() == 0)
				return 0;
			
			int ret = -1;
			for (int i = 0; i < taskChain.size(); i++) {
				String stageName = getStageName(i);
				LOG.info(stageName + ": Start...");
				BaseTask task = taskChain.get(i);
				BaseTask.setTaskInputPath(task.getConf(),
						i == 0 ? initInputPath : new Path(workDir,
								getStageName(i - 1)));
				BaseTask.setTaskOutputPath(task.getConf(), new Path(workDir,
						stageName));
				ret = task.run(null);
				if (ret != 0) {
					LOG.info(stageName + ": Failed.");
					return ret;
				}
				LOG.info(stageName + ": Finished with success.");
			}
			return ret;
		}
		
		private String getStageName(int taskIndex) {
			return String.format("%s-Stage-%02d-%s", taskChainName,
					taskIndex + 1, taskChain.get(taskIndex).getClass()
							.getSimpleName());
		}
	}
}

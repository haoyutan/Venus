package simjoin.core.exec;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import simjoin.core.SimJoinConf;
import simjoin.core.SimJoinUtils;
import simjoin.core.handler.ItemPartitionHandler;
import simjoin.core.partition.IDPair;
import simjoin.core.partition.PartitionID;
import simjoin.core.partition.VirtualPartitionID;
import simjoin.core.partition.VirtualPartitionInfo;

public class SchedulePartitionPairs extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(SchedulePartitionPairs.class);
	
	public static final String CK_PARTITIONS_DIR = "simjoin.core.exec.partitions_dir"; 
	
	private Path workDir;

	public SchedulePartitionPairs(Configuration conf) {
		setConf(conf);
		workDir = SimJoinConf.getWorkDir(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (recover())
			return 0;
		
		int ret = generateTaskInputFiles();
		if (ret == 0)
			ExecUtils.setExecSuccess(getConf(), workDir);
		return ret;
	}
	
	private boolean recover() throws IOException {
		if (ExecUtils.isExecSuccess(getConf(), workDir)) {
			LOG.info("Found saved results. Skip.");
			return true;
		} else
			return false;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private int generateTaskInputFiles() throws IOException {
		LOG.info("Generating task input files...");
		Configuration conf = getConf();
		
		// read partitions info
		Path partitionsDir = new Path(conf.get(CK_PARTITIONS_DIR));
		Path summaryFile = new Path(partitionsDir, PartitionItems.FILENAME_SUMMARY);
		HashMap<VirtualPartitionID, VirtualPartitionInfo> vpInfoMap =
				VirtualPartitionInfo.readVirtualPartitionInfo(conf, summaryFile, true);
		LOG.info("  Partitions info loaded from " + summaryFile + " ("
				+ vpInfoMap.size() + " partitions).");
		
		// get user-defined partition pairs
		ItemPartitionHandler itemPartitionHandler = SimJoinUtils
				.createItemPartitionHandler(conf);
		itemPartitionHandler.setup(conf);
		List<IDPair<PartitionID>> pairs = itemPartitionHandler
				.getPartitionIdPairs();
		LOG.info("  User-defined partition pairs loaded (" + pairs.size()
				+ " pairs).");
		for (IDPair<PartitionID> pair : pairs)
			LOG.info(pair);
		
		// get arguments for scheduling
		int numTaskSlots = SimJoinConf.getClusterTaskSlots(conf);
		LOG.info("  Arguments for scheduling: numTaskSlots = " + numTaskSlots);
		
		
		
		LOG.info("Generating task input files... Done.");
		return 1;
	}
}

package simjoin.core.exec;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import simjoin.core.SimJoinConf;
import simjoin.core.SimJoinUtils;
import simjoin.core.handler.ItemPartitionHandler;
import simjoin.core.partition.IDPair;
import simjoin.core.partition.IDPairList;
import simjoin.core.partition.VirtualPartitionID;
import simjoin.core.partition.VirtualPartitionInfo;

public class SchedulePartitionPairs extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(SchedulePartitionPairs.class);
	
	public static final String FILENAME_SUMMARY = "_partition_summary.csv";
	
	public static final String TASK_FILENAME_PREFIX = "T-";
	
	public static final String CK_PARTITIONS_DIR = "simjoin.core.exec.partitions_dir"; 
	
	private Path workDir;
	
	private Path partitionsDir, summaryFile;
	
	private Map<VirtualPartitionID, VirtualPartitionInfo> origVpInfoMap, splitVpInfoMap;
	
	private IDPairList origVpPairs, expandedVpPairs;
	
	private int numTasks;
	
	private long minPartitionLength;

	private Map<VirtualPartitionID, List<VirtualPartitionInfo>> origSplitsInfoMap;
	
	private IDPairList[] tasks;
	
	public SchedulePartitionPairs(Configuration conf) {
		setConf(conf);
		workDir = SimJoinConf.getWorkDir(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (recover())
			return 0;
		
		int ret = doScheduling();
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
	
	private int doScheduling() throws IOException {
		readOriginalParititionInfo();
		getUserDefinedPartitionPairs();
		getSchedulingArguments();
		splitPartitions();
		expandVirtualPartitionPairs();
		assignTasks();
		writeVirtualPartitionInfo();
		writeTaskInputFiles();
		return 1;
	}
	
	private void readOriginalParititionInfo() throws IOException {
		Configuration conf = getConf();
		partitionsDir = new Path(conf.get(CK_PARTITIONS_DIR));
		summaryFile = new Path(partitionsDir, PartitionItems.FILENAME_SUMMARY);
		origVpInfoMap = VirtualPartitionInfo.readVirtualPartitionInfo(conf,
				summaryFile, true);
		LOG.info("Partitions info loaded from " + summaryFile + " ("
				+ origVpInfoMap.size() + " partitions).");
	}
	
	@SuppressWarnings({ "rawtypes" })
	private void getUserDefinedPartitionPairs() throws IOException {
		Configuration conf = getConf();
		ItemPartitionHandler itemPartitionHandler = SimJoinUtils
				.createItemPartitionHandler(conf);
		itemPartitionHandler.setup(conf);
		origVpPairs = itemPartitionHandler.getPartitionIdPairs();
		itemPartitionHandler.cleanup(conf);
		LOG.info("User-defined partition pairs loaded (" + origVpPairs.size()
				+ " pairs).");
		
		// write log file
		PrintWriter writer = createLogFileWriter("UserDefinedPartitionPairs");
		writer.println("# Number of user-defined partition pairs: "
				+ origVpPairs.size());
		for (int i = 0; i < origVpPairs.size(); i++)
			writer.println(origVpPairs.get(i));
		writer.close();
	}
	
	private void getSchedulingArguments() throws IOException {
		Configuration conf = getConf();
		numTasks = SimJoinConf.getClusterTaskSlots(conf) * 2;
		minPartitionLength = SimJoinConf
				.getSequenceFileCompressionBlockSize(conf) * 2;
		LOG.info("Arguments for scheduling: numTasks = " + numTasks
				+ ", minPartitionLength = " + minPartitionLength);
		
		// write log file
		PrintWriter writer = createLogFileWriter("SchedulingArguments");
		writer.printf("numTasks = %d\n", numTasks);
		writer.printf("minPartitionLength = %d\n", minPartitionLength);
		writer.close();
	}
	
	// TODO
	private void splitPartitions() throws IOException {
		int totalNumSplits = 0;
		origSplitsInfoMap = new HashMap<VirtualPartitionID, List<VirtualPartitionInfo>>();
		splitVpInfoMap = new HashMap<VirtualPartitionID, VirtualPartitionInfo>();
		for (VirtualPartitionInfo origVpInfo : origVpInfoMap.values()) {
			int numSplits = (int) Math.max(1,
					Math.ceil(((double) origVpInfo.getLength()) / minPartitionLength));
			List<VirtualPartitionInfo> splitInfoList = origVpInfo.getSplits(numSplits);
			origSplitsInfoMap.put(origVpInfo.getId(), splitInfoList);
			totalNumSplits += splitInfoList.size();
			
			for (VirtualPartitionInfo info : splitInfoList)
				splitVpInfoMap.put(info.getId(), info);
		}
		LOG.info("Splitted " + origVpInfoMap.size()
				+ " physical partitions into " + totalNumSplits
				+ " virtual partitions.");

		// write log file
		PrintWriter writer = createLogFileWriter("SplitPartitions");
		writer.println("# Splitted " + origVpInfoMap.size()
				+ " physical partitions into " + totalNumSplits
				+ " virtual partitions.");
		writer.println();
		for (VirtualPartitionID partitionId : origSplitsInfoMap.keySet()) {
			List<VirtualPartitionInfo> infoList = origSplitsInfoMap.get(partitionId);
			writer.print(origVpInfoMap.get(partitionId));
			writer.printf(" [numSplits = %d]\n", infoList.size());
			for (VirtualPartitionInfo info : infoList)
				writer.printf("\t%s\n", info);
			writer.println();
		}
		writer.close();
	}
	
	private void expandVirtualPartitionPairs() throws IOException {
		int numSkipped = 0;
		expandedVpPairs = new IDPairList();
		for (IDPair<VirtualPartitionID> origPair : origVpPairs.getInnerList()) {
			VirtualPartitionID first = origPair.getFirst();
			VirtualPartitionID second = origPair.getSecond();
			List<VirtualPartitionInfo> firstSplitList = origSplitsInfoMap.get(first);
			List<VirtualPartitionInfo> secondSplitList = origSplitsInfoMap.get(second);
			if (firstSplitList == null || secondSplitList == null) {
				++numSkipped;
				continue;
			}
			if (first.equals(second)) {
				// self join, firstSplitList == secondSplitList
				for (int i = 0; i < firstSplitList.size(); i++)
					for (int j = i; j < firstSplitList.size(); j++)
						expandedVpPairs.add(IDPair.makePair(
								firstSplitList.get(i).getId(), firstSplitList
										.get(j).getId()));
			} else {
				// R-S join
				for (VirtualPartitionInfo s1 : firstSplitList)
					for (VirtualPartitionInfo s2 : secondSplitList)
						expandedVpPairs.add(IDPair.makePair(s1.getId(),
								s2.getId()));
			}
		}
		LOG.info("Expanded " + (origVpPairs.size() - numSkipped) + " out of "
				+ origVpPairs.size() + " physical partition pairs into "
				+ expandedVpPairs.size() + " virtual partition pairs.");
		
		// write log file
		PrintWriter writer = createLogFileWriter("ExpandVirtualPartitionPairs");
		writer.println("# Expanded " + (origVpPairs.size() - numSkipped) + " out of "
				+ origVpPairs.size() + " physical partition pairs into "
				+ expandedVpPairs.size() + " virtual partition pairs.");
		for (int i = 0; i < expandedVpPairs.size(); i++)
			writer.println(expandedVpPairs.get(i));
		writer.close();
	}
	
	// TODO
	private void assignTasks() throws IOException {
		tasks = new IDPairList[numTasks];
		for (int i = 0; i < numTasks; i++)
			tasks[i] = new IDPairList();
		
		for (IDPair<VirtualPartitionID> pair : expandedVpPairs.getInnerList())
			tasks[pair.hashCode() % tasks.length].add(pair);
		LOG.info("Tasks assigned.");
		
		// write log file
		PrintWriter writer = createLogFileWriter("AssignTasks");
		writer.printf("# Number of tasks: %d\n\n", tasks.length);
		for (int i = 0; i < tasks.length; i++) {
			writer.printf("task-%05d: %d pairs\n", i, tasks[i].size());
			long ioCost = 0;
			long cpuCost = 0;
			for (IDPair<VirtualPartitionID> pair : tasks[i].getInnerList()) {
				VirtualPartitionInfo firstInfo = splitVpInfoMap.get(pair.getFirst());
				VirtualPartitionInfo secondInfo = splitVpInfoMap.get(pair.getSecond());
				
				long ioCostDelta, cpuCostDelta;
				if (firstInfo == secondInfo) {
					// self join
					ioCostDelta = firstInfo.getLength();
					cpuCostDelta = firstInfo.getNumRecords() * (firstInfo.getNumRecords() - 1) / 2;
				} else {
					ioCostDelta = firstInfo.getLength() + secondInfo.getLength();
					cpuCostDelta = firstInfo.getNumRecords() * secondInfo.getNumRecords();
				}
				ioCost += ioCostDelta;
				cpuCost += cpuCostDelta;
				
				writer.printf("%s, io: %16d, cpu: %16d\n", pair, ioCostDelta, cpuCostDelta);
			}
			writer.printf("Total io cost : %d\nTotal cpu cost: %d\n\n", ioCost, cpuCost);
		}
		writer.close();
	}
	
	private void writeVirtualPartitionInfo() throws IOException {
		Path summaryFile = new Path(workDir, FILENAME_SUMMARY);
		VirtualPartitionInfo.writeVirtualPartitionInfo(getConf(), summaryFile,
				splitVpInfoMap.values(), true);
		LOG.info("Virtual partition info written to " + summaryFile + ".");
	}
	
	private void writeTaskInputFiles() throws IOException {
		for (int i = 0; i < tasks.length; i++) {
			String taskInputFilename = String.format("%s%05d",
					TASK_FILENAME_PREFIX, i);
			PrintWriter writer = createFileWriter(new Path(workDir,
					taskInputFilename));
			for (IDPair<VirtualPartitionID> pair : tasks[i].getInnerList())
				writer.println(pair);
			writer.close();
		}
		LOG.info("Task input files created.");
	}
	
	private PrintWriter createLogFileWriter(String logName) throws IOException {
		return createFileWriter(new Path(workDir, "_log_" + logName + ".log"));
	}
	
	private PrintWriter createFileWriter(Path file) throws IOException {
		FileSystem fs = file.getFileSystem(getConf());
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(fs.create(
				file, true)));
		return writer;
	}
}

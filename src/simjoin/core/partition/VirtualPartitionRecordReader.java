package simjoin.core.partition;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class VirtualPartitionRecordReader extends RecordReader<VirtualPartition, VirtualPartition> {
	
	private Configuration conf;
	
	private IDPairList idPairList;
	
	private VirtualPartition key;
	
	private VirtualPartition value;
	
	private Map<VirtualPartitionID, VirtualPartitionInfo> vpInfoMap;
	
	private VirtualPartitionInfo[] leftVpInfo, rightVpInfo;
	
	private int numPairs;
	
	private int currentPairIndex;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		conf = context.getConfiguration();
		Path taskInput = ((FileSplit) split).getPath();
		idPairList = IDPairList.readIDPairList(conf, taskInput);
		vpInfoMap = VirtualPartitionInfo.readVirtualPartitionInfo(conf,
				fileSplit.getPath().getParent(), true);

		numPairs = idPairList.size();
		currentPairIndex = 0;
		leftVpInfo = new VirtualPartitionInfo[numPairs];
		rightVpInfo = new VirtualPartitionInfo[numPairs];
		scheduleTaskPairs();
	}
	
	// TODO
	private void scheduleTaskPairs() {
		for (int i = 0; i < numPairs; i++) {
			leftVpInfo[i] = vpInfoMap.get(idPairList.getFirst(i));
			rightVpInfo[i] = vpInfoMap.get(idPairList.getSecond(i));
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key != null)
			key.close();
		if (value != null)
			value.close();
		
		if (currentPairIndex == numPairs)
			return false;
		
		key = new VirtualPartition(conf, leftVpInfo[currentPairIndex]);
		value = new VirtualPartition(conf, rightVpInfo[currentPairIndex]);
		
		++currentPairIndex;
		return true;
	}

	@Override
	public VirtualPartition getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public VirtualPartition getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	// TODO: take partition size into account to make it more accurate
	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (numPairs == 0)
			return 1;
		return (float) currentPairIndex / numPairs;
	}

	@Override
	public void close() throws IOException {
	}
}

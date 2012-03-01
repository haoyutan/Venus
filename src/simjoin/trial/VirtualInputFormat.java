package simjoin.trial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;


public abstract class VirtualInputFormat<K, V> extends InputFormat<K, V> {

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		int numSplits = conf.getInt("simjoin.inputformat.num_splits", 3);
		int numSplitRecords = conf.getInt("simjoin.inputformat.num_split_records", 10);
		List<InputSplit> result = new ArrayList<InputSplit>();
		for (int i = 0; i < numSplits; i++)
			result.add(new VirtualSplit("" + i, numSplitRecords));
		return result;
	}
}

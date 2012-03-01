package simjoin.trial;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public abstract class VirtualRecordReader<K, V> extends RecordReader<K, V> {
	
	protected K key;
	
	protected V value;
	
	protected int splitId;
	
	protected long totalRecords, curRecords;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		VirtualSplit vsplit = (VirtualSplit) split;
		splitId = Integer.parseInt(vsplit.getInfo());
		totalRecords = vsplit.getLength();
		curRecords = 0;
	}

	@Override
	public K getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return curRecords / (float) totalRecords;
	}

	@Override
	public void close() throws IOException {
	}
}

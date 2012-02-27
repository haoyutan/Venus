package simjoin.core.partition;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import simjoin.core.ItemWritable;

@SuppressWarnings("rawtypes")
public class VirtualPartition extends Configured implements
		Iterator<ItemWritable>, Iterable<ItemWritable> {
	
	private VirtualPartitionInfo vpInfo;
	
	private VirtualPartitionReader reader;
	
	public VirtualPartition(Configuration conf, VirtualPartitionInfo vpInfo)
			throws IOException {
		super(conf);
		this.vpInfo = vpInfo;
		reset();
	}
	
	public void reset() throws IOException {
		close();
		reader = new VirtualPartitionReader(getConf(), vpInfo);
	}
	
	public VirtualPartitionInfo getVirtualPartitionInfo() {
		return vpInfo;
	}
	
	public synchronized void close() throws IOException {
		if (reader != null)
			reader.close();
		reader = null;
	}

	@Override
	public boolean hasNext() {
		try {
			return reader.nextItem();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ItemWritable next() {
		return reader.getCurrentItem();
	}

	@Override
	public void remove() {
		throw new RuntimeException("Unimplemented method.");
	}

	@Override
	public Iterator<ItemWritable> iterator() {
		return this;
	}
}

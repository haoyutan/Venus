package simjoin.core.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;

import simjoin.core.ItemWritable;

@SuppressWarnings("rawtypes")
public class VirtualPartition extends Configured {
	
	private VirtualPartitionInfo vpInfo;
	
	private VirtualPartitionReader reader;
	
	public VirtualPartition(Configuration conf, VirtualPartitionInfo vpInfo)
			throws IOException {
		super(conf);
		this.vpInfo = vpInfo;
	}
	
	public void open() throws IOException {
		reader = new VirtualPartitionReader(getConf(), vpInfo);
	}
	
	public VirtualPartitionInfo getVirtualPartitionInfo() {
		return vpInfo;
	}
	
	public synchronized void close() throws IOException {
		if (reader != null)
			reader.close();
	}

	public boolean nextItem() {
		try {
			return reader.nextItem();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public ItemWritable getCurrentItem() {
		return getCurrentItem(false);
	}
	
	public ItemWritable getCurrentItem(boolean copy) {
		return (copy ? WritableUtils.clone(reader.getCurrentItem(), getConf())
				: reader.getCurrentItem());
	}

	@Override
	public boolean equals(Object obj) {
		VirtualPartition other = (VirtualPartition) obj;
		return this.getVirtualPartitionInfo().getId()
				.equals(other.getVirtualPartitionInfo().getId());
	}
}

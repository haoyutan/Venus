package simjoin.core.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import simjoin.core.ItemWritable;

// Most codes are copied from SequenceFileRecordReader
public class VirtualPartitionReader extends Configured {
	
	@SuppressWarnings("rawtypes")
	private Class<? extends ItemWritable> itemClass;

	private VirtualPartitionInfo info;
	
	private SequenceFile.Reader in;
	
	@SuppressWarnings("rawtypes")
	private ItemWritable item;
	
	private long start, end;
	
	private boolean more;

	public VirtualPartitionReader(Configuration conf, VirtualPartitionInfo info)
			throws IOException {
		setConf(conf);
		initialize(info);
	}
	
	public VirtualPartitionInfo getVirtualPartitionInfo() {
		return info;
	}
	
	@SuppressWarnings("rawtypes")
	public Class<? extends ItemWritable> getItemClass() {
		return itemClass;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void initialize(VirtualPartitionInfo info) throws IOException {
		this.info = info;
		this.end = info.getStart() + info.getLength();

		Configuration conf = getConf();
		FileSystem fs = info.getPartitionFile().getFileSystem(conf);
		this.in = new SequenceFile.Reader(fs, info.getPartitionFile(), conf);
		this.itemClass = (Class<? extends ItemWritable>) in.getKeyClass();
		this.item = ReflectionUtils.newInstance(itemClass, conf);
		
		if (info.getStart() > in.getPosition())
			in.sync(info.getStart());

		this.start = in.getPosition();
		this.more = start < end;
	}

	public boolean nextItem() throws IOException {
		if (!more)
			return false;
		long pos = in.getPosition();
		more = in.next(item);
		if ((pos >= end && in.syncSeen()))
			more = false;
		return more;
	}
	
	@SuppressWarnings("rawtypes")
	public ItemWritable getCurrentItem() {
		return (more ? item : null);
	}

	public synchronized void close() throws IOException {
		in.close();
	}
}

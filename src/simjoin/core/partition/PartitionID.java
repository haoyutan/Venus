package simjoin.core.partition;

import org.apache.hadoop.io.WritableComparator;

public class PartitionID extends VirtualPartitionID {
	
	public static final long PHYSICAL_PARTITION_SUBID = Long.MAX_VALUE;

	public PartitionID() {
		this(-1);
	}
	
	public PartitionID(long id) {
		super(id, PHYSICAL_PARTITION_SUBID);
	}
	
	public void setId(long id) {
		setMainId(id);
	}
	
	public long getId() {
		return getMainId();
	}
	
	static {
		WritableComparator.define(PartitionID.class, new Comparator());
	}
}

package simjoin.core.partition;

import org.apache.hadoop.io.WritableComparator;

public class PartitionID extends VirtualPartitionID {

	public PartitionID() {
		this(-1);
	}
	
	public PartitionID(long id) {
		super(id);
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

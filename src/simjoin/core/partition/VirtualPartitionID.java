package simjoin.core.partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class VirtualPartitionID implements WritableComparable<VirtualPartitionID> {
	
	public static final long PHYSICAL_PARTITION_SUBID = Long.MAX_VALUE;
	
	public static VirtualPartitionID createFromString(String idStr) {
		long mainId = Long.parseLong(idStr.substring(0, 16), 16);
		long subId = Long.parseLong(idStr.substring(17, 33), 16);
		return new VirtualPartitionID(mainId, subId);
	}
	
	private long mainId;
	
	private long subId;
	
	public VirtualPartitionID() {
		this(-1, PHYSICAL_PARTITION_SUBID);
	}
	
	public VirtualPartitionID(long mainId) {
		this(mainId, PHYSICAL_PARTITION_SUBID);
	}
	
	public VirtualPartitionID(long mainId, long subId) {
		this.mainId = mainId;
		this.subId = subId;
	}

	public long getMainId() {
		return mainId;
	}

	public void setMainId(long mainId) {
		this.mainId = mainId;
	}

	public long getSubId() {
		return subId;
	}

	public void setSubId(long subId) {
		this.subId = subId;
	}
	
	public boolean isPhysicalPartition() {
		return this.subId == PHYSICAL_PARTITION_SUBID;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(mainId);
		out.writeLong(subId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mainId = in.readLong();
		subId = in.readLong();
	}
	
	@Override
	final public boolean equals(Object o) {
		if (!(o instanceof VirtualPartitionID))
			return false;
		VirtualPartitionID other = (VirtualPartitionID) o;
		return (this.mainId == other.mainId && this.subId == other.subId);
	}

	@Override
	final public int hashCode() {
		return (int) (mainId * 997 + subId * 163);
	}
	
	@Override
	final public int compareTo(VirtualPartitionID other) {
		int cmp = this.mainId < other.mainId ? -1
				: (this.mainId == other.mainId ? 0 : 1);
		if (cmp == 0)
			cmp = this.subId < other.subId ? -1
					: (this.subId == other.subId ? 0 : 1);
		return cmp;
	}
	
	@Override
	final public String toString() {
		return String.format("%016X-%016X", mainId, subId);
	}
	
	public static class Comparator extends WritableComparator {

		public Comparator() {
			super(VirtualPartitionID.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			long thisMainId = readLong(b1, s1);
			long thisSubId = readLong(b1, s1 + 8);
			long thatMainId = readLong(b2, s2);
			long thatSubId = readLong(b2, s2 + 8);
			int cmp = thisMainId < thatMainId ? -1
					: (thisMainId == thatMainId ? 0 : 1);
			if (cmp == 0)
				cmp = thisSubId < thatSubId ? -1
						: (thisSubId == thatSubId ? 0 : 1);
			return cmp;
		}
	}
	
	static {
		WritableComparator.define(VirtualPartitionID.class, new Comparator());
	}
}

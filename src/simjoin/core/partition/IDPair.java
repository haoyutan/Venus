package simjoin.core.partition;

public class IDPair<T extends VirtualPartitionID> {

	public static <T extends VirtualPartitionID> IDPair<T> makePair(T first,
			T second) {
		return new IDPair<T>(first, second);
	}
	
	public static IDPair<VirtualPartitionID> createFromString(String pairStr) {
		String[] fields = pairStr.split(",");
		VirtualPartitionID first = VirtualPartitionID.createFromString(fields[0]);
		VirtualPartitionID second = VirtualPartitionID.createFromString(fields[1]);
		return makePair(first, second);
	}
	
	private T first, second;
	
	public IDPair(T first, T second) {
		this.first = first;
		this.second = second;
	}

	public T getFirst() {
		return first;
	}

	public void setFirst(T first) {
		this.first = first;
	}

	public T getSecond() {
		return second;
	}

	public void setSecond(T second) {
		this.second = second;
	}
	
	@Override
	public String toString() {
		return first.toString() + "," + second.toString();
	}
}

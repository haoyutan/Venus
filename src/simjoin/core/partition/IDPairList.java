package simjoin.core.partition;

import java.util.ArrayList;

public class IDPairList {
	
	private ArrayList<IDPair<VirtualPartitionID>> list;

	public IDPairList() {
		this.list = new ArrayList<IDPair<VirtualPartitionID>>();
	}
	
	public void add(IDPair<VirtualPartitionID> pair) {
		list.add(pair);
	}
	
	public void add(VirtualPartitionID id1, VirtualPartitionID id2) {
		add(IDPair.makePair(id1, id2));
	}
	
	public void add(long id1, long id2) {
		add(new PartitionID(id1), new PartitionID(id2));
	}
	
	public IDPair<VirtualPartitionID> get(int index) {
		return list.get(index);
	}
	
	public IDPair<VirtualPartitionID> remove(int index) {
		return list.remove(index);
	}
	
	public int size() {
		return list.size();
	}
	
	public ArrayList<IDPair<VirtualPartitionID>> getInnerList() {
		return list;
	}
}

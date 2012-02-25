package simjoin.core.handler;

import java.util.List;

import simjoin.core.ItemWritable;

@SuppressWarnings("rawtypes")
public abstract class ItemPartitionHandler<ITEM extends ItemWritable> extends
		SetupCleanupHandler {
	
	protected Class<ITEM> itemClass;

	public ItemPartitionHandler(Class<ITEM> itemClass) {
		this.itemClass = itemClass;
	}
	
	public abstract List<Integer> getPartitions(ItemWritable item);
	
	public static class PartitionIdPair {
		
		private int first, second;
		
		public PartitionIdPair(int first, int second) {
			this.first = first;
			this.second = second;
		}

		public int getFirst() {
			return first;
		}

		public void setFirst(int first) {
			this.first = first;
		}

		public int getSecond() {
			return second;
		}

		public void setSecond(int second) {
			this.second = second;
		}
		
		@Override
		public String toString() {
			return "(" + first + ", " + second + ")";
		}
	}
	
	public abstract List<PartitionIdPair> getPartitionIdPairs();
}

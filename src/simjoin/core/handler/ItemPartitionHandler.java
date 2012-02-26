package simjoin.core.handler;

import java.util.List;

import simjoin.core.ItemWritable;
import simjoin.core.partition.IDPairList;
import simjoin.core.partition.PartitionID;

@SuppressWarnings("rawtypes")
public abstract class ItemPartitionHandler<ITEM extends ItemWritable> extends
		SetupCleanupHandler {
	
	protected Class<ITEM> itemClass;

	public ItemPartitionHandler(Class<ITEM> itemClass) {
		this.itemClass = itemClass;
	}
	
	public abstract List<PartitionID> getPartitions(ItemWritable item);
	
	public abstract IDPairList getPartitionIdPairs();
}

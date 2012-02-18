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
}

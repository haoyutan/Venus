package simjoin.core.handler;

import simjoin.core.ItemWritable;

@SuppressWarnings("rawtypes")
public class ItemSetupCleanupHandler<ITEM extends ItemWritable> extends SetupCleanupHandler {
	
	protected Class<ITEM> itemClass;

	public ItemSetupCleanupHandler(Class<ITEM> itemClass) {
		this.itemClass = itemClass;
	}
}

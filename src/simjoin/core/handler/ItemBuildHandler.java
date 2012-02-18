package simjoin.core.handler;

import simjoin.core.ItemWritable;

@SuppressWarnings("rawtypes")
public abstract class ItemBuildHandler<KEYIN, VALUEIN, ITEM extends ItemWritable>
		extends SetupCleanupHandler {
	
	protected Class<ITEM> itemClass;

	public ItemBuildHandler(Class<ITEM> itemClass) {
		this.itemClass = itemClass;
	}
	
	public ITEM createItem() {
		ITEM item = null;
		try {
			item = itemClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return item;
	}
	
	public ITEM buildItem(KEYIN key, VALUEIN value) {
		ITEM item = createItem();
		resetItem(item, key, value);
		return item;
	}
	
	public abstract void resetItem(ItemWritable item, KEYIN key, VALUEIN value);
}

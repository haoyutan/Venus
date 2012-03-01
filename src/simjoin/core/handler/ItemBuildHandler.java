package simjoin.core.handler;

import simjoin.core.ItemWritable;

@SuppressWarnings("rawtypes")
public abstract class ItemBuildHandler<KEYIN, VALUEIN, ITEM extends ItemWritable>
		extends ItemSetupCleanupHandler<ITEM> {

	public ItemBuildHandler(Class<ITEM> itemClass) {
		super(itemClass);
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
	
	public void resetItem(ItemWritable item, KEYIN key, VALUEIN value) {
		resetItem(item, key, value, ItemWritable.MASK_ALL);
	}
	
	public abstract void resetItem(ItemWritable item, KEYIN key, VALUEIN value, int mask);
}

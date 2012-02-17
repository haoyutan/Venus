package simjoin.core;


@SuppressWarnings("rawtypes")
public abstract class SimJoinHandler<KEYIN, VALUEIN, ITEM extends ItemWritable> {
	
	private Class<ITEM> itemClass;

	public SimJoinHandler(Class<ITEM> itemClass) {
		this.itemClass = itemClass;
	}
	
	public ITEM buildItem(KEYIN key, VALUEIN value) {
		ITEM item = null;
		try {
			item = itemClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		resetItem(item, key, value);
		return item;
	}
	
	protected abstract void resetItem(ITEM item, KEYIN key, VALUEIN value);
}

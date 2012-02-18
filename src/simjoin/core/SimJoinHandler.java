package simjoin.core;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;


@SuppressWarnings("rawtypes")
public abstract class SimJoinHandler<KEYIN, VALUEIN, ITEM extends ItemWritable> {
	
	private Class<ITEM> itemClass;

	public SimJoinHandler(Class<ITEM> itemClass) {
		this.itemClass = itemClass;
	}
	
	public void setupBuildItem(Configuration conf) {
	}
	
	public void cleanupBuildItem(Configuration conf) {
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
	
	public void setupGetPartitions(Configuration conf) throws IOException {
	}
	
	public void cleanupGetPartitions(Configuration conf) throws IOException {
	}
	
	public abstract List<Integer> getPartitions(ItemWritable item);
}

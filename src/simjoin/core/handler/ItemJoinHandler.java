package simjoin.core.handler;

import java.io.IOException;

import simjoin.core.ItemWritable;
import simjoin.core.partition.VirtualPartition;

@SuppressWarnings("rawtypes")
public abstract class ItemJoinHandler<ITEM extends ItemWritable> extends
		ItemSetupCleanupHandler<ITEM> {
	
	private int itemOutputMask = ItemWritable.MASK_ID;
	
	public ItemJoinHandler(Class<ITEM> itemClass) {
		super(itemClass);
	}

	public abstract void joinItem(VirtualPartition vp1, VirtualPartition vp2)
			throws IOException, InterruptedException;
	
	public abstract void joinSignature(VirtualPartition vp1, VirtualPartition vp2)
			throws IOException, InterruptedException;
	
	public abstract void joinPayload(VirtualPartition vp1, VirtualPartition vp2)
			throws IOException, InterruptedException;
	
	public void setItemOutputMask(int itemOutputMask) {
		this.itemOutputMask = itemOutputMask;
	}
	
	public int getItemOutputMask() {
		return itemOutputMask;
	}
	
	@SuppressWarnings("unchecked")
	protected void output(ITEM item1, ITEM item2)
			throws IOException, InterruptedException {
		int origMask1 = item1.getMask();
		int origMask2 = item2.getMask();
		
		item1.setMask(itemOutputMask);
		item2.setMask(itemOutputMask);
		taskContext.write(item1, item2);
		
		item1.setMask(origMask1);
		item2.setMask(origMask2);
	}
}

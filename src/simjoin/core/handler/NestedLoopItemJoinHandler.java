package simjoin.core.handler;

import java.io.IOException;
import java.util.ArrayList;

import simjoin.core.ItemWritable;
import simjoin.core.partition.VirtualPartition;

@SuppressWarnings("rawtypes")
public class NestedLoopItemJoinHandler<ITEM extends ItemWritable> extends
		ItemJoinHandler<ITEM> {
	
	public NestedLoopItemJoinHandler() {
		this(null);
	}

	public NestedLoopItemJoinHandler(Class<ITEM> itemClass) {
		super(itemClass);
	}
	
	private interface Acceptor<ITEM> {
		boolean accept(ITEM item1, ITEM item2);
	}

	@SuppressWarnings({ "unchecked" })
	protected void join(VirtualPartition vp1, VirtualPartition vp2,
			Acceptor<ITEM> acceptor) throws IOException, InterruptedException {
		if (vp1.equals(vp2)) {
			// self join
			vp1.open();
			ArrayList<ItemWritable> itemList = new ArrayList<ItemWritable>();
			while (vp1.nextItem())
				itemList.add(vp1.getCurrentItem(true));
			vp1.close();
			for (int i = 0; i < itemList.size(); i++) {
				for (int j = i + 1; j < itemList.size(); j++) {
					ITEM item1 = (ITEM) itemList.get(i);
					ITEM item2 = (ITEM) itemList.get(j);
					if (acceptor.accept(item1, item2))
						output(item1, item2);
				}
			}
		} else {
			// R-S join
			vp1.open();
			ArrayList<ItemWritable> itemList1 = new ArrayList<ItemWritable>();
			while (vp1.nextItem())
				itemList1.add(vp1.getCurrentItem(true));
			vp1.close();
			
			vp2.open();
			ArrayList<ItemWritable> itemList2 = new ArrayList<ItemWritable>();
			while (vp2.nextItem())
				itemList2.add(vp2.getCurrentItem(true));
			vp2.close();
			
			for (int i = 0; i < itemList1.size(); i++) {
				for (int j = 0; j < itemList2.size(); j++) {
					ITEM item1 = (ITEM) itemList1.get(i);
					ITEM item2 = (ITEM) itemList2.get(j);
					if (acceptor.accept(item1, item2))
						output(item1, item2);
				}
			}
		}
	}
	
	@Override
	public void joinItem(VirtualPartition vp1, VirtualPartition vp2)
			throws IOException, InterruptedException {
		join(vp1, vp2, new Acceptor<ITEM>() {
			@Override
			public boolean accept(ITEM item1, ITEM item2) {
				return acceptItemPair(item1, item2);
			}
		});
	}
	
	protected boolean acceptItemPair(ITEM item1, ITEM item2) {
		return acceptSignaturePair(item1, item2)
				&& acceptPayloadPair(item1, item2);
	}

	@Override
	public void joinSignature(VirtualPartition vp1, VirtualPartition vp2)
			throws IOException, InterruptedException {
		join(vp1, vp2, new Acceptor<ITEM>() {
			@Override
			public boolean accept(ITEM item1, ITEM item2) {
				return acceptSignaturePair(item1, item2);
			}
		});
	}
	
	protected boolean acceptSignaturePair(ITEM item1, ITEM item2) {
		return item1.getSignature().equals(item2.getSignature());
	}

	@Override
	public void joinPayload(VirtualPartition vp1, VirtualPartition vp2)
			throws IOException, InterruptedException {
		join(vp1, vp2, new Acceptor<ITEM>() {
			@Override
			public boolean accept(ITEM item1, ITEM item2) {
				return acceptPayloadPair(item1, item2);
			}
		});
	}
	
	protected boolean acceptPayloadPair(ITEM item1, ITEM item2) {
		return item1.getPayload().equals(item2.getPayload());
	}
}

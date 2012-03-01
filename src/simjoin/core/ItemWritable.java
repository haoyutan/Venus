package simjoin.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings("rawtypes")
public abstract class ItemWritable<I extends WritableComparable, P extends Writable, S extends Writable>
		implements WritableComparable<ItemWritable> {
	
	public static final int MASK_ID = 0x1;
	public static final int MASK_PLD = 0x2;
	public static final int MASK_SIG = 0x4;
	public static final int MASK_ALL = MASK_ID | MASK_PLD | MASK_SIG;
	
	public static final int MASK_TAG = 0x40000000;
	
	public static ItemWritable copy(Configuration conf, ItemWritable src,
			ItemWritable dst, int mask) throws IOException {
		int origMask = src.getMask();
		src.setMask(mask);
		ReflectionUtils.copy(conf, src, dst);
		src.setMask(origMask);
		return dst;
	}	
	
	public static ItemWritable copy(Configuration conf, ItemWritable src,
			ItemWritable dst) throws IOException {
		return ReflectionUtils.copy(conf, src, dst);
	}
	
	public static ItemWritable clone(Configuration conf, ItemWritable orig,
			int mask) {
		int origMask = orig.getMask();
		orig.setMask(mask);
		ItemWritable copy = WritableUtils.clone(orig, conf);
		orig.setMask(origMask);
		return copy;
	}

	public static ItemWritable clone(Configuration conf, ItemWritable orig) {
		return WritableUtils.clone(orig, conf);
	}
	
	protected Class<? extends Writable> idClass, payloadClass, signatureClass;
	
	protected I id;
	protected P payload;
	protected S signature;
	private int mask = MASK_ID | MASK_PLD;

	@SuppressWarnings("unchecked")
	public ItemWritable(Class<? extends Writable> idClass,
			Class<? extends Writable> payloadClass,
			Class<? extends Writable> signatureClass,
			boolean createFields) {
		this.idClass = idClass;
		this.payloadClass = payloadClass;
		this.signatureClass = signatureClass;
		
		if (createFields) {
			try {
				id = (I) idClass.newInstance();
				payload = (P) payloadClass.newInstance();
				signature = (S) signatureClass.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public int getMask() {
		return mask;
	}

	public void setMask(int mask) {
		this.mask = mask;
	}
	
	public void setTag(boolean tag) {
		if (tag)
			mask |= MASK_TAG;
		else
			mask &= ~MASK_TAG;
	}
	
	public boolean hasTag() {
		return (mask & MASK_TAG) != 0;
	}

	public I getId() {
		return id;
	}

	public void setId(I id) {
		this.id = id;
	}

	public P getPayload() {
		return payload;
	}

	public void setPayload(P payload) {
		this.payload = payload;
	}

	public S getSignature() {
		return signature;
	}

	public void setSignature(S signature) {
		this.signature = signature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mask = in.readInt();
		if ((mask & MASK_ID) != 0)
			id.readFields(in);
		if ((mask & MASK_PLD) != 0)
			payload.readFields(in);
		if ((mask & MASK_SIG) != 0)
			signature.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(mask);
		if ((mask & MASK_ID) != 0)
			id.write(out);
		if ((mask & MASK_PLD) != 0)
			payload.write(out);
		if ((mask & MASK_SIG) != 0)
			signature.write(out);
	}
	
	@Override
	public String toString() {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.append('[');
		if ((mask & MASK_ID) != 0)
			pw.append('I');
		if ((mask & MASK_PLD) != 0)
			pw.append('P');
		if ((mask & MASK_SIG) != 0)
			pw.append('S');
		pw.append(']');
		
		if ((mask & MASK_ID) != 0)
			pw.format(",,{%s}", id.toString());
		if ((mask & MASK_PLD) != 0)
			pw.format(",,{%s}", payload.toString());
		if ((mask & MASK_SIG) != 0)
			pw.format(",,{%s}", signature.toString());
		
		return sw.toString();
	}
	
	@Override
	final public int hashCode() {
		return id.hashCode();
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	final public int compareTo(ItemWritable o) {
		return getId().compareTo(o.getId());
	}
	
	@Override
	final public boolean equals(Object o) {
		return getId().equals(((ItemWritable) o).getId());
	}
	
	// NOTE: some codes are copied from WritableComparator
	// TODO: optimization
	public static abstract class Comparator extends Configured implements
			RawComparator<ItemWritable> {
		
		private Class<? extends ItemWritable> itemClass;
		
		private DataInputBuffer buffer;
		
		private ItemWritable item1, item2;

		@Override
		public void setConf(Configuration conf) {
			if (conf != null) {
				itemClass = SimJoinConf.getItemClass(conf);
				item1 = ReflectionUtils.newInstance(itemClass, conf);
				item2 = ReflectionUtils.newInstance(itemClass, conf);
				buffer = new DataInputBuffer();
			}
		}

		@Override
		public abstract int compare(ItemWritable o1, ItemWritable o2);

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				buffer.reset(b1, s1, l1); // parse item1
				item1.readFields(buffer);

				buffer.reset(b2, s2, l2); // parse item2
				item2.readFields(buffer);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			return compare(item1, item2); // compare them
		}
	}
	
	// TODO: optimization
	public static class ItemIDComparator extends ItemWritable.Comparator {
		@Override
		public int compare(ItemWritable i1, ItemWritable i2) {
			return i1.compareTo(i2);
		}
	}
	
	// TODO: optimization
	public static class ItemIDTagComparator extends ItemWritable.Comparator {
		@SuppressWarnings({ "unchecked" })
		@Override
		public int compare(ItemWritable i1, ItemWritable i2) {
			int cmp = i1.getId().compareTo(i2.getId());
			if (cmp == 0)
				cmp = i1.hasTag() ? -1 : (i2.hasTag() ? 1 : 0);
			return cmp;
		}
	}
}

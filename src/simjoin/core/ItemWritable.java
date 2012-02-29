package simjoin.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public abstract class ItemWritable<I extends WritableComparable, P extends Writable, S extends Writable>
		implements WritableComparable<ItemWritable> {
	
	public static final int MASK_ID = 0x1;
	public static final int MASK_PLD = 0x2;
	public static final int MASK_SIG = 0x4;
	
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
	public int hashCode() {
		return id.hashCode();
	}
}

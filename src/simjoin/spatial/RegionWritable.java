package simjoin.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;

public class RegionWritable implements Writable {
	
	private int numPoints;
	
	private double x[], y[];

	public int getNumPoints() {
		return numPoints;
	}

	public void setNumPoints(int numPoints) {
		this.numPoints = numPoints;
	}

	public double[] getX() {
		return x;
	}

	public void setX(double[] x) {
		this.x = x;
	}

	public double[] getY() {
		return y;
	}

	public void setY(double[] y) {
		this.y = y;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(numPoints);
		for (int i = 0; i < numPoints; i++) {
			out.writeDouble(x[i]);
			out.writeDouble(y[i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		numPoints = in.readInt();
		x = new double[numPoints];
		y = new double[numPoints];
		for (int i = 0; i < numPoints; i++) {
			x[i] = in.readDouble();
			y[i] = in.readDouble();
		}
	}
	
	@Override
	public String toString() {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.append('[');
		pw.print(numPoints);
		pw.append(']');
		for (int i = 0; i < numPoints; i++)
			pw.format(",(%f,%f)", x[i], y[i]);
		return sw.toString();
	}
}

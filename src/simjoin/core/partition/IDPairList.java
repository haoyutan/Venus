package simjoin.core.partition;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IDPairList {
	
	private ArrayList<IDPair<VirtualPartitionID>> list;

	public IDPairList() {
		this.list = new ArrayList<IDPair<VirtualPartitionID>>();
	}
	
	public void add(IDPair<VirtualPartitionID> pair) {
		list.add(pair);
	}
	
	public void add(VirtualPartitionID id1, VirtualPartitionID id2) {
		add(IDPair.makePair(id1, id2));
	}
	
	public void add(long id1, long id2) {
		add(new PartitionID(id1), new PartitionID(id2));
	}
	
	public IDPair<VirtualPartitionID> get(int index) {
		return list.get(index);
	}
	
	public VirtualPartitionID getFirst(int index) {
		return list.get(index).getFirst();
	}
	
	public VirtualPartitionID getSecond(int index) {
		return list.get(index).getSecond();
	}
	
	public IDPair<VirtualPartitionID> remove(int index) {
		return list.remove(index);
	}
	
	public int size() {
		return list.size();
	}
	
	public ArrayList<IDPair<VirtualPartitionID>> getInnerList() {
		return list;
	}
	
	public static void writeIDPairList(Configuration conf, Path file,
			IDPairList idPairList, boolean overwrite) throws IOException {
		FileSystem fs = file.getFileSystem(conf);
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(fs.create(
				file, overwrite)));
		for (IDPair<VirtualPartitionID> pair : idPairList.getInnerList())
			writer.println(pair);
		writer.close();
	}
	
	public static IDPairList readIDPairList(Configuration conf, Path file)
			throws IOException {
		IDPairList idPairList = new IDPairList();
		FileSystem fs = file.getFileSystem(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs.open(file)));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] fields = line.split(",");
			VirtualPartitionID first = VirtualPartitionID.createFromString(fields[0]);
			VirtualPartitionID second = VirtualPartitionID.createFromString(fields[1]);
			idPairList.add(IDPair.makePair(first, second));
		}
		reader.close();
		return idPairList;
	}
}

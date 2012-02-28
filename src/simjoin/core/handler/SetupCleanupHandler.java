package simjoin.core.handler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public abstract class SetupCleanupHandler {
	
	@SuppressWarnings("rawtypes")
	protected TaskInputOutputContext taskContext;
	
	@SuppressWarnings("rawtypes")
	final public void setup(TaskInputOutputContext context) throws IOException {
		this.taskContext = context;
		coreSetup();
		userSetup(context.getConfiguration());
	}
	
	@SuppressWarnings("rawtypes")
	final public void cleanup(TaskInputOutputContext context) throws IOException {
		userCleanup(context.getConfiguration());
		coreCleanup();
	}

	final public void setup(Configuration conf) throws IOException {
		this.taskContext = null;
		coreSetup();
		userSetup(conf);
	}

	final public void cleanup(Configuration conf) throws IOException {
		userCleanup(conf);
		coreCleanup();
	}

	protected void userSetup(Configuration conf) throws IOException {
	}
	
	protected void userCleanup(Configuration conf) throws IOException {
	}
	
	protected void coreSetup() throws IOException {
	}
	
	protected void coreCleanup() throws IOException {
	}
}

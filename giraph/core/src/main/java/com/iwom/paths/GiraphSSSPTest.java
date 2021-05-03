package com.iwom.paths;

import com.iwom.LongDoubleFloatDoubleEdgeInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GiraphSSSPTest implements Tool {

  private Configuration conf;

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public int run(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];
    GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
    giraphConf.set("SimpleShortestPathsVertex.sourceId", args[2]);
    giraphConf.setComputationClass(SimpleShortestPathsComputation.class);
    giraphConf.setEdgeInputFormatClass(LongDoubleFloatDoubleEdgeInputFormat.class);
    GiraphFileInputFormat.addEdgeInputPath(giraphConf, new Path(inputPath));
    giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    giraphConf.setLocalTestMode(true);
    giraphConf.setWorkerConfiguration(1, 1, 100);
    giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
    InMemoryVertexOutputFormat.initializeOutputGraph(giraphConf);
    GiraphJob giraphJob = new GiraphJob(giraphConf, "GiraphDemo");
    FileOutputFormat.setOutputPath(giraphJob.getInternalJob(), new Path(outputPath));
    giraphJob.run(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GiraphSSSPTest(), args);
  }
}

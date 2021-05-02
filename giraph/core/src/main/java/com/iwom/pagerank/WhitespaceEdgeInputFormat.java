package com.iwom.pagerank;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

public class WhitespaceEdgeInputFormat extends TextEdgeInputFormat<LongWritable, NullWritable> {
  private static final Pattern SEPARATOR = Pattern.compile("\\s+");

  public WhitespaceEdgeInputFormat() {
  }

  public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new WhitespaceEdgeReader();
  }

  public class WhitespaceEdgeReader extends TextEdgeInputFormat<LongWritable, NullWritable>.TextEdgeReaderFromEachLineProcessed<IntPair> {
    public WhitespaceEdgeReader() {
      super();
    }

    @Override
    protected IntPair preprocessLine(Text text) throws IOException {
      String[] tokens = WhitespaceEdgeInputFormat.SEPARATOR.split(text.toString());
      return new IntPair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
    }

    @Override
    protected LongWritable getTargetVertexId(IntPair intPair) throws IOException {
      return new LongWritable(intPair.getFirst());
    }

    @Override
    protected LongWritable getSourceVertexId(IntPair intPair) throws IOException {
      return new LongWritable(intPair.getSecond());
    }

    @Override
    protected NullWritable getValue(IntPair intPair) throws IOException {
      return NullWritable.get();
    }
  }
}

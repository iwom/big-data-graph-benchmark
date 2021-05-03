package com.iwom;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

public class LongDoubleFloatDoubleEdgeInputFormat extends TextEdgeInputFormat<LongWritable, FloatWritable> {
  private static final Pattern SEPARATOR = Pattern.compile("\\s+");

  public LongDoubleFloatDoubleEdgeInputFormat() {
  }

  public EdgeReader<LongWritable, FloatWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new WhitespaceEdgeReader();
  }

  public class WhitespaceEdgeReader extends TextEdgeInputFormat<LongWritable, FloatWritable>.TextEdgeReaderFromEachLineProcessed<IntPair> {
    public WhitespaceEdgeReader() {
      super();
    }

    @Override
    protected IntPair preprocessLine(Text text) throws IOException {
      String[] tokens = LongDoubleFloatDoubleEdgeInputFormat.SEPARATOR.split(text.toString());
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
    protected FloatWritable getValue(IntPair intPair) throws IOException {
      return new FloatWritable(1.0f);
    }
  }
}

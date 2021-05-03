package com.iwom;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

public class DoubleDoubleFloatDoubleEdgeInputFormat extends TextEdgeInputFormat<DoubleWritable, FloatWritable> {
  private static final Pattern SEPARATOR = Pattern.compile("\\s+");

  public DoubleDoubleFloatDoubleEdgeInputFormat() {
  }

  public EdgeReader<DoubleWritable, FloatWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new WhitespaceEdgeReader();
  }

  public class WhitespaceEdgeReader extends TextEdgeReaderFromEachLineProcessed<IntPair> {
    public WhitespaceEdgeReader() {
      super();
    }

    @Override
    protected IntPair preprocessLine(Text text) throws IOException {
      String[] tokens = DoubleDoubleFloatDoubleEdgeInputFormat.SEPARATOR.split(text.toString());
      return new IntPair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
    }

    @Override
    protected DoubleWritable getTargetVertexId(IntPair intPair) throws IOException {
      return new DoubleWritable(intPair.getSecond());
    }

    @Override
    protected DoubleWritable getSourceVertexId(IntPair intPair) throws IOException {
      return new DoubleWritable(intPair.getFirst());
    }

    @Override
    protected FloatWritable getValue(IntPair intPair) throws IOException {
      return new FloatWritable(1.0f);
    }
  }
}

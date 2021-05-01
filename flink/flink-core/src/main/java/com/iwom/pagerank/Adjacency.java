package com.iwom.pagerank;

import java.util.List;

public class Adjacency {
  public Long id;
  public List<Long> neighbours;

  public Adjacency(Long id, List<Long> neighbours) {
    this.id = id;
    this.neighbours = neighbours;
  }

  public Adjacency() {
  }
}

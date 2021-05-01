package com.iwom.pagerank;

public class Page {
  public Long id;
  public Double rank;

  public Page(Long id, Double rank) {
    this.id = id;
    this.rank = rank;
  }

  public Page() {
  }

  @Override
  public String toString() {
    return "Page{" +
      "id=" + id +
      ", rank=" + rank +
      '}';
  }
}

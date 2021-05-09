package com.iwom;

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
    return id.toString() + " " + rank.toString();
  }
}

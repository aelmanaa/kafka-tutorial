package com.sbibits.ptrc.tutorials.kafka.advanced;

import io.advantageous.boon.json.JsonFactory;

/**
 * Holds a stock price has a name, dollar, and cents
 *
 */
public class StockPrice {

  private final int dollars;
  private final int cents;
  private final String name;

  public StockPrice(final String json) {
    this(JsonFactory.fromJson(json, StockPrice.class));
  }

  public StockPrice() {
    dollars = 0;
    cents = 0;
    name = "";
  }

  public StockPrice(final StockPrice stockPrice) {
    this.cents = stockPrice.cents;
    this.dollars = stockPrice.dollars;
    this.name = stockPrice.name;
  }



  public StockPrice(String name,int dollars, int cents) {
    this.dollars = dollars;
    this.cents = cents;
    this.name = name;
  }

  public int getDollars() {
    return dollars;
  }

  public int getCents() {
    return cents;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "StockPrice [dollars=" + dollars + ", cents=" + cents + ", name=" + name + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + cents;
    result = prime * result + dollars;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    StockPrice other = (StockPrice) obj;
    if (cents != other.cents)
      return false;
    if (dollars != other.dollars)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }



  public String toJson() {
    return "{" + "\"dollars\": " + dollars + ", \"cents\": " + cents + ", \"name\": \"" + name
        + '\"' + '}';
  }
}

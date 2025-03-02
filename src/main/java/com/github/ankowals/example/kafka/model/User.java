package com.github.ankowals.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class User {

  private String name;

  @JsonProperty("favorite_number")
  private int favorite_number;

  @JsonProperty("favorite_color")
  private String favorite_color;

  public User(String name, int favorite_number, String favorite_color) {
    this.name = name;
    this.favorite_number = favorite_number;
    this.favorite_color = favorite_color;
  }

  public User() {}

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getFavorite_number() {
    return favorite_number;
  }

  public void setFavorite_number(int favorite_number) {
    this.favorite_number = favorite_number;
  }

  public String getFavorite_color() {
    return favorite_color;
  }

  public void setFavorite_color(String favorite_color) {
    this.favorite_color = favorite_color;
  }
}

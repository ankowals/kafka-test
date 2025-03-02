package com.github.ankowals.example.kafka.model;

import java.util.List;
import lombok.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Subscriber {

  int id;
  String fname;
  String lname;
  String phone_number;
  int age;
  @Singular List<EmailAddress> emailAddresses;
}

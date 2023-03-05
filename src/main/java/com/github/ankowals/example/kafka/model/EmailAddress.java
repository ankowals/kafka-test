package com.github.ankowals.example.kafka.model;

import lombok.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class EmailAddress {

    String email;
    boolean address;

}

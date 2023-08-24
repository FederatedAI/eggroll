package com.eggroll.core.pojo;

import lombok.Data;

@Data
public class Tuple<F, S, T> {
    private F first;
    private S second;
    private T third;

    public Tuple(F first, S second, T third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}
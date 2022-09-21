package com.example.noc;

public class Order {
    public Long user;
    public int productId;
    public int amount;

    // for POJO detection in DataStream API
    public Order() {}

    // for structured type detection in Table API
    public Order(Long user, int productId, int amount) {
        this.user = user;
        this.productId = productId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Order{"
                + "user="
                + user
                + ", product='"
                + productId
                + '\''
                + ", amount="
                + amount
                + '}';
    }
}

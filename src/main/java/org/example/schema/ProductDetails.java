package org.example.schema;

public class ProductDetails {
    public String description;
    public int productId;
    public long costPrice;

    public ProductDetails(String description, int productId, long costPrice) {
        this.description = description;
        this.productId = productId;
        this.costPrice = costPrice;
    }

    public ProductDetails() {}

    @Override
    public String toString() {
        return "ProductDetails{" +
                "description='" + description + '\'' +
                ", productId=" + productId +
                ", costPrice=" + costPrice +
                '}';
    }
}

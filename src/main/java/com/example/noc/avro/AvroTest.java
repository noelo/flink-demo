package com.example.noc.avro;

import org.example.schema.FixedIncomeReturnMacroSchema;
import org.example.schema.Order;
import org.example.schema.ProductDetails;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class AvroTest {
    public static void main(String[] args) throws Exception {
        // Generate schema using Reflect
        Schema schema = ReflectData.get().getSchema(ProductDetails.class);
        System.out.println("ProductDetails==>" + schema + "\n");

        Schema schema2 = ReflectData.get().getSchema(Order.class);
        System.out.println("Order==> " + schema2 + "\n");

        Schema schema3 = ReflectData.get().getSchema(FixedIncomeReturnMacroSchema.class);
        System.out.println("FixedIncomeReturnMacroSchema==> " + schema2 + "\n");
    }
}

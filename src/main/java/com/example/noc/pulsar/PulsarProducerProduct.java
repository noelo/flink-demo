package com.example.noc.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.example.schema.ProductDetails;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PulsarProducerProduct {

    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .allowTlsInsecureConnection(Boolean.TRUE)
                .enableTlsHostnameVerification(Boolean.FALSE)
                .tlsTrustCertsFilePath("/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .serviceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .enableTcpNoDelay(Boolean.TRUE)
                .statsInterval(5, TimeUnit.MINUTES)
                .build();

        Producer<ProductDetails> pulsarProducerProduct = client.newProducer(JSONSchema.of(ProductDetails.class))
                .producerName("ProductProducer")
                .topic("ProductTopic")
                .enableBatching(Boolean.TRUE)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(10000)
                .blockIfQueueFull(true)
                .create();

        Random rand = new Random();
        for (int i = 0; i <= 10; i++) {
            ProductDetails product = new ProductDetails("testProduct description for product"+i, i, rand.nextLong());
            pulsarProducerProduct
                    .newMessage().value(product)
                    .eventTime(System.currentTimeMillis())
                    .send();
        }

        pulsarProducerProduct.flush();
        pulsarProducerProduct.close();
        client.close();
    }
}
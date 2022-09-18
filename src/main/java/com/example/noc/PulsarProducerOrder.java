package com.example.noc;

import com.google.gson.Gson;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.util.concurrent.TimeUnit;

public class PulsarProducerOrder {

    public static void main(String[] args) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .allowTlsInsecureConnection(Boolean.TRUE)
                .enableTlsHostnameVerification(Boolean.FALSE)
                .tlsTrustCertsFilePath("/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .serviceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .enableTcpNoDelay(Boolean.TRUE)
                .statsInterval(5, TimeUnit.MINUTES)
                .build();

        Producer<Order> pulsarProducerFixedIncome = client.newProducer(JSONSchema.of(Order.class))
                .producerName("OrderProducer")
                .topic("OrderDataTopic")
                .enableBatching(Boolean.TRUE)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(10000)
                .blockIfQueueFull(true)
                .create();

        Order myOrder = new Order();
        myOrder.product = "testproduct2";
        myOrder.amount = 99;
        myOrder.user = 999999L;

        Gson gson = new Gson();
        String json = gson.toJson(myOrder);
        System.out.println(json);

        pulsarProducerFixedIncome
                .newMessage().value(myOrder)
                .eventTime(System.currentTimeMillis())
                .send();

        pulsarProducerFixedIncome.flush();
        pulsarProducerFixedIncome.close();
        client.close();
    }
}

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package software.amazon.samples.kafka.lambda;

import static software.amazon.lambda.powertools.utilities.jmespath.Base64Function.decode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.tracing.Tracing;

public class SimpleApiGatewayKafkaProxy implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // private static final Logger log = LogManager.getLogger(SimpleApiGatewayKafkaProxy.class);
    public KafkaProducerPropertiesFactory kafkaProducerProperties = new KafkaProducerPropertiesFactoryImpl();
    private KafkaProducer<String, String> producer;

    @Override
    @Tracing
    @Logging(logEvent = true)
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        APIGatewayProxyResponseEvent response = createEmptyResponse();
        try {
            // log.debug("handleRequest - inpunt: " + input);
            System.out.println("1.handleRequest - inpunt: " + input);

            String message = getMessageBody(input);

            String topic = input.getPathParameters()
                    .get("topic");

            String key = (input.getQueryStringParameters() != null) ? input.getQueryStringParameters()
                    .get("key") : null;
            String partitionS = (input.getQueryStringParameters() != null) ? input.getQueryStringParameters()
                    .get("partition") : null;

            // log.debug("handleRequest - message: " + message);
            System.out.println("2.handleRequest K[" + key + "] T[" + topic + "] P[" + partitionS + "] - " + message);

            KafkaProducer<String, String> producer = createProducer();

            if (key == null || key.trim()
                    .length() < 1) {
                key = context.getAwsRequestId();
            }
            ProducerRecord<String, String> record = null;

            if (partitionS != null && partitionS.trim()
                    .length() > 0) {
                record = new ProducerRecord<String, String>(topic, Integer.parseInt(partitionS), key, message);
            } else {
                record = new ProducerRecord<String, String>(topic, key, message);
            }

            System.out.println("3.sending");
            Future<RecordMetadata> send = producer.send(record);
            System.out.println("4.sent");
            producer.flush();
            System.out.println("5.flushed");
            RecordMetadata metadata = send.get();

            // log.info(String.format("Message was send to partition %s", metadata.partition()));
            System.out.println(String.format("6.Message was send to partition %s", metadata.partition()));

            return response.withStatusCode(200).withBody("Message successfully pushed to kafka");
        } catch (Exception e) {
            // log.error(e.getMessage(), e);
            System.out.println("EEE: " + e);
            return response.withBody(e.getMessage()).withStatusCode(500);
        }
    }

    @Tracing
    private KafkaProducer<String, String> createProducer() {
        if (producer == null) {
            // log.info("Connecting to kafka cluster");
            System.out.println("a.Connecting to kafka cluster");
            producer = new KafkaProducer<String, String>(kafkaProducerProperties.getProducerProperties());
            System.out.println("b.producer created");
        }
        return producer;
    }


    private String getMessageBody(APIGatewayProxyRequestEvent input) {
        String body = input.getBody();

        if (input.getIsBase64Encoded()) {
            body = decode(body);
        }
        return body;
    }

    private APIGatewayProxyResponseEvent createEmptyResponse() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);
        return response;
    }





}

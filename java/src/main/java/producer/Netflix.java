package producer;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.io.IOException;
import java.lang.System;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.time.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import java.io.IOException;
import java.util.Map;
import io.confluent.kafka.schemaregistry.avro.*;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.commons.csv.*;

import java.io.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
/**
 *
 */
public class Netflix {
    public static void main(String[] args) {
        try {
            String topic = "netflixbehavior";
            String home = "C:\\Users\\tspan\\Desktop\\code\\netflixaudience\\";
            //String home = "/opt/demo/java/netflixaudience/";

            Properties config = readConfig(home + "client.properties");

            produce(topic, config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Properties readConfig(final String configFile) throws IOException {
        // reads the client configuration from client.properties
        // and returns it as a Properties object
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }

        Properties config = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            config.load(inputStream);
        }

        return config;
    }

    public static void produce(String topic, Properties config) throws Exception {
        String schemaRegistryUrl = (String)config.get("schema.registry.url");

        // sets the message serializers
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put("schema.registry.url", schemaRegistryUrl);
        System.out.println(schemaRegistryUrl);

        Schema schema = null;

        String subjectName = "netflixbehavior-value"; // Your subject name in Schema Registry

        String apiKey = (String)config.get("schema.key");
        String apiSecret = (String)config.get("schema.secret");

        io.confluent.kafka.schemaregistry.client.SchemaRegistryClient schemaRegistryClient = null;
        RestService restService = null;

        System.out.println("Started");
        try {
            restService = new RestService(schemaRegistryUrl);
            BasicAuthCredentialProvider provider = new SaslBasicAuthCredentialProvider();
            provider.configure(
                    Map.of(
                            "sasl.jaas.config",
                            String.format(
                                    "org.apache.kafka.common.security.scram.ScramLoginModule"
                                            + " required username=\"%s\" password=\"%s\";",
                                    apiKey, apiSecret)));
            restService.setBasicAuthCredentialProvider(provider);
            schemaRegistryClient = new CachedSchemaRegistryClient(restService, 5);
            List<ParsedSchema> response = schemaRegistryClient.getSchemas(subjectName, false, true);

            String schemaString = response.get(0).canonicalString();

            schema = new Schema.Parser().parse(schemaString);

            System.out.println(schemaString);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        int rowCounter = 0;

        config.put("sasl.jaas.config",
                String.format(
                        "org.apache.kafka.common.security.scram.ScramLoginModule"
                                + " required username=\"%s\" password=\"%s\";",
                        apiKey, apiSecret));

        KafkaProducer<String, GenericRecord> producer = null;
        producer = new KafkaProducer<String,GenericRecord>(config);

        String home = "C:\\Users\\tspan\\Desktop\\code\\netflixaudience\\";
        //String home = "/opt/demo/notebook/";

        try (FileReader reader = new FileReader(home + "vodclickstream_uk_movies_03.csv")) {
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            for (CSVRecord csvRecord : csvParser) {
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("row_id", csvRecord.get(0));
                avroRecord.put("datetime", csvRecord.get(1));
                avroRecord.put("duration", csvRecord.get(2));
                avroRecord.put("title", csvRecord.get(3));
                avroRecord.put("genres", csvRecord.get(4));
                avroRecord.put("release_date", csvRecord.get(5));
                avroRecord.put("movie_id", csvRecord.get(6));
                avroRecord.put("user_id", csvRecord.get(7));

                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("netflixbehavior", avroRecord);
                producer.send(record);
                rowCounter++;
                if ( rowCounter % 100 == 0 ) {
                    System.out.println(rowCounter);
                    producer.flush();
                }
            }
        }

        // closes the producer connection
        try {
            producer.close();
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

}
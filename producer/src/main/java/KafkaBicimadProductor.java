import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * KafkaBicimadProducer
 */
public class KafkaBicimadProductor {

    private String topic = null;

    public KafkaProducer<Integer, byte[]> create(String servers, String topic) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", servers);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.topic = topic;
        return new KafkaProducer<>(kafkaProps);
    }

    public void send(KafkaProducer<Integer, byte[]> producer, Station station) throws IOException {
        int key  = (int) Math.floor(Math.random() * 100);
        ProducerRecord<Integer, byte[]> record = new ProducerRecord<>(topic, key, serialize(station));
        producer.send(record);
    }

    private byte[] serialize(Station record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SpecificDatumWriter<Station> writer = new SpecificDatumWriter<>(Station.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

}

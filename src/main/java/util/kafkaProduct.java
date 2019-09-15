package util;






import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

public class kafkaProduct {


    public static void test1(){


        String topic = "test2";

        Properties props = new Properties();
        props.setProperty("num.partitions","4");
        props.setProperty("bootstrap.servers","localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");




        KafkaProducer<String,String> producer = new KafkaProducer(props);



        try {
                BufferedReader bf = new BufferedReader(new FileReader(new File("test1")));
                String line  = null;

                while((line = bf.readLine())!=null){
                    System.out.println(line);
                    Thread.sleep(5000);
                    producer.send(new ProducerRecord<String,String>(topic,line));
                }
                producer.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) {
        kafkaProduct.test1();
    }

}

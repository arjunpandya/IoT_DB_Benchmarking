package com.iot.kafka;
/*
Author: Arjun Pandya
Date: 2018-07-09
Purpose: This program will produce synthetic sensor data, converts it into JSON format and finally publishes on Kafka server
         for a given frequency and time.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iot.sensors.PressureSensor;
import com.iot.sensors.RHSensor;
import com.iot.sensors.TempSensor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.io.*;



public class MongoRun {
    public static void main(String args[]) {
        Properties sysprop = new Properties();
        InputStream input = null;
        int execTime = 0;
        int producer_cnt = 0;
        int recPersec = 0;
        String sensorType = null;
        String brokerList = null;
        String rhTopic = null;
        String pTopic = null;
        String tTopic = null;

        try {

            input = new FileInputStream("resources/config.properties");

            // load a properties file
            sysprop.load(input);

            brokerList = sysprop.getProperty("BROKER_LIST");
            sensorType = sysprop.getProperty("SENSOR_TYPE");
            if (sensorType.equals("ALL") || sensorType.equals("RH")) {
                rhTopic = sysprop.getProperty("RHSENSOR_TOPIC");
            }
            if (sensorType.equals("ALL") || sensorType.equals("P")) {
                pTopic = sysprop.getProperty("PSENSOR_TOPIC");
            }
            if (sensorType.equals("ALL") || sensorType.equals("T")) {
                tTopic = sysprop.getProperty("TSENSOR_TOPIC");
            }
            execTime = Integer.parseInt(sysprop.getProperty("EXEC_TIME")); //Converting hours into seconds
            recPersec = Integer.parseInt(sysprop.getProperty("RECS_PER_SEC"));
            producer_cnt = Integer.parseInt(sysprop.getProperty("PRODUCER_COUNT"));

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        Producer producer = new KafkaProducer(props);
        ObjectMapper mapper = new ObjectMapper();
//        String topicName = "mongotest";

        RHSensor rhSensor = new RHSensor();  //Relative Humidity
        PressureSensor pSensor = new PressureSensor(); // Pressure
        TempSensor tempSensor = new TempSensor(); // Temperature

        Random r = new Random();
        StringBuffer rhs = new StringBuffer();
        StringBuffer ps = new StringBuffer();
        StringBuffer ts = new StringBuffer();
        Double latitude =0.0;
        Double longitude =0.0;

        System.out.println("Starting the Producer messages"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS")));
        for (int t = 1; t < execTime; t++) {
            for (int i = 0; i < recPersec; i++) {
                longitude = Math.random() * Math.PI * 2;
                latitude = Math.acos(Math.random() * 2 - 1);
// RH Sensor
                if (sensorType.equals("ALL") || sensorType.equals("RH")) {
                    rhs.append(r.nextInt(6));  // id
                    rhs.append(",");
                    rhs.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))); //end
                    rhs.append(",");
                    rhs.append("N"); // rain
                    rhs.append(",");
                    rhs.append(r.nextInt(99) / 5); //air temprature
                    rhs.append(",");
                    rhs.append("N"); //quality
                    rhs.append(",");
                    rhs.append(r.nextDouble() * 20); //h wind speed
                    rhs.append(",");
                    rhs.append(r.nextDouble() * 45); // relative humidity
                    rhs.append(",");
                    rhs.append(r.nextInt(90)); // wind direction
                    rhs.append(",");
                    rhs.append(latitude);
                    rhs.append(",");
                    rhs.append(longitude);
                    rhSensor.parseString(rhs.toString());
                    JsonNode rNode = mapper.valueToTree(rhSensor);
                    ProducerRecord<String, JsonNode> rh = new ProducerRecord<String, JsonNode>(rhTopic, rNode); // Change topic name for each experiment
                    producer.send(rh);
                    rhs.setLength(0);
                }
            }
// Pressure Sensor
                if (sensorType.equals("ALL") || sensorType.equals("P") ) {
                    ps.append(r.nextInt(6));  // Sensor_id
                    ps.append(",");
                    ps.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))); // Clock time
                    ps.append(",");
                    ps.append(r.nextLong()); // Altitude
                    ps.append(",");
                    ps.append(r.nextLong()); // Pressure
                    ps.append(",");
                    ps.append(r.nextDouble()); // Temperature
                    ps.append(",");
                    ps.append(latitude); // Latitude
                    ps.append(",");
                    ps.append(longitude); // Longitude
                    pSensor.parseString(ps.toString());
                    JsonNode pNode = mapper.valueToTree(pSensor);
                    ProducerRecord<String, JsonNode> pressure = new ProducerRecord<String, JsonNode>(pTopic, pNode); // Change topic name for each experiment
                    producer.send(pressure);
                    ps.setLength(0);
                }
// Temperature Sensor
                if (sensorType.equals("ALL") || sensorType.equals("T") ) {
                    ts.append(r.nextInt(6));  // id
                    ts.append(",");
                    ts.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))); // Clock time
                    ts.append(",");
                    ts.append(r.nextLong()); // Air Temp
                    ts.append(",");
                    ts.append(r.nextLong()); // Wind Speed
                    ts.append(",");
                    ts.append(r.nextDouble()); // Surface Temperature
                    ts.append(",");
                    ts.append(latitude); // Latitudebcdmps
                    ts.append(",");
                    ts.append(longitude); // Longitude
                    tempSensor.parseString(ts.toString());
                    JsonNode tNode = mapper.valueToTree(tempSensor);
                    ProducerRecord<String, JsonNode> temp = new ProducerRecord<String, JsonNode>(tTopic, tNode); // Change topic name for each experiment
                    producer.send(temp);
                    ts.setLength(0);
                }
            try {
            Thread.sleep(1000);
            } catch (InterruptedException e) {
            e.printStackTrace();
            }
        }
        producer.close(); // Closing producer
        System.out.println("Done sending messages"+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }
}

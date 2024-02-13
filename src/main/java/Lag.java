import consumerpackage.Customer;
import consumerpackage.CustomerDeserializer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Lag {
    private static final Logger log = LogManager.getLogger(Lag.class);
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static String topic;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;
    static long totalLag;
    //////////////////////////////////////////////////////////////////////////////
    static ArrayList<Partition> partitions = new ArrayList<>();

    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;


    static   KafkaConsumer<String, Customer> cons;



   static  Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
  static   Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();



    public  static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        topic = "testtopic1";
        CONSUMER_GROUP = "testgroup1";
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        for (int i = 0; i < 5; i++) {
            //ArrivalProducer.topicpartitions.get(i).setLag(0L);
            Partition p = new Partition(i,0L,0.0);
            partitions.add(p);

//            requestLatestOffsets.put(new TopicPartition(topic, i), OffsetSpec.latest());
//
//            requestTimestampOffsets1.put(new TopicPartition(topic, i),
//                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1000).toEpochMilli()));
        }
        cons = createConsumer();
        cons.subscribe(Arrays.asList("testtopic1"));
       // cons.poll(0L);

    }


    public static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            requestLatestOffsets.put(new TopicPartition(topic, i), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
         totalLag=0L;
        for (int i = 0; i < 5; i++) {
            TopicPartition t = new TopicPartition(topic, i);
            long latestOffset = latestOffsets.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();
            partitions.get(i).setLag(latestOffset - committedoffset);
            ArrivalProducer.topicpartitions.get(i).setLag(latestOffset-committedoffset);
            totalLag += partitions.get(i).getLag();
            log.info("partition {} has lag {}", i, partitions.get(i).getLag());
        }
        //addParentLag(totalLag);
        log.info("total lag {}", totalLag);
    }




     static int queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList("testgroup1"));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();

        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();


       int members = consumerGroupDescriptionMap.get("testgroup1").members().size();

        log.info("consumers nb as per kafka {}", members );

        return members;


    }



    public  static void getCommittedLatestOffsetsAndLag2() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        for (int i = 0; i < 5; i++) {
        requestLatestOffsets.put(new TopicPartition(topic, i), OffsetSpec.latest());
        requestTimestampOffsets1.put(new TopicPartition(topic, i),
                  OffsetSpec.forTimestamp(Instant.now().minusMillis(1000).toEpochMilli()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                admin.listOffsets(requestTimestampOffsets1).all().get();

        totalLag=0L;

       long  totalpone = 0L;
        for (Partition p : partitions) {
            TopicPartition t = new TopicPartition(topic, p.getId());
            long latestOffset = latestOffsets.get(t).offset();
            long latestOffsetOneSec = timestampOffsets1.get(t).offset();

            if(latestOffsetOneSec == -1) {
                log.info("could not find offset!");
                return;
            }
            long ProducedOverOneSec = latestOffset - latestOffsetOneSec;

            long committedoffset = committedOffsets.get(t).offset();
            long CommittedOverOneSec = committedoffset - latestOffsetOneSec;
            partitions.get(p.getId()).setLag(ProducedOverOneSec - CommittedOverOneSec);
            log.info("partition {} has lag {}", p.getId(), partitions.get(p.getId()).getLag());

            totalLag += partitions.get(p.getId()).getLag();
            totalpone += ProducedOverOneSec;

        }

        log.info("total lag {}", totalLag);
        log.info("total pone {}", totalpone);

    }





    public  static void getCommittedLatestOffsetsAndLag3() throws ExecutionException, InterruptedException {
        int first = 1;

        long committimestamp = 0;

        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();

        Map<TopicPartition, OffsetSpec> requestTimestampOffsetscommit = new HashMap<>();


        for (Partition p : partitions) {
            requestLatestOffsets.put(new TopicPartition(topic, p.getId()), OffsetSpec.latest());

            requestTimestampOffsets1.put(new TopicPartition(topic, p.getId()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1000).toEpochMilli()));



            requestTimestampOffsetscommit.put(new TopicPartition(topic, p.getId()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(committimestamp - 1000).toEpochMilli()));

        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                admin.listOffsets(requestTimestampOffsets1).all().get();

        totalLag=0L;

        long  totalpone = 0L;
        for (Partition p : partitions) {
            TopicPartition t = new TopicPartition(topic, p.getId());
            long latestOffset = latestOffsets.get(t).offset();
            long latestOffsetOneSec = timestampOffsets1.get(t).offset();
            long ProducedOverOneSec = latestOffset - latestOffsetOneSec;





            long committedoffset = committedOffsets.get(t).offset();


            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsetscommit = new HashMap<>();



            if (first == 1) {
                first = 0;


                cons.seek(new TopicPartition("testtopic1", p.getId()), committedoffset);



                ConsumerRecords<String, Customer> records = cons.poll(0L);





                for (ConsumerRecord<String, Customer> record : records.records(new TopicPartition
                        ("testtopic1", p.getId()))) {
                    committimestamp = record.timestamp();
                    break;
                }


                for (int i  = 0; i<=4; i++) {
                    requestTimestampOffsetscommit.put(new TopicPartition(topic,i),
                            OffsetSpec.forTimestamp(Instant.now().minusMillis(committimestamp - 5000).toEpochMilli()));

                }


            timestampOffsetscommit =
                        admin.listOffsets(requestTimestampOffsetscommit).all().get();

            }

          long  commitof = timestampOffsetscommit.get(t).offset();



            long CommittedOverOneSec = committedoffset - commitof;//latestOffsetOneSec;
            partitions.get(p.getId()).setLag(ProducedOverOneSec - CommittedOverOneSec);
            log.info("partition {} has lag {}", p.getId(), partitions.get(p.getId()).getLag());

            totalLag += partitions.get(p.getId()).getLag();
            totalpone += ProducedOverOneSec;

        }

        log.info("total lag {}", totalLag);

        log.info("total pone {}", totalpone);

    }




    static KafkaConsumer<String, Customer> createConsumer() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
        //properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "timestamp");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        return new KafkaConsumer<>(properties);




    }


}


   /* Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets2 =
                admin.listOffsets(requestTimestampOffsets2).all().get();

        requestTimestampOffsets2.put(new TopicPartition(topic, p.getId()),
                OffsetSpec.forTimestamp(Instant.now().minusMillis(1000).toEpochMilli()));
*/

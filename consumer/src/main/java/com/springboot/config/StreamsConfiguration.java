package com.springboot.config;

import com.springboot.model.Student;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@Configuration

public class StreamsConfiguration {

	@Autowired
	private KafkaProperties kafkaProperties;

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
//        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
//        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return new KafkaStreamsConfiguration(props);
    }

	private Consumer<String> out;

	@Bean
	public Topology createTopology(StreamsBuilder streamsBuilder) {
		KStream<String, Student> input = streamsBuilder.
						stream("myTopic-kafkasender",
										Consumed.with(Serdes.String(),
														new JsonSerde<>(Student.class))
						);
        /*Key: "Germany-Belgium:H"
         Value: Bet{
                   bettor = John Doe;
                   match = Germany-Belgium;
                   outcome = H;
                   amount = 100;
                   odds = 1.7;
                   timestamp = 1554215083998;
                }
        */
//		KStream<String, Long> gain = input.mapValues(v -> {
//			long val = Math.round(v.getAge() + v.getAge());
//			return val;
//		});
//		KStream<String, Long> gain = input.mapValues()
        /*  Key: "Germany-Belgium:H"
            Value: 170L
        */


		KStream<String, Long> gain = input.map((key, student) -> KeyValue.pair(key + "_" +student.getAge(), student))
						.groupByKey()
						.count()
						.toStream();



		gain.filter((key, count) -> count > Integer.parseInt(key.split("_")[1]))
						.to("myTopic-kafkasender-out", Produced.with(Serdes.String(),
						new JsonSerde<>(Long.class)));


		Topology topology = streamsBuilder.build();

		System.out.println("===============================");
		System.out.println(topology.describe());
		System.out.println("===============================");

		return topology;
	}

//	@Bean
//	StreamsBuilderFactoryBeanConfigurer uncaughtExceptionConfigurer(
//					@Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME) StreamsBuilderFactoryBean factoryBean,
//					ApplicationContext ctx) {
//		return new StreamsBuilderFactoryBeanConfigurer(factoryBean, ctx);
//	}
//
//	@AllArgsConstructor
//	static class StreamsBuilderFactoryBeanConfigurer implements InitializingBean {
//		private final StreamsBuilderFactoryBean factoryBean;
//		private final ApplicationContext ctx;
//
//		@Override
//		public void afterPropertiesSet() {
//
//			this.factoryBean.setUncaughtExceptionHandler(
//							(t, e) -> {
//								log.error("Uncaught exception in thread {}", t.getName(), e);
//								factoryBean.getKafkaStreams().close(Duration.ofSeconds(10));
//								log.info("Kafka streams closed.");
//							});
//			this.factoryBean.setStateListener((newState, oldState) -> {
//				if (newState == KafkaStreams.State.NOT_RUNNING) {
//					log.info("Now exiting the application.");
//					SpringApplication.exit(ctx, () -> 1);
//				}
//			});
//		}
//	}

//	@Bean
//	public KStream<String, String> kStreamJson(StreamsBuilder builder) {
//		KStream<String, String> stream = builder.stream("myTopic-kafkasender", Consumed.with(Serdes.String(), Serdes.String()));
//
//		KTable<String, String> combinedDocuments = stream
//						.map((key, value) -> new KeyValue<>(key, value))
//						.groupByKey()
//						.reduce((x, y)-> "new String()");
//
//		combinedDocuments.toStream().to("myTopic-kafkasender-out", Produced.with(Serdes.String(), Serdes.String()));
//
//		return stream;
//	}

//	@Bean
//	public KStream<String, Student> kStreamJson(StreamsBuilder builder) {
//		KStream<String, Student> stream = builder.stream("myTopic-kafkasender", Consumed.with(Serdes.String(), new JsonSerde<>(Student.class)));
//
//		KTable<String, Student> combinedDocuments = stream
//						.map((key, value) -> new KeyValue<>(key, value))
//						.groupByKey()
//						.reduce((x, y)-> new Student());
//
//		combinedDocuments.toStream().to("myTopic-kafkasender-out", Produced.with(Serdes.String(), new JsonSerde<>(Student.class)));
//
//		return stream;
//	}
//
//    @Bean
//    public KStream<String, Student> kStreamJson(StreamsBuilder builder) {
//    	KStream<String, Test> stream = builder.stream("streams-json-input", Consumed.with(Serdes.String(), new JsonSerde<>(Test.class)));
//
//    	KTable<String, Student> combinedDocuments = stream
//			.map(new StudentKeyValueMapper())
//			.groupByKey()
//			.reduce(new StudentReducer(), Materialized.<String, Test, KeyValueStore<Bytes, byte[]>>as("streams-json-store"))
//    		;
//
//    	combinedDocuments.toStream().to("streams-json-output", Produced.with(Serdes.String(), new JsonSerde<>(Student.class)));
//
//        return stream;
//    }
//
//    public static class StudentKeyValueMapper implements KeyValueMapper<String, Student, KeyValue<String, Student>> {
//
//		@Override
//		public KeyValue<String, Student> apply(String key, Student value) {
//			return new KeyValue<String, Student>(key, value);
//		}
//
//    }
//
//    public static class StudentReducer implements Reducer<Student> {
//
//		@Override
//		public Student apply(Student value1, Student value2) {
//			value1.setStudentId(value1.getStudentId().concat(value2.getStudentId()));
//			return value1;
//		}
//
//    }
//
//    @Data
//    public static class Test {
//
//    	private String key;
//    	private List<String> words;
//
//    	public Test() {
//    		words = new ArrayList<>();
//    	}
//
//    }

}
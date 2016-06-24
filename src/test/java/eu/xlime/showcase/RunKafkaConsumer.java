package eu.xlime.showcase;

import eu.xlime.kafka.RunExtractor;
/**
 * Wrapper around {@link RunExtractor} for easier testing (since some classes are 
 * not available from the kafka-consumer project).
 * 
 * @author rdenaux
 *
 */
public class RunKafkaConsumer {

	public static void main(String[] args) {
		RunExtractor.main(args);
	}
}

package eu.xlime.kafka.rdf;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Properties;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import org.apache.jena.riot.Lang;
import org.junit.Test;

import com.google.common.base.Optional;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.mongo.ConfigOptions;
import eu.xlime.testkit.DatasetLoader;

public class TVOCRToMongoITCase {

	private static DatasetLoader dsLoader = new DatasetLoader();
	
	@Test
	public void testProcessData() throws Exception {
		Properties props = new Properties();
		props.put(ConfigOptions.XLIME_MONGO_RESOURCE_DATABASE_NAME.getKey(), "test-xlimeress");

		TVOCRToMongo testObj = new TVOCRToMongo(props);
		MessageAndMetadata<byte[], byte[]> mm = mockKafkaMessage();
		
		Optional<Dataset> ds = dsLoader.loadDataset(new File("src/test/resources/zattoo-ocr-example-graph.trig"), Lang.TRIG);
		assertTrue(ds.isPresent());
		
		
		testObj.processDataset(mm, ds.get());
		testObj.processDataset(mm, ds.get());
		testObj.processDataset(mm, ds.get());
		String summary = testObj.generateSummary();
		System.out.println("summary: " + summary);
		assertNotNull(summary);
		Summary sum = testObj.generateObjectSummary();
		System.out.println(sum.toString());
	}

	private MessageAndMetadata<byte[], byte[]> mockKafkaMessage() {
		Decoder<byte[]> nullDecoder = null;
		Message rawMessage = new Message("mockRawMessage".getBytes());
		MessageAndMetadata<byte[], byte[]> mm = new MessageAndMetadata<byte[], byte[]>("testTopic", 0, rawMessage, 0, nullDecoder, nullDecoder);
		return mm;
	}
	
}

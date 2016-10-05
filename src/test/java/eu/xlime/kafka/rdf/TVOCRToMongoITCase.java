package eu.xlime.kafka.rdf;

import static org.junit.Assert.assertEquals;
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

		String summary = testObj.generateSummaryString();
		System.out.println(summary);
		assertNotNull(summary);
		
		Summary sum = testObj.generateSummary();
		System.out.println(sum.toString());
		assertEquals(Long.valueOf(2), sum.getCounters().get("messagesProcessed"));
		assertEquals(Long.valueOf(2), sum.getCounters().get("OCRAnnotation_Stored"));
		assertEquals(Long.valueOf(1), sum.getCounters().get("OCRAnnotation_InMongo"));
		assertEquals("TVOCRToMongo_0",sum.getConsumerId());
	}

	private MessageAndMetadata<byte[], byte[]> mockKafkaMessage() {
		Decoder<byte[]> nullDecoder = null;
		Message rawMessage = new Message("mockRawMessage".getBytes());
		MessageAndMetadata<byte[], byte[]> mm = new MessageAndMetadata<byte[], byte[]>("testTopic", 0, rawMessage, 0, nullDecoder, nullDecoder);
		return mm;
	}
	
}

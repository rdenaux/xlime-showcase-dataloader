package eu.xlime.kafka.rdf;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import org.apache.jena.riot.Lang;
import org.junit.Test;

import com.google.common.base.Optional;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.StatMetrics;
import eu.xlime.mongo.ConfigOptions;
import eu.xlime.testkit.DatasetLoader;

public class ASRToMongoITCase {
	private static DatasetLoader dsLoader = new DatasetLoader();
	
	@Test
	public void testProcessData() throws Exception {
		Properties props = new Properties();
		props.put(ConfigOptions.XLIME_MONGO_RESOURCE_DATABASE_NAME.getKey(), "test-xlimeress");

		ASRToMongo testObj = new ASRToMongo(props);
		MessageAndMetadata<byte[], byte[]> mm = mockKafkaMessage();
		
		Optional<Dataset> ds = dsLoader.loadDataset(new File("src/test/resources/zattoo-asr-example-graph.trig"), Lang.TRIG);
		assertTrue(ds.isPresent());
		
		
		boolean result = testObj.processDataset(mm, ds.get());
		assertTrue(result);
		
		StatMetrics sum = (StatMetrics)testObj.generateSummary();
		System.out.println("summary: " + sum);
		assertNotNull(sum);
		assertTrue(sum.getCounters().keySet().contains("rdfQuads"));
		assertEquals(Long.valueOf(1), sum.getCounters().get("messagesProcessed"));
		assertEquals(Long.valueOf(1), sum.getCounters().get("ASRAnnotation_Read"));
		assertFalse(sum.getCounters().containsKey("EntityAnnotation_Read"));
		assertTrue(sum.getCounters().containsKey("ASRAnnotation_InMongo"));
		assertTrue(sum.getMeterId().startsWith("ASRToMongo_"));
		assertEquals(Long.valueOf(15), sum.getCounters().get("rdfQuads"));
	}

	@Test
	public void testProcessData02() throws Exception {
		Properties props = new Properties();
		props.put(ConfigOptions.XLIME_MONGO_RESOURCE_DATABASE_NAME.getKey(), "test-xlimeress");

		ASRToMongo testObj = new ASRToMongo(props);
		MessageAndMetadata<byte[], byte[]> mm = mockKafkaMessage();
		
		Optional<Dataset> ds = dsLoader.loadDataset(new File("src/test/resources/zattoo-asr-example-graph2.trig"), Lang.TRIG);
		assertTrue(ds.isPresent());
		
		
		boolean result = testObj.processDataset(mm, ds.get());
		assertTrue(result);
		
		StatMetrics sum = (StatMetrics)testObj.generateSummary();
		System.out.println("summary: " + sum);
		assertNotNull(sum);
		assertTrue(sum.getCounters().keySet().contains("rdfQuads"));
		assertEquals(Long.valueOf(1), sum.getCounters().get("messagesProcessed"));
		assertEquals(Long.valueOf(1), sum.getCounters().get("ASRAnnotation_Read"));
		assertTrue(sum.getCounters().containsKey("EntityAnnotation_Read"));
		assertEquals(Long.valueOf(1), sum.getCounters().get("EntityAnnotation_Read"));
		assertTrue(sum.getCounters().containsKey("ASRAnnotation_InMongo"));
		assertTrue(sum.getMeterId().startsWith("ASRToMongo_"));
		assertEquals(Long.valueOf(31), sum.getCounters().get("rdfQuads"));
	}
	
	private MessageAndMetadata<byte[], byte[]> mockKafkaMessage() {
		Decoder<byte[]> nullDecoder = null;
		Message rawMessage = new Message("mockRawMessage".getBytes());
		MessageAndMetadata<byte[], byte[]> mm = new MessageAndMetadata<byte[], byte[]>("testTopic", 0, rawMessage, 0, nullDecoder, nullDecoder);
		return mm;
	}

}

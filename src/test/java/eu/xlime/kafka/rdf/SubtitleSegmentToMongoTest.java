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


public class SubtitleSegmentToMongoTest {

	private static DatasetLoader dsLoader = new DatasetLoader();
	
	@Test
	public void testProcessData() throws Exception {
		Properties props = new Properties();
		props.put(ConfigOptions.XLIME_MONGO_RESOURCE_DATABASE_NAME.getKey(), "test-xlimeress");

		SubtitleSegmentToMongo testObj = new SubtitleSegmentToMongo(props);
		Decoder<byte[]> nullDecoder = null;
		Message rawMessage = new Message("mockRawMessage".getBytes());
		MessageAndMetadata<byte[], byte[]> mm = new MessageAndMetadata<byte[], byte[]>("testTopic", 0, rawMessage, 0, nullDecoder, nullDecoder);
		
		Optional<Dataset> ds = dsLoader.loadDataset(new File("src/test/resources/zattoo-sub-example-graph.trig"), Lang.TRIG);
		assertTrue(ds.isPresent());
		
		
		boolean result = testObj.processDataset(mm, ds.get());
		assertTrue(result);
		StatMetrics sum = (StatMetrics)testObj.generateSummary();
		System.out.println("summary: " + sum);
		assertNotNull(sum);
		assertTrue(sum.getCounters().keySet().contains("rdfQuads"));
		assertEquals(Long.valueOf(1), sum.getCounters().get("messagesProcessed"));
		assertEquals(Long.valueOf(5), sum.getCounters().get("SubtitleSegment_Read"));
		assertEquals(Long.valueOf(15), sum.getCounters().get("EntityAnnotation_Read"));
		assertEquals(Long.valueOf(2), sum.getCounters().get("namedGraphs"));
//		assertEquals(Long.valueOf(5), sum.getCounters().get("SubtitleSegment_Stored"));
//		assertEquals(Long.valueOf(20), sum.getCounters().get("SubtitleSegment_InMongo"));
		assertEquals("SubtitleSegmentToMongo_0",sum.getMeterId());
		assertEquals(Long.valueOf(438), sum.getCounters().get("rdfQuads"));
	}
	
}

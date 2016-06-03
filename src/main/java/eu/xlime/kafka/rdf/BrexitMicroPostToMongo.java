package eu.xlime.kafka.rdf;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.message.MessageAndMetadata;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.MicroPostBean;
import eu.xlime.dao.MediaItemDao;
import eu.xlime.dao.MediaItemDaoFromDataset;
import eu.xlime.dao.MongoMediaItemDao;
import eu.xlime.kafka.ConfigOptions;
import eu.xlime.kafka.RunExtractor;
import eu.xlime.kafka.msgproc.DatasetProcessor;

/**
 * {@link DatasetProcessor} meant to be used in an xLiMe Kafka {@link RunExtractor} execution.
 *  
 * It expects a {@link Dataset} from a xLime socialmedia stream and it will extract {@link MicroPostBean}s that match
 * the Brexit keywordFilters; then it will push those beans to a MongoDB. See {@link ConfigOptions} for configuration keys.
 *  
 * @author rdenaux
 *
 */
public class BrexitMicroPostToMongo implements DatasetProcessor {

	private static final Logger log = LoggerFactory.getLogger(BrexitMicroPostToMongo.class);
	
	public static final List<String> allowedKeywords = ImmutableList.of("Brexit EN", "Brexit ES", "Brexit DE", "Brexit IT");
	
	private final MongoMediaItemDao mongoDao; 

	private static int instCount = 0;
	private final int instId;
	private long summariseEvery;
	private long messagesProcessed = 0;
	private long brexitPostsRead = 0;
	private long brexitPostsStored = 0;
	
	public BrexitMicroPostToMongo(Properties props) {
		instId = instCount++;
		Optional<Long> optVal = ConfigOptions.XLIME_KAFKA_CONSUMER_TOPIC_RDF_DATASET_PROCESSOR_SUMMARISE_EVERY.getOptLongVal(props);
		summariseEvery = optVal.or(500L); 
		mongoDao = new MongoMediaItemDao(props);
	}
	
	@Override
	public boolean processDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset) {
		messagesProcessed++;
		trySummarise();
		try {
			MediaItemDao miDao = new MediaItemDaoFromDataset(dataset);
			List<MicroPostBean> brexitMicroPosts = miDao.findMicroPostsByKeywordsFilter(allowedKeywords);
			if (brexitMicroPosts != null) brexitPostsRead += brexitMicroPosts.size();
			return pushToMongo(brexitMicroPosts);
		} catch (Exception e) {
			log.error("Failed to process dataset", e);
			return false;
		}
	}

	private void trySummarise() {
		if (summariseEvery <= 0) return;
		try {
			if (messagesProcessed % summariseEvery == 0) {
				log.info(generateSummary());
			}
		} catch (Exception e) {
			log.warn("Failed to log summary", e);
		}
	}

	private String generateSummary() {
		return String.format("instance_%s processed=%s, postsRead=%s, postsStored=%s, micropostsInMongo=%s", instId, messagesProcessed, brexitPostsRead, brexitPostsStored, tryGetMongoMicroPostCount());
	}

	private Long tryGetMongoMicroPostCount() {
		try {
			return mongoDao.count(MicroPostBean.class);
		} catch (Exception e) {
			return -1L;
		}
	}

	private boolean pushToMongo(List<MicroPostBean> microPosts) {
		boolean result = true;
		for (MicroPostBean bean: microPosts) {
			try {
				mongoDao.insertOrUpdate(bean);
				brexitPostsStored++;
			} catch (Exception e) {
				log.error("Failed to upsert micropost", e);
				result = false;
			}
		}
		return result;
	}
	
}

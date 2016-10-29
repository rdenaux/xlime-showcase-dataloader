package eu.xlime.kafka.rdf;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.ASRAnnotation;
import eu.xlime.bean.EntityAnnotation;
import eu.xlime.bean.MediaItem;
import eu.xlime.bean.MicroPostBean;
import eu.xlime.bean.NewsArticleBean;
import eu.xlime.bean.OCRAnnotation;
import eu.xlime.bean.StatMetrics;
import eu.xlime.bean.SubtitleSegment;
import eu.xlime.bean.TVProgramBean;
import eu.xlime.bean.VideoSegment;
import eu.xlime.bean.XLiMeResource;
import eu.xlime.dao.MediaItemDao;
import eu.xlime.dao.MongoXLiMeResourceStorer;
import eu.xlime.dao.mediaitem.MediaItemDaoFromDataset;
import eu.xlime.kafka.msgproc.BaseDatasetProcessor;
import eu.xlime.kafka.msgproc.DatasetProcessor;
import eu.xlime.util.KBEntityMapper;
import eu.xlime.util.NullEnDBpediaKBEntityMapper;
import eu.xlime.util.ResourceTypeResolver;
import eu.xlime.util.XLiMeResourceTyper;

/**
 * Base class for {@link DatasetProcessor}s which read one or more types of {@link XLiMeResource} from a stream of
 * kafka {@link MessageAndMetadata} (which has been previously parsed into an RDF {@link Dataset}) and pushes
 * these beans to a Mongo database.
 * 
 * @author rdenaux
 */
public abstract class BaseXLiMeResourceToMongo extends BaseDatasetProcessor {
	
	private static final Logger log = LoggerFactory.getLogger(BaseXLiMeResourceToMongo.class);

	private static final XLiMeResourceTyper xresTyper = new XLiMeResourceTyper();
	private static final ResourceTypeResolver xresResolver = new ResourceTypeResolver();
	
	/**
	 * The component which performs the writing of the {@link XLiMeResource} beans to a mongo db. 
	 */
	private final MongoXLiMeResourceStorer mongoStorer;
	
	/**
	 * {@link KBEntityMapper} used to canonicalise KB entity urls when producing {@link EntityAnnotation}s. 
	 */
	protected final KBEntityMapper kbEntityMapper;

	/**
	 * The number of media items (of all types) in the messages
	 */
	protected long medItsInMessages = 0;
	
	
	/**
	 * The number of the {@link XLiMeResource}s read (from Kafka messages), classified by their type
	 */
	private Map<Class<? extends XLiMeResource>, Long> resourcesRead = new HashMap<>();

	/**
	 * The number of {@link XLiMeResource}s stored to Mongo, classified by their type
	 */
	private Map<Class<? extends XLiMeResource>, Long> resourcesStored = new HashMap<>();
	
	private class LatencyMeter {
		/**
		 * Accumulates the milliseconds between processed times and publication times of 
		 * various resources. Used in combination with {@link #strictPubDateLatencyDataPoints} to
		 * average 
		 */
		private long latencySumMs = 0;
		private long latencyDataPoints = 0;
		
		public void accumulateLatency(long latencyMs) {
			latencySumMs += latencyMs;
			latencyDataPoints++;
		}

		public long readAvgLatencyMsAndReset() {
			final long result = readAvgLatencyMs();
			reset();
			return result;
		}
		
		public void reset() {
			latencySumMs = 0;
			latencyDataPoints = 0;
		}
		
		public long readAvgLatencyMs() {
			if (latencyDataPoints > 0) {
				return latencySumMs / latencyDataPoints;
			} else return 0;
		}
	}
	
	/**
	 * Used to estimate the latency between publication date and processing date
	 * for the processed resources. This is done strictly: if a given resource
	 * has a publication date in the future, this is not included in this 
	 * latency meter.
	 * @see  #lenientPubDateLatencyMeter
	 */
	private LatencyMeter strictPubDateLatencyMeter = new LatencyMeter();
	/**
	 * Used to estimate the latency between publication date and processing date
	 * for the processed resources. This is done leniently: if a given resource
	 * has a publication date in the future, this is still included in this 
	 * latency meter, which means the result may be distorted by data points for 
	 * which the publication date has been read incorrectly. 
	 */
	private LatencyMeter lenientPubDateLatencyMeter = new LatencyMeter();
	
	public BaseXLiMeResourceToMongo(Properties props) {
		super(props);
		mongoStorer = new MongoXLiMeResourceStorer(props);
		kbEntityMapper = new NullEnDBpediaKBEntityMapper();
	}
	
	@Override
	protected Object generateSummary(){
		StatMetrics sum = new StatMetrics();
		sum.setMeterId(getClass().getSimpleName() + "_" + instId);
		sum.setMeterStartDate(processorCreationDate);
		sum.setCounter("bytesProcessed", getBytesProcessed());
		sum.setCounter("messagesProcessed", getMessagesProcessed());
		sum.setCounter("messageFailedToProcess", getMessagesFailedToProcess());
		sum.setCounter("datasetProcessingTimeMs", getDatasetProcessingTimeInMs());
		sum.setCounter("namedGraphs", getSeenNamedGraphs());
		sum.setCounter("nullDatasets", getSeenNullDatasets());
		sum.setCounter("rdfQuads", getSeenRDFQuads());
		sum.setCounter("strictAvgLatencyMs", strictPubDateLatencyMeter.readAvgLatencyMsAndReset());
		sum.setCounter("lenientAvgLatencyMs", lenientPubDateLatencyMeter.readAvgLatencyMsAndReset());		
		sum.addCounters(summariseResourcesProcessed());
		return sum;
	}

	@Override
	protected void processSummaryObject(Object summary) {
		super.processSummaryObject(summary);
		if (summary instanceof StatMetrics) {
			try {
				StatMetrics sm = (StatMetrics)summary;
				mongoStorer.insertOrUpdate(sm);
			} catch (Exception e) {
				log.error("Failed to insert metrics to Mongo.", e);
			}
		} else {
			log.warn(String.format("Unexpected summary object. Expecting %s, but found %s", 
					StatMetrics.class, summary));
		}
	}

	private Map<String,Long> summariseResourcesProcessed() {
		if (log.isTraceEnabled()) {
			log.trace("Summarising resources processed based on\n\tread: " + resourcesRead + "\n\tstored: " + resourcesStored);
		}
		Set<Class<? extends XLiMeResource>> clzz = ImmutableSet.<Class<? extends XLiMeResource>>builder()
				.addAll(resourcesRead.keySet()).addAll(resourcesStored.keySet()).build();
		if (clzz.isEmpty()) return ImmutableMap.of();
		Map<String,Long> result = new HashMap<String,Long>();
		if (medItsInMessages > 0) {
			result.put("mediaItemsInMessages", medItsInMessages);
		}
		for (Class<? extends XLiMeResource> clz: clzz) {
			String tName = clz.getSimpleName();
			result.put(String.format("%s_Read",tName),getResourcesRead(clz));
			result.put(String.format("%s_Stored",tName),getResourcesStored(clz));
			result.put(String.format("%s_InMongo", tName), tryGetMongoXLiMeResourceCount(clz));	
		}
		return result;
	}
	
	/**
	 * Subclasses should implement the extraction of the {@link XLiMeResource} beans here.
	 * 
	 * @param mm
	 * @param dataset
	 * @return a {@link List} of resources (which could be of different types!) 
	 * 	which were extracted from the dataset.
	 */
	protected abstract List<? extends XLiMeResource> extractXLiMeResourceBeans(
			MessageAndMetadata<byte[], byte[]> mm, Dataset dataset);
	
	@Override
	protected final boolean doProcessDataset(MessageAndMetadata<byte[], byte[]> mm,
			Dataset dataset) {
		List<? extends XLiMeResource> beans = extractXLiMeResourceBeans(mm, dataset);
		for (Class<? extends XLiMeResource> clz: xresTyper.findResourceClasses(beans)) {
			List<? extends XLiMeResource> cBeans = xresTyper.filterByType(clz, beans);
			long old = resourcesRead.containsKey(clz) ? resourcesRead.get(clz) : 0L;
			resourcesRead.put(clz, old + (cBeans == null ? 0 : cBeans.size()));
		}
		boolean result = pushToMongo(beans);
		tryAccumPubDateLatency(beans);
		return result;
	}

	/**
	 * Tries to accumulate data about the latency between the current (processing) date 
	 * and the date when the beans were published (according to their metadata).
	 * 
	 * @param beans
	 */
	private void tryAccumPubDateLatency(List<? extends XLiMeResource> beans) {
		Date now = new Date();
		List<XLiMeResource> estimatable = new ArrayList<>();
		for (XLiMeResource res: beans){
			if (canEstimatePubDateLatency(res)) estimatable.add(res);
		}
		for (XLiMeResource res: estimatable) {
			Optional<Date> optDate = findResourcePubDate(res);
			if (optDate.isPresent()) {
				Date pubDate = optDate.get();
				long latencyMs = now.getTime() - pubDate.getTime();
				if (latencyMs >= 0) {
					strictPubDateLatencyMeter.accumulateLatency(latencyMs);
				} else {
					//potentially weird as we have negative latency, 
					// this can be OK for e.g. TVPrograms as their EPG data is 
					// available before broadcasting, but mostly this is undesired.   
				} 
				lenientPubDateLatencyMeter.accumulateLatency(latencyMs);
			}
		}
		
	}

	private boolean canEstimatePubDateLatency(XLiMeResource res) {
		return isInstanceOfOneOf(res, TVProgramBean.class, MicroPostBean.class, NewsArticleBean.class,
				ASRAnnotation.class, OCRAnnotation.class, SubtitleSegment.class);
	}

	private boolean isInstanceOfOneOf(XLiMeResource res,
			Class<? extends XLiMeResource>... classes) {
		for (Class<? extends XLiMeResource> clz: classes) {
			if (clz.isAssignableFrom(res.getClass())) return true; 
		}
		return false;
	}

	private boolean pushToMongo(List<? extends XLiMeResource> beans) {
		boolean result = true;
		for (XLiMeResource bean: beans) {
			try {
				mongoStorer.insertOrUpdate(bean);
				Class<? extends XLiMeResource> clz = xresTyper.findResourceClass(bean);
				long old = resourcesStored.containsKey(clz) ? resourcesStored.get(clz) : 0L;
				resourcesStored.put(clz, old + 1);
			} catch (Exception e) {
				log.error("Failed to upsert micropost", e);
				result = false;
			}
		}
		return result;
	}
	
	protected final Optional<Date> findResourcePubDate(XLiMeResource resource) {
		try {
			if (resource instanceof TVProgramBean) {
				return Optional.fromNullable(((TVProgramBean) resource).getBroadcastDate().getTimestamp());
			} else if (resource instanceof MicroPostBean) {
				return Optional.fromNullable(((MicroPostBean) resource).getCreated().getTimestamp());
			} else if (resource instanceof NewsArticleBean) {
				return Optional.fromNullable(((NewsArticleBean) resource).getCreated().getTimestamp());
			} else if (resource instanceof VideoSegment) {
				return Optional.fromNullable(((VideoSegment) resource).getStartTime().getTimestamp());
			} else	if (resource instanceof SubtitleSegment) {
				return findResourcePubDate(((SubtitleSegment) resource).getPartOf());
			} else if (resource instanceof ASRAnnotation) {
				return findResourcePubDate(((ASRAnnotation) resource).getInSegment());
			} else if (resource instanceof OCRAnnotation) {
				return findResourcePubDate(((OCRAnnotation) resource).getInSegment());
			} else {
				log.warn("Cannot get publication date for resource " + resource);
				return Optional.absent();
			}
		} catch (RuntimeException e) {
			log.info("Failed to extract publication date for " + resource);
			return Optional.absent();
		}
	}

	protected final Long tryGetMongoXLiMeResourceCount(Class<? extends XLiMeResource> clz) {
		try {
			return mongoStorer.count(clz);
		} catch (Exception e) {
			return -1L;
		}
	}

	public final long getResourcesRead(Class<? extends XLiMeResource> clz) {
		if (resourcesRead == null || resourcesRead.isEmpty()) return 0L;
		if (!resourcesRead.containsKey(clz)) return 0L;
		return resourcesRead.get(clz);
	}

	public final long getResourcesStored(Class<? extends XLiMeResource> clz) {
		if (resourcesStored == null || resourcesStored.isEmpty()) return 0L;
		if (!resourcesStored.containsKey(clz)) return 0L;
		return resourcesStored.get(clz);
	}
	
	/**
	 * Convenience method for extracting MediaItems from a given Dataset. Subclasses can use it in their
	 * implementation of {@link #extractXLiMeResourceBeans(MessageAndMetadata, Dataset)}.
	 *
	 * @param mm
	 * @param dataset
	 * @param clzz zero or more {@link MediaItem} sub classes that you want to extract. If you don't pass any class,
	 * 	this method will not extract any {@link MediaItem}. 
	 * @return
	 */
	protected final List<MediaItem> extractXLiMeMediaItems( 
			MessageAndMetadata<byte[], byte[]> mm, Dataset dataset, Class<? extends MediaItem>... clzz) {
		if (clzz == null || clzz.length == 0) return ImmutableList.of();
		MediaItemDao miDao = new MediaItemDaoFromDataset(dataset);
		List<String> miUrls = miDao.findAllMediaItemUrls(1000);
		if (miUrls != null) medItsInMessages += miUrls.size();
		List<MediaItem> result = new ArrayList<>();
		for (Class<? extends MediaItem> clz: ImmutableSet.copyOf(clzz)) {
			result.addAll(miDao.findMediaItems(clz, miUrls));
		}
		return ImmutableList.copyOf(result);
	}

}

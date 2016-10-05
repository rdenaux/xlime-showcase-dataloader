package eu.xlime.kafka.rdf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hp.hpl.jena.query.Dataset;

import eu.xlime.bean.MediaItem;
import eu.xlime.bean.XLiMeResource;
import eu.xlime.dao.MediaItemDao;
import eu.xlime.dao.MongoXLiMeResourceStorer;
import eu.xlime.dao.mediaitem.MediaItemDaoFromDataset;
import eu.xlime.kafka.msgproc.BaseDatasetProcessor;
import eu.xlime.kafka.msgproc.DatasetProcessor;
import eu.xlime.util.KBEntityMapper;
import eu.xlime.util.NullEnDBpediaKBEntityMapper;
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
	
	/**
	 * The component which performs the writing of the {@link XLiMeResource} beans to a mongo db. 
	 */
	private final MongoXLiMeResourceStorer mongoStorer;
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
	
	
	public BaseXLiMeResourceToMongo(Properties props) {
		super(props);
		mongoStorer = new MongoXLiMeResourceStorer(props);
		kbEntityMapper = new NullEnDBpediaKBEntityMapper();
	}

	@Override
	protected String generateSummary() {
		return String.format("%s_%s processed=%s %s", 
				getClass().getSimpleName(), instId, getMessagesProcessed(), summariseResourcesProcessed());
	}
	
	private String summariseResourcesProcessed() {
		Set<Class<? extends XLiMeResource>> clzz = ImmutableSet.<Class<? extends XLiMeResource>>builder()
				.addAll(resourcesRead.keySet()).addAll(resourcesStored.keySet()).build();
		if (clzz.isEmpty()) return "\n\tNo xLiMe resources read or stored yet";
		StringBuilder sb = new StringBuilder();
		if (medItsInMessages > 0) {
			sb.append(String.format("\n\tmediaItemsInMessages=%s", medItsInMessages));
		}
		for (Class<? extends XLiMeResource> clz: clzz) {
			String tName = clz.getSimpleName();
			sb.append("\n\t").append(
					String.format("%s_Read=%s, %s_Stored=%s, %s_InMongo=%s", tName, getResourcesRead(clz), 
							tName, getResourcesStored(clz), tName, tryGetMongoXLiMeResourceCount(clz)));	
		}
		
		return sb.toString();
	}
	
	protected Summary generateObjectSummary(){
		Summary sum = new Summary();
		sum.setConsumerId(getClass().getSimpleName() + "_" + instId);
		sum.setProcessed(getMessagesProcessed());
		sum.setAnnotations(summariseResourcesProcessedForObject());
		return sum;
	}
	
	private Map<String,Long> summariseResourcesProcessedForObject() {
		Set<Class<? extends XLiMeResource>> clzz = ImmutableSet.<Class<? extends XLiMeResource>>builder()
				.addAll(resourcesRead.keySet()).addAll(resourcesStored.keySet()).build();
		if (clzz.isEmpty()) return null;
		Map<String,Long> annotations = new HashMap<String,Long>();
		if (medItsInMessages > 0) {
			annotations.put("mediaItemsInMessages", medItsInMessages);
		}
		for (Class<? extends XLiMeResource> clz: clzz) {
			String tName = clz.getSimpleName();
			annotations.put(String.format("%s_Read",tName),getResourcesRead(clz));
			annotations.put(String.format("%s_Stored",tName),getResourcesStored(clz));
			annotations.put(String.format("%s_InMongo", tName), tryGetMongoXLiMeResourceCount(clz));	
		}
		return annotations;
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
		return pushToMongo(beans);
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

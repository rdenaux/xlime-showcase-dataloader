package eu.xlime.showcase;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.xlime.bean.StatMetrics;
import eu.xlime.bean.XLiMeResource;
import eu.xlime.dao.MongoXLiMeResourceStorer;
import eu.xlime.kafka.side.SideTask;
import eu.xlime.util.BaseEnDBpedKBEntityMapper;

/**
 * {@link SideTask} for storing {@link StatMetrics} about KB entity canonicalisation to Mongo.
 * 
 * @author rdenaux
 *
 */
public class EntityCanonicalisationToMongo extends SideTask {

	private static final Logger log = LoggerFactory.getLogger(EntityCanonicalisationToMongo.class);
	
	/**
	 * The component which performs the writing of the {@link XLiMeResource} beans to a mongo db. 
	 */
	private final MongoXLiMeResourceStorer mongoStorer;

	public EntityCanonicalisationToMongo(Properties props) {
		super(props);
		mongoStorer = new MongoXLiMeResourceStorer(props);
	}


	@Override
	public void run() {
		try {
			StatMetrics metrics = BaseEnDBpedKBEntityMapper.getStatMetrics();
			log.debug("Storing EntityCanonicalisation metrics to Mongo", metrics);
			mongoStorer.insertOrUpdate(metrics);
		} catch (Exception e) {
			log.error("Failed to store entity canonicalisation metrics to mongo", e);
		}
	}

}

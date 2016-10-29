package eu.xlime.dbpedia;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.xlime.Config;
import eu.xlime.dbpedia.AnnotatedUIEntityToMongo.EntityUpsertFromTTlSummary;

public class AnnotatedUIEntityToMongoITCase {

	@Test
	@Ignore("Load test mongo db and use that to test with expected results")
	public void test_findEntityUrisInEntityAnnotations() throws Exception {
		AnnotatedUIEntityToMongo testObj = new AnnotatedUIEntityToMongo(new Config().getCfgProps());
		Set<String> result = testObj.findEntityUrisInEntityAnnotations();
		assertNotNull(result);
//		System.out.println(String.format("Found %s entityUrls %s", result.size(), result));
		assertTrue(result.size() > 500);
		File f = new File("target/entUrls.txt");
		testObj.writeEntityUrisToFile(result, f);
		assertTrue(f.exists());
	}
	
	@Test
	@Ignore("Create subset of dbpedia turtle files to test")
	public void test_loadToMongoFromDBpediaTtlFiles() throws Exception {
		AnnotatedUIEntityToMongo testObj = new AnnotatedUIEntityToMongo(new Config().getCfgProps());
		List<String> entUrls = testObj.readEntityUrisFromFile(new File("target/entUrls.txt"));
		File labelsF = new File("D:/ont/dbpedia-2015-10/labels_en.ttl");
		File imagesF = new File("D:/ont/dbpedia-2015-10/images_en.ttl");
		File typesF = new File("D:/ont/dbpedia-2015-10/instance_types_transitive_en.ttl");
		Map<File, EntityUpsertFromTTlSummary> summs = testObj.loadToMongoFromDBpediaTtlFiles(ImmutableSet.copyOf(entUrls), "en", 
//				labelsF,
				imagesF); //, 
//				typesF);
		assertNotNull(summs);
	}
	
//	@Test
	//TODO: this should be a main in AnnotatedUIEntityToMongo
	@Ignore("This is a long running process")
	public void test_loadToMongoFromDBpediaTtlFiles_all() throws Exception {
		AnnotatedUIEntityToMongo testObj = new AnnotatedUIEntityToMongo(new Config().getCfgProps());
		List<String> entUrls = ImmutableList.of();
		File labelsF = new File("D:/ont/dbpedia-2015-10/labels_en.ttl");
		File imagesF = new File("D:/ont/dbpedia-2015-10/images_en.ttl");
		File typesF = new File("D:/ont/dbpedia-2015-10/instance_types_transitive_en.ttl");
		Map<File, EntityUpsertFromTTlSummary> summs = testObj.loadToMongoFromDBpediaTtlFiles(ImmutableSet.copyOf(entUrls), "en", 
				labelsF,
				imagesF, //); //, 
				typesF);
		assertNotNull(summs);
	}
	
	@Test
	@Ignore("use predefined lists of entUrls")
	public void test_toCanonicalEntUris() throws Exception {
		AnnotatedUIEntityToMongo testObj = new AnnotatedUIEntityToMongo(new Config().getCfgProps());
		List<String> entUrls = testObj.readEntityUrisFromFile(new File("target/entUrls.txt"));
		Set<String> ncEntUrls = ImmutableSet.copyOf(entUrls);
		System.out.println(String.format("Loaded %s potentially encoded entUrls", ncEntUrls.size()));
		long start = System.currentTimeMillis();
		Set<String> cEntUrls = testObj.toCanonicalEntUris(ncEntUrls);
		System.out.println(String.format("Merged %s potentially encoded entUrls into %s entUris in %sms", ncEntUrls.size(), cEntUrls.size(), (System.currentTimeMillis() - start)));
		assertTrue(cEntUrls.size() <= ncEntUrls.size());
	}
}

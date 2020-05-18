package com.huawei.cloudtable.geomesa.examples;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.huawei.cloudtable.hbase.examples.HBaseSample;
import com.vividsolutions.jts.geom.Geometry;

/**
 * GeoMesa Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using GeoTools API
 */
public class GeoMesaSample {
	private final static Log LOG = LogFactory.getLog(HBaseSample.class.getName());

	/**
	 *  The name of catalog table which stores metadata information about each feature
	 *  Here, a feature can be defined as a row based on the abstract geometry model which is "Simple Feature Model"
	 */
	private final String catalogName = "DEMO";
	
	/** 
	 *  The simpleFeatureTypeName is the name of a type of simple features  
	 */
	private final String simpleFeatureTypeName = "SFT";

	/** 
	 *  Define the format of attributes in a feature
	 */
	private final List<String> attributes = Lists.newArrayList(
			"Who:String",
			"What:java.lang.Long",     // some types require full qualification (see DataUtilities docs)
			"When:Date",               // a date-time field is optional, but can be indexed
			"*Where:Point:srid=4326",  // the "*" denotes the default geometry (used for indexing)
			"Why:String"               // you may have as many other attributes as you like...
			);

	/**
	 * create the bare simple-feature type based on format of the attributes
	 */
	private final SimpleFeatureType simpleFeatureType = SimpleFeatureTypes.createType(simpleFeatureTypeName, Joiner.on(",").join(attributes));

	/**
	 * This represents a physical source of feature data, such as a shapefiles or database, where the features will be instances of SimpleFeature. 
	 */
	private DataStore dataStore = null;
	
	public GeoMesaSample() {
		Map<String, Serializable> dsConf = new HashMap<>();
		dsConf.put("bigtable.table.name", catalogName);
		try {
			dataStore = DataStoreFinder.getDataStore(dsConf);
		} catch (IOException e) {
			LOG.error("initilize dataStore failed ", e);
		}
	}

	/**
	 * @param simpleFeatureType: The type of a SimpleFeature.
	 * @param numNewFeatures: The count of new features generated
	 * @return FeatureCollection: Access to "simple" Feature content where each feature has the same SimpleFeatureType.
	 */
	private FeatureCollection createNewFeatures(SimpleFeatureType simpleFeatureType, int numNewFeatures) {
		DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

		String id;
		Object[] NO_VALUES = {};
		String[] PEOPLE_NAMES = {"Addams", "Bierce", "Clemens"};
		Long SECONDS_PER_YEAR = 365L * 24L * 60L * 60L;
		Random random = new Random(5771);
		DateTime MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"));
		Double MIN_X = -79.5;
		Double MIN_Y =  37.0;
		Double DX = 2.0;
		Double DY = 2.0;

		for (int i = 0; i < numNewFeatures; i ++) {
			// create the new (unique) identifier and empty feature shell
			id = "Observation." + Integer.toString(i);
			SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, NO_VALUES, id);

			// be sure to tell GeoTools explicitly that you want to use the ID you provided
			simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

			// populate the new feature's attributes

			// Who: string value
			simpleFeature.setAttribute("Who", PEOPLE_NAMES[i % PEOPLE_NAMES.length]);

			// What: long value
			simpleFeature.setAttribute("What", i);

			// Where: location: construct a random point within a 2-degree-per-side square
			double x = MIN_X + random.nextDouble() * DX;
			double y = MIN_Y + random.nextDouble() * DY;
			Geometry geometry = WKTUtils$.MODULE$.read("POINT(" + x + " " + y + ")");
			simpleFeature.setAttribute("Where", geometry);

			// When: date-time:  construct a random instant within a year
			DateTime dateTime = MIN_DATE.plusSeconds((int) Math.round(random.nextDouble() * SECONDS_PER_YEAR));
			simpleFeature.setAttribute("When", dateTime.toDate());

			// Why: another string value
			// left empty, showing that not all attributes need values

			// accumulate this new feature in the collection
			featureCollection.add(simpleFeature);
		}

		return featureCollection;
	}

	/**
	 * @param simpleFeatureTypeName: The type of a SimpleFeature.
	 * @param dataStore:This represents a physical source of feature data, such as a shapefiles or database, where the features will be instances of SimpleFeature.
	 * @param featureCollection: Access to "simple" Feature content where each feature has the same SimpleFeatureType.
	 * @throws IOException
	 */
	private void insertFeatures(String simpleFeatureTypeName,
			DataStore dataStore,
			FeatureCollection featureCollection)
					throws IOException {

		FeatureStore featureStore = (FeatureStore)dataStore.getFeatureSource(simpleFeatureTypeName);
		featureStore.addFeatures(featureCollection);
	}

	/**
	 * @param simpleFeatureTypeName: The type of a SimpleFeature.
	 * @param dataStore: This represents a physical source of feature data, such as a shapefiles or database, where the features will be instances of SimpleFeature.
	 * @param geomField: the name of field which represents the geolocation
	 * @param x0: the lower limit of longitude
	 * @param y0: the lower limit of latitude
	 * @param x1: the upper limit of longitude
	 * @param y1: the upper limit of latitude
	 * @param dateField: the name of field which represents the date
	 * @param t0: the lower limit of date range
	 * @param t1: the upper limit of date range
	 * @param attributesQuery: the clause of an SQL statement which make queries on the attributes.
	 * @throws CQLException
	 * @throws IOException
	 */
	private void queryFeatures(String simpleFeatureTypeName,
			DataStore dataStore,
			String geomField, double x0, double y0, double x1, double y1,
			String dateField, String t0, String t1,
			String attributesQuery)
					throws CQLException, IOException {

		// construct a (E)CQL filter from the search parameters,
		// and use that as the basis for the query
		Filter cql = createFilter(geomField, x0, y0, x1, y1, dateField, t0, t1, attributesQuery);
		Query query = new Query(simpleFeatureTypeName,cql);

		// submit the query, and get back an iterator over matching features
		FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
		FeatureIterator featureItr = featureSource.getFeatures(query).features();

		// loop through all results
		int n = 0;
		while (featureItr.hasNext()) {
			Feature feature = featureItr.next();
			System.out.println((++n) + ".  " +
					feature.getProperty("Who").getValue() + "|" +
					feature.getProperty("What").getValue() + "|" +
					feature.getProperty("When").getValue() + "|" +
					feature.getProperty("Where").getValue() + "|" +
					feature.getProperty("Why").getValue());
		}
		featureItr.close();
	}

	/**
	 * @param geomField: the name of field which represents the geolocation
	 * @param x0: the lower limit of longitude
	 * @param y0: the lower limit of latitude
	 * @param x1: the upper limit of longitude
	 * @param y1: the upper limit of latitude
	 * @param dateField: the name of field which represents the date
	 * @param t0: the lower limit of date range
	 * @param t1: the upper limit of date range
	 * @param attributesQuery: the clause of an SQL statement which make queries on the attributes.
	 * @return Filter:  A Filter is similar to the where clause of an SQL statement; 
	 *         defining a condition that each selected feature needs to meet in order to be included.
	 * @throws CQLException
	 * @throws IOException
	 */
	private Filter createFilter(String geomField, double x0, double y0, double x1, double y1,
			String dateField, String t0, String t1,
			String attributesQuery)
					throws CQLException, IOException {

		// there are many different geometric predicates that might be used;
		// here, we just use a bounding-box (BBOX) predicate as an example.
		// this is useful for a rectangular query area
		String cqlGeometry = "BBOX(" + geomField + ", " +
				x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";

		// there are also quite a few temporal predicates; here, we use a
		// "DURING" predicate, because we have a fixed range of times that
		// we want to query
		String cqlDates = "(" + dateField + " DURING " + t0 + "/" + t1 + ")";

		// there are quite a few predicates that can operate on other attribute
		// types; the GeoTools Filter constant "INCLUDE" is a default that means
		// to accept everything
		String cqlAttributes = attributesQuery == null ? "INCLUDE" : attributesQuery;

		String cql = cqlGeometry + " AND " + cqlDates  + " AND " + cqlAttributes;
		return CQL.toFilter(cql);
	}


	/**
	 * create schema based on the given simpleFeatureType
	 */
	public void createSchema() {
		LOG.info("Entering testCreateSchema.");
		try {
			dataStore.createSchema(simpleFeatureType);
		} catch (IOException e) {
			LOG.error("create schema failed ", e);
		}

		LOG.info("Existing testCreateSchema.");
	}

	
	/**
	 * insert features into specified schema
	 */
	public void insertFeatures(){
		LOG.info("Inserting testInsertFeatures.");

		try {
			// Creating new features
			FeatureCollection featureCollection = createNewFeatures(simpleFeatureType, 1000);
			// Inserting new features
			insertFeatures(simpleFeatureTypeName, dataStore, featureCollection);
		} catch (IOException e) {
			LOG.error("insert features failed ", e);
		}

		LOG.info("Existing testInsertFeatures.");
	}

	/**
	 * query features based on the given cql
	 */
	public void queryFeatures(){
		LOG.info("Inserting testQuerytFeatures.");
		
		try {
			// Querying features
			queryFeatures(simpleFeatureTypeName, dataStore,
					"Where", -78.5, 37.5, -78.0, 38.0,
					"When", "2014-07-01T00:00:00.000Z", "2014-09-30T23:59:59.999Z",
					"(Who = 'Bierce')");
		} catch (IOException e) {
			LOG.error("query features failed ", e);
		} catch (CQLException e) {
			LOG.error("query features failed ", e);
		}

		LOG.info("Existing testQuerytFeatures.");
	}
}

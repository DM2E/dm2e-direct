package eu.dm2e.direct;



//CHECKSTYLE.OFF: JavadocVariable

/**
 * Central list of RDF entities.
 *
 * <p>
 * Every vocabulary is represented by a static final class, which must define
 * a BASE constant (the base URI of the vocabulary) and constants for all entities,
 * (by convention prefixed with CLASS_ for classes and PROP_ for properties).
 * </p>
 *
 * @author Konstantin Baierer
 *
 */
public final class NS {



	/**
	 * Resource types for files, used for content-negotiating webservices, validation...
	 */
	public static final class OMNOM_TYPES {

		public static final String BASE    = "http://onto.dm2e.eu/omnom-types/";
		public static final String XSLT    = BASE + "XSLT";
		public static final String TGZ     = BASE + "TGZ";
		public static final String TGZ_XML = BASE + "TGZ-XML";
		public static final String ZIP_XML = BASE + "ZIP-XML";
		public static final String XML     = BASE + "XML";
	}

	/**
	 * RDFS.
	 */
	public static final class RDFS {
		public static final String BASE = "http://www.w3.org/2000/01/rdf-schema#";
		public static final String PROP_LABEL = BASE + "label";
		public static final String PROP_COMMENT = BASE + "comment";
	}

	/**
	 * Collections Ontology.
	 */
	public static final class CO {

		public static final String BASE = "http://purl.org/co/";

		public static final String PROP_ITEM_CONTENT = BASE + "itemContent";
		public static final String PROP_SIZE		   = BASE + "size";
		public static final String PROP_INDEX        = BASE + "index";
		public static final String PROP_ITEM  	   = BASE + "item";
		public static final String PROP_FIRST_ITEM   = BASE + "firstItem";
		public static final String PROP_NEXT_ITEM   = BASE + "nextItem";
		public static final String PROP_LAST_ITEM   = BASE + "lastItem";

		public static final String CLASS_LIST = BASE + "List";
		public static final String CLASS_ITEM = BASE + "Item";
	}

	/**
	 * RDF.
	 */
	public static final class RDF {

		public static final String BASE      = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		public static final String PROP_TYPE = BASE + "type";
	}

	/** SKOS. */
	public static final class SKOS {

		public static final String BASE       = "http://www.w3.org/2004/02/skos/core#";
		public static final String PROP_LABEL = BASE + "label";
	}

	/**
	 * Dublin Core Elements.
	 */
	public static final class DC {

		public static final String BASE       = "http://purl.org/dc/elements/1.1/";
		public static final String PROP_TITLE = BASE + "title";
		public static final String PROP_DATE  = BASE + "date";

	}

	/**
	 * Dublin Core Terms.
	 */
	public static final class DCTERMS {

		public static final String BASE          = "http://purl.org/dc/terms/";
		public static final String PROP_FORMAT   = BASE + "format";
		public static final String PROP_CREATOR   = BASE + "creator";
		public static final String PROP_EXTENT   = BASE + "extent";
		public static final String PROP_MODIFIED = BASE + "modified";
		public static final String PROP_CREATED  = BASE + "created";

	}

	/**
	 * PROV ontology.
	 */
	public static final class PROV {

		public static final String BASE                   = "http://www.w3.org/ns/prov#";
		public static final String PROP_WAS_GENERATED_BY  = BASE + "wasGeneratedBy";
		public static final String PROP_SPECIALIZATION_OF = BASE + "specializationOf";
        public static final String PROP_WAS_REVISION_OF   = BASE + "wasRevisionOf";
        public static final String PROP_WAS_ASSOCIATED_WITH   = BASE + "wasAssociatedWith";
        public static final String PROP_USED   = BASE + "used";
        public static final String CLASS_ACTIVITY   = BASE + "Activity";

    }

	/**
	 * XML Schema.
	 */
	public static final class XSD {

		public static final String BASE = "http://www.w3.org/2001/XMLSchema#";
		public static final String INT  = BASE + "int";
	}

	/**
	 * VOID dataset description.
	 */
	public static final class VOID {

		public static final String BASE          = "http://rdfs.org/ns/void#";
		public static final String CLASS_DATASET = BASE + "Dataset";
	}

	/**
	 * FOAF Friend of a Friend.
	 */
	public static final class FOAF {

		public static final String BASE         = "http://xmlns.com/foaf/0.1/";
		public static final String CLASS_PERSON = BASE + "Person";
		public static final String PROP_NAME    = BASE + "name";
	}

//	public static final String
////            NS_OMNOM = Config.getString("dm2e.ns.dm2e")
////			, RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
//			, DM2ELOG = "http://onto.dm2e.eu/logging#"
//			;
//	public static final String[] NAMESPACE_MAP;
//
//	static {
//		HashMap<String, String> map = new HashMap<>();
//		map.put("omnom", OMNOM.BASE);
//		map.put("skos", SKOS.BASE);
//		map.put("dc", DC.BASE);
//		map.put("dcterms", DCTERMS.BASE);
//		map.put("dct", DCTERMS.BASE);
//		map.put("prov", PROV.BASE);
//		map.put("rdf", RDF.BASE);
//		map.put("rdfs", RDFS.BASE);
//		map.put("co", CO.BASE);
//		NAMESPACE_MAP = (String[]) Arrays.asList(map).toArray();
//	}
}

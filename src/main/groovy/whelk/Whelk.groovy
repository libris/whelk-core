package whelk

import groovy.util.logging.Log4j2 as Log
import org.apache.commons.collections4.map.LRUMap
import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.filter.JsonLdLinkExpander
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

import java.util.concurrent.ConcurrentHashMap

/**
 * Created by markus on 15-09-03.
 */
@Log
class Whelk {

    PostgreSQLComponent storage
    ElasticSearch elastic
    JsonLdLinkExpander expander
    Map displayData
    Map vocabData
    JsonLd jsonld

    String vocabDisplayUri = "https://id.kb.se/vocab/display" // TODO: encapsulate and configure (LXL-260)
    String vocabUri = "https://id.kb.se/vocab/" // TODO: encapsulate and configure (LXL-260)

    private DocumentCache docCache
    private static ConcurrentHashMap<String, Document> authCache
    private static final int CACHE_MAX_SIZE = 1000
    private static final int CACHE_LIFETIME_MILLIS = 30000

    static {
        authCache = new ConcurrentHashMap<>( )
    }

    static void putInAuthCache(Document authDocument) {
        for (String uri : authDocument.getRecordIdentifiers()) {
            authCache.put(uri, authDocument)
        }
        for (String uri : authDocument.getThingIdentifiers()) {
            authCache.put(uri, authDocument)
        }
        authCache.put(authDocument.getShortId(), authDocument)
    }

    static void removeFromAuthCache(Document authDocument) {
        for (String uri : authDocument.getRecordIdentifiers()) {
            authCache.remove(uri)
        }
        for (String uri : authDocument.getThingIdentifiers()) {
            authCache.remove(uri)
        }
        authCache.remove(authDocument.getShortId())
    }

    public Whelk(PostgreSQLComponent pg, ElasticSearch es) {
        this.storage = pg
        this.elastic = es
        this.docCache = new DocumentCache()
        log.info("Whelk started with storage $storage and index $elastic")
    }

    public Whelk(PostgreSQLComponent pg) {
        this.storage = pg
        this.docCache = new DocumentCache()
        log.info("Whelk started with storage $storage")
    }

    public Whelk() {
        this.docCache = new DocumentCache()
    }

    private class DocumentCache {
        private Map cache
        private int staleCount
        private int cacheHits

        private class Entry {
            Document doc
            long lastAccessed

            Entry(Document doc) {
                this.doc = doc
                this.lastAccessed = System.currentTimeMillis()
            }
        }

        DocumentCache() {
            this.cache = Collections.synchronizedMap(
                new LRUMap<String, Entry>(CACHE_MAX_SIZE))
            this.staleCount = 0
            this.cacheHits = 0
        }

        Document get(String key) {
            synchronized(cache) {
                Entry entry = cache[key]
                if (entry) {
                    cacheHits += 1
                    if(isStale(entry)) {
                        cache.remove(key)
                        staleCount += 1
                        return null
                    } else {
                        return entry.doc
                    }
                } else {
                    return null
                }
            }
        }

        void put(String key, Document doc) {
            synchronized(cache) {
                cache[key] = new Entry(doc)
            }
        }

        int getSize() {
            synchronized(cache) {
                return cache.size()
            }
        }

        int getStaleCount() {
            return staleCount
        }

        int getCacheHits() {
            return cacheHits
        }

        private boolean isStale(Entry entry) {
            return (entry.lastAccessed + CACHE_LIFETIME_MILLIS) <
                   System.currentTimeMillis()
        }
    }

    int cacheSize() {
        return docCache.getSize()
    }

    int cacheStaleCount() {
        return docCache.getStaleCount()
    }

    int cacheHits() {
        return docCache.getCacheHits()
    }

    public static DefaultPicoContainer getPreparedComponentsContainer(Properties properties) {
        DefaultPicoContainer pico = new DefaultPicoContainer(new PropertiesPicoContainer(properties))
        Properties componentProperties = PropertyLoader.loadProperties("component")
        for (comProp in componentProperties) {
            if (comProp.key.endsWith("Class") && comProp.value && comProp.value != "null") {
                log.info("Adding pico component ${comProp.key} = ${comProp.value}")
                pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(Class.forName(comProp.value))
            }
        }
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(Whelk.class)
        return pico
    }

    void loadCoreData() {
        loadDisplayData()
        loadVocabData()
        jsonld = new JsonLd(displayData, vocabData)
    }

    void loadDisplayData() {
        this.displayData = this.storage.locate(vocabDisplayUri, true).document.data
    }

    void loadVocabData() {
        this.vocabData = this.storage.locate(vocabUri, true).document.data
    }

    Map<String, Document> bulkLoad(List ids, boolean useDocumentCache = false) {
        Map result = [:]
        ids.each { id ->

            Document doc

            // Check the auth cache
            if (authCache.containsKey(id)) {
                result[id] = authCache.get(id)
            } else {

                // Check the general purpose cache
                Document cached = docCache.get(id)
                if (useDocumentCache && cached) {
                    result[id] = cached
                } else {

                    // Fetch from DB
                    if (id.startsWith(Document.BASE_URI.toString())) {
                        id = Document.BASE_URI.resolve(id).getPath().substring(1)
                        doc = storage.load(id)
                    } else {
                        doc = storage.locate(id, true)?.document
                    }

                    if (doc && !doc.deleted) {
                        result[id] = doc
                        docCache.put(id, doc)
                        String collection = LegacyIntegrationTools.determineLegacyCollection(doc, jsonld)
                        if (collection == "auth" || collection == "definitions")
                            putInAuthCache(doc)
                    }
                }
            }
        }
        return result
    }

    public void reindexDependers(Document document) {
        List<Tuple2<String, String>> dependers = storage.getDependers(document.getShortId())

        // Filter out "itemOf"-links. In other words, do not bother reindexing hold posts (they're not embellished in elastic)
        List<String> idsToReindex = []
        for (Tuple2<String, String> depender : dependers) {
            if (!depender.get(1).equals("itemOf")) {
                idsToReindex.add( (String) depender.get(0))
            }
        }

        // If the number of dependers isn't too large. Update them synchronously
        if (dependers.size() < 20) {
            Map dependingDocuments = bulkLoad(idsToReindex)
            for (String id : dependingDocuments.keySet()) {
                Document dependingDoc = dependingDocuments.get(id)
                String dependingDocCollection = LegacyIntegrationTools.determineLegacyCollection(dependingDoc, jsonld)
                elastic.index(dependingDoc, dependingDocCollection, this)
            }
        } else {
            // else use a fire-and-forget thread
            Whelk _this = this
            new Thread(new Runnable() {
                void run() {
                    for (String id : idsToReindex) {
                        Document dependingDoc = storage.load(id)
                        String dependingDocCollection = LegacyIntegrationTools.determineLegacyCollection(dependingDoc, jsonld)
                        if (dependingDocCollection != null)
                            elastic.index(dependingDoc, dependingDocCollection, _this)
                    }
                }
            }).start()

        }
    }

    /**
     * Returns tuples for ID collisions, the first entry in the tuple is the system ID of the colliding record,
     * the second is a freetext description of the reason for the collision
     */
    List<Tuple2<String, String>> getIdCollisions(Document document, boolean includingTypedIDs) {

        List<Tuple2<String, String>> collidingSystemIDs = []

        // Identifiers-table lookup on:
        List<String> uriIDs = document.getRecordIdentifiers()
        uriIDs.addAll( document.getThingIdentifiers() )
        for (String uriID : uriIDs) {
            String systemId = storage.getSystemIdByIri(uriID)
            if (systemId != null && systemId != document.getShortId()) {
                log.info("Determined that " + document.getShortId() + " is duplicate of " + systemId + " due to collision on URI: " + uriID)
                collidingSystemIDs.add( new Tuple2(systemId, "on URI: " + uriID) )
            }
        }

        // Typed id queries on:
        List<Tuple> typedIDs = document.getTypedRecordIdentifiers()
        typedIDs.addAll( document.getTypedThingIdentifiers() )
        for (Tuple typedID : typedIDs) {
            String type = typedID[0]
            String value = typedID[1]
            int graphIndex = typedID[2].intValue()
            
            // "Identifier" and "SystemNumber" are too general/meaningless to use for duplication checking.
            if (type.equals("Identifier") || type.equals("SystemNumber"))
                continue

            List<String> collisions = storage.getSystemIDsByTypedID(type, value, graphIndex)
            if (!collisions.isEmpty()) {
                if (includingTypedIDs) {
                    for (String collision : collisions) {
                        if (collision != document.getShortId())
                        collidingSystemIDs.add( new Tuple2(collision, "on typed id: " + type + "," + graphIndex + "," + value) )
                    }
                } else {

                    // We currently are not allowed to enforce typed identifier uniqueness. :(
                    // We can warn at least.
                    log.warn("While testing " + document.getShortId() + " for collisions: Ignoring typed ID collision with : "
                            + collisions + " on " + type + "," + graphIndex + "," + value + " for political reasons.")
                }
            }
        }

        return collidingSystemIDs
    }

    /**
     * NEVER use this to _update_ a document. Use storeAtomicUpdate() instead. Using this for new documents is fine.
     */
    Document createDocument(Document document, String changedIn, String changedBy, String collection, boolean deleted) {

        boolean detectCollisionsOnTypedIDs = false
        List<Tuple2<String, String>> collidingIDs = getIdCollisions(document, detectCollisionsOnTypedIDs)
        if (!collidingIDs.isEmpty()) {
            log.info("Refused initial store of " + document.getShortId() + ". Document considered a duplicate of : " + collidingIDs)
            throw new whelk.exception.StorageCreateFailedException(document.getShortId(), "Document considered a duplicate of : " + collidingIDs)
        }

        if (storage.createDocument(document, changedIn, changedBy, collection, deleted)) {
            if (collection == "auth" || collection == "definitions")
                putInAuthCache(document)
            if (elastic) {
                elastic.index(document, collection, this)
                reindexDependers(document)
            }
        }
        return document
    }

    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, String collection, boolean deleted, PostgreSQLComponent.UpdateAgent updateAgent) {
        Document updated = storage.storeAtomicUpdate(id, minorUpdate, changedIn, changedBy, collection, deleted, updateAgent)
        if (collection == "auth" || collection == "definitions")
            putInAuthCache(updated)
        if (elastic) {
            elastic.index(updated, collection, this)
            reindexDependers(updated)
        }
        return updated
    }

    void bulkStore(final List<Document> documents, String changedIn,
                   String changedBy, String collection, boolean useDocumentCache = false) {
        if (storage.bulkStore(documents, changedIn, changedBy, collection)) {
            for (Document doc : documents) {
                if (collection == "auth" || collection == "definitions") {
                    putInAuthCache(doc)
                }
            }
            if (elastic) {
                elastic.bulkIndex(documents, collection, this, useDocumentCache)
                for (Document doc : documents) {
                    reindexDependers(doc)
                }
            }
        } else {
            log.warn("Bulk store failed, not indexing : ${documents.first().id} - ${documents.last().id}")
        }
    }

    void remove(String id, String changedIn, String changedBy, String collection) {
        log.debug "Deleting ${id} from Whelk"
        Document toBeRemoved = storage.load(id)
        if (storage.remove(id, changedIn, changedBy, collection)) {
            if (collection == "auth" || collection == "definitions")
                removeFromAuthCache(toBeRemoved)
            if (elastic) {
                elastic.remove(id)
                log.debug "Object ${id} was removed from Whelk"
            }
            else {
                log.warn "No Elastic present when deleting. Skipping call to elastic.remove(${id})"
            }
        } else {
            log.warn "storage did not remove ${id} from whelk"
        }
    }
}

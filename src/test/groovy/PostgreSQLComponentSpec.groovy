package whelk.component

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Specification
import groovy.util.logging.Slf4j as Log
import whelk.Document

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp

@Log
class PostgreSQLComponentSpec extends Specification {

    PostgreSQLComponent storage
    def stmt = GroovyMock(PreparedStatement)
    def conn = GroovyMock(Connection)
    def result = GroovyMock(ResultSet)

    static private final ObjectMapper mapper = new ObjectMapper()

    static String documentManifest = mapper.writeValueAsString(["identifier":"testid", "collection": "test"])
    static String documentData = mapper.writeValueAsString(["@id":"testid","name":"foobar"])

    def setup() {
        conn.prepareStatement(_) >> { stmt }
        stmt.executeQuery() >> { result }
        storage = new PostgreSQLComponent(null, "lddb") {
            @Override
            Connection getConnection() {
                println("Getting connection ...")
                conn
            }
        }
    }

    def "should save document to database"() {
        given:
        result.next() >> { true }
        result.getTimestamp(_) >> {
            return new Timestamp(new Date().getTime())
        }
        result.getString("id") >> { return "hej" }
        stmt.executeUpdate() >> { 1 }

        Document doc = null
        when:
        doc = new Document("hej", ["@id": "hej"]).inCollection("test")
        then:
        doc.checksum == null
        doc.id == "hej"
        doc.collection == "test"
        doc.created == null
        doc.modified == null
        and:
        Document r = storage.store(doc, true)
        then:
        r.created != null
        r.modified != null
        r.collection == "test"
        r.id == "hej"
        r.checksum != null
    }

    def "should load document from database"() {
        given:
        2 * result.next() >> { true }
        1 * result.next() >> { false }
        result.getString(_) >> {
            if (it.first() == "id") {
                return "testid"
            }
            if (it.first() == "manifest") {
                return documentManifest
            }
            if (it.first() == "data") {
                return documentData
            }
        }
        result.getTimestamp(_) >> {
            return new Timestamp(new Date().getTime())
        }
        when:
        Document r = storage.load("testid")
        then:
        r.id == "testid"
        r.created != null
        r.modified != null
        r.collection == "test"
        r.deleted == false
    }

    def "should return null for non existing identifier"() {
        given:
        result.next() >> { false }
        when:
        Document r = storage.load("nonexistingid")
        then:
        r == null
    }

    def "should generate correct jsonb query according to storage type"() {
        expect:
        storage.translateToSql(key, value, storageType) == [sqlKey, sqlValue]
        where:
        key                              | value                  | storageType                               | sqlKey                               | sqlValue
        "entry.@id"                      | "/bib/12345"           | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'entry' @> ?" | "{\"@id\":\"/bib/12345\"}"
        "@id"                            | "/resource/auth/345"   | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"@id\":\"/resource/auth/345\"}]"
        "items.title"                    | "Kalldrag"             | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"title\":\"Kalldrag\"}]"
        "items.instanceTitle.titleValue" | "Kalldrag"             | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"instanceTitle\":{\"titleValue\":\"Kalldrag\"}}]"
        "instanceTitle.titleValue"       | "Kalldrag"             | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items' @> ?" | "[{\"instanceTitle\":{\"titleValue\":\"Kalldrag\"}}]"
        "245.a"                          | "Kalldrag"             | StorageType.MARC21_JSON                   | "data->'fields' @> ?"                | "[{\"245\":{\"subfields\":[{\"a\":\"Kalldrag\"}]}}]"
        "024.a"                          | "d12345"               | StorageType.MARC21_JSON                   | "data->'fields' @> ?"                | "[{\"024\":{\"subfields\":[{\"a\":\"d12345\"}]}}]"
    }

    def "should generate correct ORDER BY according to storage type"() {
        expect:
        storage.translateSort(keys, storageType) == orderBy
        where:
        keys                       | storageType                               | orderBy
        "245.a"                    | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC"
        "245.a,024.a"              | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC, data->'fields'->'024'->'subfields'->'a' ASC"
        "245.a,-024.a"             | StorageType.MARC21_JSON                   | "data->'fields'->'245'->'subfields'->'a' ASC, data->'fields'->'024'->'subfields'->'a' DESC"
        "entry.title,entry.-isbn"  | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'entry'->'title' ASC, data->'descriptions'->'entry'->'isbn' DESC"
        "items.title,items.-isbn"  | StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS | "data->'descriptions'->'items'->'title' ASC, data->'descriptions'->'items'->'isbn' DESC"
    }

    def "should calculate correct checksum regardless of created, modified or previous checksum"() {
        when:
        String cs1 = storage.calculateChecksum(new Document("testid", ["key":"some data", "@id": "testid"], ["identifier": "testid", "collection": "test", "created": 1298619287, "modified": 10284701287, "checksum": "qwudhqiuwhdiu12872"]))
        String cs2 = storage.calculateChecksum(new Document("testid", ["@id": "testid", "key":"some data"], ["identifier": "testid", "collection": "test", "created": 1298619287, "modified": 1298461982639, "checksum": "qwudhqiuwhdiu1287ssss2"]))
        String cs3 = storage.calculateChecksum(new Document("testid", ["@id": "testid", "key":"some new data"], ["identifier": "testid", "collection": "test", "created": 1298619287, "modified": 1298461982639, "checksum": "qwudhqiuwhdiu1287ssss2"]))
        then:
        cs1 == "16e8221f8f252482e4c12fd3fcc5703c"
        cs2 == "16e8221f8f252482e4c12fd3fcc5703c"
        cs3 == "eb5a60fe9ba7a02f1b2e58a30328762a"

    }


}

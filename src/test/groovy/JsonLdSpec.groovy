package whelk.util

import com.google.common.base.Charsets
import org.apache.commons.io.IOUtils
import spock.lang.Specification
import spock.lang.Ignore
import org.codehaus.jackson.map.*
import whelk.Document
import whelk.JsonLd
import whelk.exception.FramingException
import whelk.exception.ModelValidationException
import java.lang.StackOverflowError


class JsonLdSpec extends Specification {
    static final String FLAT_INPUT_FILE = "flatten-001-in.jsonld"
    static final String FLAT_OUTPUT_FILE = "flatten-001-out.jsonld"
    static final String FRAMED_INPUT_FILE = "frame-001-in.jsonld"
    static final String FRAMED_OUTPUT_FILE = "frame-001-out.jsonld"

    static final String FLAT_INPUT = read_file(FLAT_INPUT_FILE)
    static final String FLAT_OUTPUT = read_file(FLAT_OUTPUT_FILE)
    static final String FRAMED_INPUT = read_file(FRAMED_INPUT_FILE)
    static final String FRAMED_OUTPUT = read_file(FRAMED_OUTPUT_FILE)

    static final String INPUT_ID = "https://libris.kb.se/record/something"

    static final Map simpleGraph = ["@graph": [["@id": "/foo", "bar": "1"]]]
    static final Map simpleGraphFramed = ["@id": "/foo", "bar": "1"]
    static final Map simpleGraph2 = ["@graph": [["@id": "/foo",
                                                 "bar": ["@id": "/bar"]],
                                                ["@id": "/bar",
                                                 "someValue": 1]]]
    static final Map simpleGraphFramed2 = ["@id": "/foo",
                                           "bar": ["@id": "/bar",
                                                   "someValue": 1]]
    static final Map simpleGraph3 = ["@graph": [["@id": "/foo",
                                                 "bar": ["@id": "/bar"]]]]
    static final Map simpleGraphFramed3 = ["@id": "/foo",
                                           "bar": ["@id": "/bar"]]
    static final Map listGraph = ["@graph": [["@id": "/foo",
                                              "bar": [["@id": "/baz"],
                                                      ["@id": "/quux"]]],
                                             ["@id": "/baz",
                                              "someValue": 1]]]
    static final Map listGraphFramed = ["@id": "/foo",
                                        "bar": [["@id": "/baz",
                                                 "someValue": 1],
                                                ["@id": "/quux"]]]
    static final Map nestedGraph = ["@graph": [["@id": "/foo",
                                                "bar": [
                                                    "baz": ["@id": "/baz"]
                                                ]
                                               ],
                                               ["@id": "/baz",
                                                "someValue": 1]]]
    static final Map nestedGraphFramed = ["@id": "/foo",
                                          "bar": ["baz": ["@id": "/baz",
                                                          "someValue": 1]]]
    static final Map nestedGraph2 = ["@graph": [["@id": "/foo",
                                                 "bar": [
                                                     "baz": ["@id": "/baz"]
                                                 ],
                                                 "quux": ["@id": "/baz"]
                                                ],
                                                ["@id": "/baz",
                                                 "someValue": 1]]]
    static final Map nestedGraphFramed2 = ["@id": "/foo",
                                           "bar": ["baz": ["@id": "/baz",
                                                           "someValue": 1]],
                                           "quux": ["@id": "/baz",
                                                    "someValue": 1]]
    static final Map loopedGraph = ["@graph": [["@id": "/foo",
                                                "bar": ["@id": "/bar"]],
                                               ["@id": "/bar",
                                                "foo": ["@id": "/foo"]]]]
    static final Map quotedGraph = ["@graph": [["@id": "/foo",
                                                "quoted": ["@id": "/quoted"]],
                                               ["@graph": ["@id": "/quoted",
                                                           "quote": "yes"]]]]
    static final Map quotedGraphFramed = ["@id": "/foo",
                                          "quoted": ["@id": "/quoted",
                                                     "quote": "yes"]]
    static final Map quotedGraphFramedExcludeQuotes = ["@id": "/foo",
                                                       "quoted": [
                                                         "@id": "/quoted"
                                                        ]]


    static final ObjectMapper mapper = new ObjectMapper()

    def "should get id map"() {
        expect:
        JsonLd.getIdMap(['@graph': items]).keySet() == ids as Set
        where:
        ids                     | items
        ['/some', '/other']     | [['@id': '/some'], ['@id': '/other']]
        ['/some', '/other']     | [['@id': '/some'], ['@graph': ['@id': '/other']]]
    }

    @Ignore
    // FIXME should we really keep this test?
    def "should frame flat jsonld, I"() {
        given:
        Map input = mapper.readValue(FLAT_OUTPUT, Map)
        Map output = mapper.readValue(FLAT_INPUT, Map)
        expect:
        assert JsonLd.frame(INPUT_ID, input) == output
    }

    def "should flatten jsonld"() {
        given:
        Map input = mapper.readValue(FLAT_INPUT, Map)
        Map output = mapper.readValue(FLAT_OUTPUT, Map)
        expect:
        assert JsonLd.flatten(input) == output
    }

    def "should frame and expand"() {
        given:
        Map input = mapper.readValue(FRAMED_INPUT, Map)
        Map output = mapper.readValue(FRAMED_OUTPUT, Map)
        expect:
        assert JsonLd.frameAndExpand(INPUT_ID, input) == output
    }

    def "should also frame and expand"() {
        expect:
        assert JsonLd.frameAndExpand(id, input) == output
        where:
        id     | input        | output
        "/foo" | simpleGraph  | simpleGraphFramed
        "/foo" | simpleGraph2 | simpleGraphFramed2
        "/foo" | simpleGraph3 | simpleGraphFramed3
        "/foo" | listGraph  | listGraphFramed
        "/foo" | nestedGraph  | nestedGraphFramed
        "/foo" | quotedGraph  | quotedGraphFramed
    }

    def "should flatten framed jsonld"() {
        expect:
        assert JsonLd.flatten(input) == output
        where:
        input              | output
        simpleGraphFramed  | simpleGraph
        simpleGraphFramed2 | simpleGraph2
        simpleGraphFramed3 | simpleGraph3
        listGraphFramed    | listGraph
        nestedGraphFramed  | nestedGraph
        nestedGraphFramed2 | nestedGraph2
        // quotedGraphFramed  | quotedGraph
    }

    def "should not include quoted items in framing"() {
        expect:
        assert JsonLd.frameAndExpand(id, input, false) == output
        where:
        id     | input       | output
        "/foo" | quotedGraph | quotedGraphFramedExcludeQuotes
    }

    def "should not frame and expand"() {
        when:
        JsonLd.frameAndExpand(id, input)
        then:
        def error = thrown(expectedException)
        assert error.message == expectedMessage
        where:
        id     | input       | expectedException | expectedMessage
        "/foo" | [:]         | FramingException  | "Missing '@graph' key in input"
        "/foo" | loopedGraph | FramingException  | "Circular dependency in input"
    }

    def "should expand references"() {
        given:
        String mainId = "/foo"
        Map input = ["@graph": [["@id": "/foo",
                                 "bar": ["@id": "/bar"]],
                                ["@id": "/bar",
                                 "baz": ["quux": ["@id": "/quux"]]],
                                ["@id": "/quux",
                                 "someValue": 1]]]
        Map output = ["@id": "/foo",
                      "bar": ["@id": "/bar",
                              "baz": ["quux": ["@id": "/quux",
                                               "someValue": 1]]]]
        expect:
        assert JsonLd.frameAndExpand(mainId, input) == output
    }

    def "should find root objects with @id"() {
        given:
        Map input = ["@graph": [["@id": "/foo",
                                 "bar": ["@id": "/bar"]],
                                ["@id": "/bar",
                                 "baz": ["quux": ["@id": "/quux"]]],
                                ["@id": "/quux",
                                 "someValue": 1],
                                ["someOtherValue": 2],
                                "A lonely string"]]
        Map output = ["/foo": ["@id": "/foo",
                               "bar": ["@id": "/bar"]],
                      "/bar": ["@id": "/bar",
                               "baz": ["quux": ["@id": "/quux"]]],
                      "/quux": ["@id": "/quux",
                                "someValue": 1]]
        expect:
        assert JsonLd.getObjectsWithId(input) == output
    }

    def "should frame flat jsonld"() {
        given:
        def id = "1234"
        def documentId = Document.BASE_URI.resolve(id).toString()
        def input = ["@graph": [["@id": documentId, "foo": "bar"]]]
        def expected = ["@id": documentId, "foo": "bar"]
        expect:
        assert JsonLd.frame(id, input) == expected
    }

    def "framing framed jsonld should preserve input"() {
        given:
        def id = "1234"
        def documentId = Document.BASE_URI.resolve(id).toString()
        def input = ["@id": documentId, "foo": "bar"]
        expect:
        assert JsonLd.frame(id, input) == input
    }

    def "should flatten framed jsonld"() {
        given:
        def id = "1234"
        def documentId = Document.BASE_URI.resolve(id).toString()
        def input = ["@id": documentId, "foo": "bar"]
        def expected = ["@graph": [["@id": documentId, "foo": "bar"]]]
        expect:
        assert JsonLd.flatten(input) == expected
    }

    def "flattening flat jsonld should preserve input"() {
        given:
        def id = "1234"
        def documentId = Document.BASE_URI.resolve(id).toString()
        def input = ["@graph": [["@id": documentId, "foo": "bar"]]]
        expect:
        assert JsonLd.flatten(input) == input
    }

    def "should preserve unframed json"() {
        expect:
        JsonLd.flatten(mundaneJson).equals(mundaneJson)
        where:
        mundaneJson = ["data":"foo","sameAs":"/some/other"]
    }

    def "should detect flat jsonld"() {
        given:
        def flatInput = """
        {"@graph": [{"@id": "/bib/13531679",
                     "encLevel": {"@id": "/def/enum/record/AbbreviatedLevel"},
                     "catForm": {"@id":"/def/enum/record/AACR2"},
                     "@type": "Record",
                     "controlNumber": "13531679",
                     "created": "2013-03-02T00:00:00.0+01:00",
                     "catalogingSource": {"@id":"/def/enum/content/OtherSource"},
                     "modified":"2015-09-23T15:20:09.0+02:00",
                     "systemNumber": ["(Elib)9789174771107"],
                     "technicalNote": ["Ändrad av Elib 2013-03-01"],
                     "_marcUncompleted": [{"655": {"ind1": " ",
                                                   "ind2": "4",
                                                   "subfields": [{"a": "E-böcker"}]},
                                           "_unhandled": ["ind2"]},
                                          {"655": {"ind1": " ",
                                                   "ind2": "4",
                                                   "subfields": [{"a":"Unga vuxna"}]},
                                           "_unhandled": ["ind2"]}],
                     "about": {"@id": "/resource/bib/13531679"}}]}
        """
        def framedInput = """
        {"@id": "/bib/13531679",
         "encLevel": {"@id": "/def/enum/record/AbbreviatedLevel"},
         "catForm": {"@id":"/def/enum/record/AACR2"},
         "@type": "Record",
         "controlNumber": "13531679",
         "created": "2013-03-02T00:00:00.0+01:00",
         "catalogingSource": {"@id":"/def/enum/content/OtherSource"},
         "modified":"2015-09-23T15:20:09.0+02:00",
         "systemNumber": ["(Elib)9789174771107"],
         "technicalNote": ["Ändrad av Elib 2013-03-01"],
         "_marcUncompleted": [{"655": {"ind1": " ",
                                       "ind2": "4",
                                       "subfields": [{"a": "E-böcker"}]},
                               "_unhandled": ["ind2"]},
                              {"655": {"ind1": " ",
                                       "ind2": "4",
                                        "subfields": [{"a":"Unga vuxna"}]},
                               "_unhandled": ["ind2"]}],
         "about": {"@id": "/resource/bib/13531679"}}]}
        """
        def flatJson = mapper.readValue(flatInput, Map)
        def framedJson = mapper.readValue(framedInput, Map)
        expect:
        JsonLd.isFlat(flatJson) == true
        JsonLd.isFlat(framedJson) == false
    }

    def  "should retrieve actual URI from @id in document"() {
        when:
        URI uri1 = JsonLd.findRecordURI(
            ["@graph": [["@id": "/qowiudhqw",
                         "name": "foo"]]])
        URI uri2 = JsonLd.findRecordURI(
            ["@graph": [["@id": "http://id.kb.se/foo/bar",
                         "name": "foo"]]])
        URI uri3 = JsonLd.findRecordURI(["data":"foo","sameAs":"/some/other"])
        then:
        uri1.toString() == Document.BASE_URI.toString() + "qowiudhqw"
        uri2.toString() == "http://id.kb.se/foo/bar"
        uri3 == null
    }

    def "should find database id from @id in document"() {
        when:
        String s1 = JsonLd.findIdentifier(
            ["@graph": [["@id": "/qowiudhqw",
                         "name": "foo"]]])
        String s2 = JsonLd.findIdentifier(
            ["@graph": [["@id": "http://id.kb.se/foo/bar",
                         "name": "foo"]]])
        String s3 = JsonLd.findIdentifier(
            ["@graph": [["@id": Document.BASE_URI.resolve("/qowiudhqw").toString(),
                         "name": "foo"]]])
        then:
        s1 == "qowiudhqw"
        s2 == "http://id.kb.se/foo/bar"
        s3 == "qowiudhqw"
    }

    // FIXME This test contains invalid input data
    @Ignore()
    def "should validate item model"() {
        when:
        def id = Document.BASE_URI.resolve("/valid1").toString()
        def mainEntityId = Document.BASE_URI.resolve("/valid1main").toString()
        def validDocument = new Document(
            ["@graph": [["@id": id,
                         "@type": "HeldMaterial",
                         "numberOfItems": 1,
                         "mainEntity": [
                           "@id": mainEntityId
                         ],
                         "heldBy": ["@type":"Organization",
                                    "notation":"Gbg"],
                         "holdingFor": ["@id": "https://libris.kb.se/foobar"]]]
            ])
        def invalidDocument = new Document(["foo": "bar"])

        then:
        assert JsonLd.validateItemModel(validDocument)
        assert !JsonLd.validateItemModel(invalidDocument)
    }

    static String read_file(String filename) {
        InputStream is = JsonLdSpec.class.getClassLoader().getResourceAsStream(filename)
        return IOUtils.toString(is, Charsets.UTF_8)
    }
}

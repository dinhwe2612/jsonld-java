package com.github.jsonldjava.core;

import static com.github.jsonldjava.core.RDFDatasetUtils.parseNQuads;
import static com.github.jsonldjava.core.RDFDatasetUtils.toNQuad;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the URDNA2015 RDF Dataset Normalization Algorithm.
 * This algorithm provides deterministic canonicalization of RDF datasets
 * with blank nodes.
 * 
 * Based on the W3C RDF Dataset Normalization specification.
 */
public class Urdna2015 {

    private static final Map<String, String> QUAD_POSITIONS = new HashMap<String, String>() {
        {
            put("subject", "s");
            put("object", "o");
            put("name", "g");
        }
    };

    private List<Map<String, Map<String, String>>> quads;
    private Map<String, Map<String, List<Object>>> blankNodeInfo;
    private Map<String, List<String>> hashToBlankNodes;
    private IdentifierIssuer canonicalIssuer;

    private Map<String, Object> dataset;
    private JsonLdOptions options;

    public Urdna2015(Map<String, Object> dataset, JsonLdOptions options) {
        this.dataset = dataset;
        this.options = options;
    }

    public Object normalize() throws JsonLdError {
        this.quads = new ArrayList<>();
        this.blankNodeInfo = new HashMap<>();
        this.hashToBlankNodes = new HashMap<>();
        this.canonicalIssuer = new IdentifierIssuer("_:c14n");

        /*
         * 2) For every quad in input dataset:
         */
        for (String graphName : this.dataset.keySet()) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> triples = (List<Map<String, Object>>) this.dataset.get(graphName);

            if (JsonLdConsts.DEFAULT.equals(graphName)) {
                graphName = null;
            }

            for (Map<String, Object> quad : triples) {
                // Convert quad to the format expected by URDNA2015
                Map<String, Map<String, String>> quadMap = convertQuadToMap(quad);
                
                if (graphName != null) {
                    if (graphName.startsWith("_:")) {
                        Map<String, String> tmp = new HashMap<>();
                        tmp.put("type", "blank node");
                        quadMap.put("name", tmp);
                    } else {
                        Map<String, String> tmp = new HashMap<>();
                        tmp.put("type", "IRI");
                        quadMap.put("name", tmp);
                    }
                    quadMap.get("name").put("value", graphName);
                }
                this.quads.add(quadMap);

                /*
                 * 2.1) For each blank node that occurs in the quad, add a
                 * reference to the quad using the blank node identifier in the
                 * blank node to quads map, creating a new entry if necessary.
                 */
                for (String key : quadMap.keySet()) {
                    Map<String, String> component = quadMap.get(key);
                    if (key.equals("predicate") || !"blank node".equals(component.get("type"))) {
                        continue;
                    }
                    String id = component.get("value");
                    if (this.blankNodeInfo.get(id) == null) {
                        Map<String, List<Object>> quadList = new HashMap<>();
                        quadList.put("quads", new ArrayList<>());
                        quadList.get("quads").add(quadMap);
                        this.blankNodeInfo.put(id, quadList);
                    } else {
                        this.blankNodeInfo.get(id).get("quads").add(quadMap);
                    }
                }
            }
        }

        /*
         * 3) Create a list of non-normalized blank node identifiers and
         * populate it using the keys from the blank node to quads map.
         */
        List<String> nonNormalized = new ArrayList<>();
        nonNormalized.addAll(blankNodeInfo.keySet());

        /*
         * 4) Initialize simple, a boolean flag, to true.
         */
        boolean simple = true;

        /*
         * 5) While simple is true, issue canonical identifiers for blank nodes:
         */
        while (simple) {
            // 5.1) Set simple to false.
            simple = false;

            // 5.2) Clear hash to blank nodes map.
            this.hashToBlankNodes.clear();

            /*
             * 5.3) For each blank node identifier identifier in non-normalized
             * identifiers:
             */
            for (String id : nonNormalized) {
                String hash = hashFirstDegreeQuads(id);

                if (this.hashToBlankNodes.containsKey(hash)) {
                    this.hashToBlankNodes.get(hash).add(id);
                } else {
                    List<String> idList = new ArrayList<>();
                    idList.add(id);
                    this.hashToBlankNodes.put(hash, idList);
                }
            }

            /*
             * 5.4) For each hash to identifier list mapping in hash to blank
             * nodes map, lexicographically-sorted by hash:
             */
            for (String hash : NormalizeUtils.sortMapKeys(this.hashToBlankNodes)) {
                List<String> idList = this.hashToBlankNodes.get(hash);
                if (idList.size() > 1) {
                    continue;
                }

                /*
                 * 5.4.2) Use the Issue Identifier algorithm, passing canonical
                 * issuer and the single blank node identifier in identifier
                 * list, identifier, to issue a canonical replacement identifier
                 * for identifier.
                 */
                String id = idList.get(0);
                this.canonicalIssuer.getId(id);

                // 5.4.3) Remove identifier from non-normalized identifiers.
                nonNormalized.remove(id);

                // 5.4.4) Remove hash from the hash to blank nodes map.
                this.hashToBlankNodes.remove(hash);

                // 5.4.5) Set simple to true.
                simple = true;
            }
        }

        /*
         * 6) For each hash to identifier list mapping in hash to blank nodes
         * map, lexicographically-sorted by hash:
         */
        for (String hash : NormalizeUtils.sortMapKeys(this.hashToBlankNodes)) {
            List<String> idList = this.hashToBlankNodes.get(hash);

            /*
             * 6.1) Create hash path list where each item will be a result of
             * running the Hash N-Degree Quads algorithm.
             */
            List<Map<String, Object>> hashPathList = new ArrayList<>();

            /*
             * 6.2) For each blank node identifier identifier in identifier
             * list:
             */
            for (String id : idList) {
                /*
                 * 6.2.1) If a canonical identifier has already been issued for
                 * identifier, continue to the next identifier.
                 */
                if (this.canonicalIssuer.hasID(id)) {
                    continue;
                }

                /*
                 * 6.2.2) Create temporary issuer, an identifier issuer
                 * initialized with the prefix _:b.
                 */
                IdentifierIssuer issuer = new IdentifierIssuer("_:b");

                /*
                 * 6.2.3) Use the Issue Identifier algorithm, passing temporary
                 * issuer and identifier, to issue a new temporary blank node
                 * identifier for identifier.
                 */
                issuer.getId(id);

                /*
                 * 6.2.4) Run the Hash N-Degree Quads algorithm, passing
                 * temporary issuer, and append the result to the hash path
                 * list.
                 */
                hashPathList.add(hashNDegreeQuads(issuer, id));
            }

            /*
             * 6.3) For each result in the hash path list,
             * lexicographically-sorted by the hash in result:
             */
            NormalizeUtils.sortMapList(hashPathList);
            for (Map<String, Object> result : hashPathList) {
                if (result.get("issuer") != null) {
                    @SuppressWarnings("unchecked")
                    IdentifierIssuer resultIssuer = (IdentifierIssuer) result.get("issuer");
                    for (String existing : resultIssuer.getOrder()) {
                        this.canonicalIssuer.getId(existing);
                    }
                }
            }
        }

        /*
         * Note: At this point all blank nodes in the set of RDF quads have been
         * assigned canonical identifiers, which have been stored in the
         * canonical issuer. Here each quad is updated by assigning each of its
         * blank nodes its new identifier.
         */

        // 7) For each quad, quad, in input dataset:
        List<String> normalized = new ArrayList<>();
        for (Map<String, Map<String, String>> quadMap : this.quads) {
            /*
             * Create a copy, quad copy, of quad and replace any existing
             * blank node identifiers using the canonical identifiers previously
             * issued by canonical issuer.
             */
            for (String key : quadMap.keySet()) {
                if (key.equals("predicate")) {
                    continue;
                } else {
                    Map<String, String> component = quadMap.get(key);
                    if ("blank node".equals(component.get("type"))
                            && !component.get("value").startsWith(this.canonicalIssuer.getPrefix())) {
                        component.put("value", this.canonicalIssuer.getId(component.get("value")));
                    }
                }
            }

            // 7.2) Add quad copy to the normalized dataset.
            RDFDataset.Quad quad = convertMapToQuad(quadMap);
            String graphName = quadMap.containsKey("name") && quadMap.get("name") != null
                    ? quadMap.get("name").get("value")
                    : null;
            normalized.add(toNQuad(quad, graphName));
        }

        // 8) Return the normalized dataset.
        Collections.sort(normalized);
        if (this.options.format != null) {
            if (JsonLdConsts.APPLICATION_NQUADS.equals(this.options.format)) {
                StringBuilder rval = new StringBuilder();
                for (String n : normalized) {
                    rval.append(n);
                }
                return rval.toString();
            } else {
                throw new JsonLdError(JsonLdError.Error.UNKNOWN_FORMAT, this.options.format);
            }
        } else {
            StringBuilder rval = new StringBuilder();
            for (final String n : normalized) {
                rval.append(n);
            }
            return parseNQuads(rval.toString());
        }
    }

    /**
     * Converts a quad from Map<String, Object> format (as used in JsonLdApi)
     * to Map<String, Map<String, String>> format (as expected by URDNA2015)
     */
    @SuppressWarnings("unchecked")
    private Map<String, Map<String, String>> convertQuadToMap(Map<String, Object> quad) {
        Map<String, Map<String, String>> quadMap = new LinkedHashMap<>();
        
        // Convert subject
        Map<String, Object> subject = (Map<String, Object>) quad.get("subject");
        Map<String, String> subjectMap = new HashMap<>();
        subjectMap.put("type", (String) subject.get("type"));
        subjectMap.put("value", (String) subject.get("value"));
        quadMap.put("subject", subjectMap);
        
        // Convert predicate
        Map<String, Object> predicate = (Map<String, Object>) quad.get("predicate");
        Map<String, String> predicateMap = new HashMap<>();
        predicateMap.put("type", (String) predicate.get("type"));
        predicateMap.put("value", (String) predicate.get("value"));
        quadMap.put("predicate", predicateMap);
        
        // Convert object
        Map<String, Object> object = (Map<String, Object>) quad.get("object");
        Map<String, String> objectMap = new HashMap<>();
        objectMap.put("type", (String) object.get("type"));
        objectMap.put("value", (String) object.get("value"));
        // Preserve datatype and language for literals
        if ("literal".equals(object.get("type"))) {
            String datatype = (String) object.get("datatype");
            if (datatype != null) {
                objectMap.put("datatype", datatype);
            }
            String language = (String) object.get("language");
            if (language != null) {
                objectMap.put("language", language);
            }
        }
        quadMap.put("object", objectMap);
        
        return quadMap;
    }

    /**
     * Converts a quad from Map<String, Map<String, String>> format back to
     * RDFDataset.Quad format
     */
    private RDFDataset.Quad convertMapToQuad(Map<String, Map<String, String>> quadMap) {
        Map<String, String> subjectMap = quadMap.get("subject");
        Map<String, String> predicateMap = quadMap.get("predicate");
        Map<String, String> objectMap = quadMap.get("object");
        
        // Create subject Node
        RDFDataset.Node subject;
        String subjectType = subjectMap.get("type");
        String subjectValue = subjectMap.get("value");
        if ("blank node".equals(subjectType)) {
            subject = new RDFDataset.BlankNode(subjectValue);
        } else {
            subject = new RDFDataset.IRI(subjectValue);
        }
        
        // Create predicate Node (should always be IRI)
        RDFDataset.Node predicate = new RDFDataset.IRI(predicateMap.get("value"));
        
        // Create object Node - check type to create correct Node type
        RDFDataset.Node object;
        String objectType = objectMap.get("type");
        String objectValue = objectMap.get("value");
        if ("blank node".equals(objectType)) {
            object = new RDFDataset.BlankNode(objectValue);
        } else if ("literal".equals(objectType)) {
            // Create Literal with datatype and language if available
            String datatype = objectMap.get("datatype");
            String language = objectMap.get("language");
            object = new RDFDataset.Literal(objectValue, datatype, language);
        } else {
            // IRI
            object = new RDFDataset.IRI(objectValue);
        }
        
        String graph = quadMap.containsKey("name") && quadMap.get("name") != null
                ? quadMap.get("name").get("value")
                : null;
        
        return new RDFDataset.Quad(subject, predicate, object, graph);
    }

    private String hashFirstDegreeQuads(String id) {
        // return cached hash
        Map<String, List<Object>> info = this.blankNodeInfo.get(id);
        if (info.containsKey("hash")) {
            return String.valueOf(info.get("hash"));
        }

        // 1) Initialize nquads to an empty list. It will be used to store quads
        // in N-Quads format.
        List<String> nquads = new ArrayList<>();

        // 2) Get the list of quads quads associated with the reference blank
        // node identifier in the blank node to quads map.
        @SuppressWarnings("unchecked")
        List<Object> quads = (List<Object>) info.get("quads");

        // 3) For each quad quad in quads:
        for (Object quadObj : quads) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, String>> quad = (Map<String, Map<String, String>>) quadObj;
            
            // 3.1) Serialize the quad in N-Quads format with the following
            // special rule:
            // 3.1.1) If any component in quad is an blank node, then serialize
            // it using a special identifier as follows:
            // 3.1.2) If the blank node's existing blank node identifier
            // matches the reference blank node identifier then use the
            // blank node identifier _:a, otherwise, use the blank node
            // identifier _:z.

            Map<String, Map<String, String>> copy = new HashMap<>();
            for (String key : quad.keySet()) {
                Map<String, String> component = quad.get(key);
                if (key.equals("predicate")) {
                    copy.put(key, component);
                    continue;
                }
                copy.put(key, modifyFirstDegreeComponent(component, id));
            }
            
            RDFDataset.Quad copyQuad = convertMapToQuad(copy);
            String graphName = copy.containsKey("name") && copy.get("name") != null
                    ? copy.get("name").get("value")
                    : null;
            nquads.add(toNQuad(copyQuad, graphName));
        }
        
        // 4) Sort nquads in lexicographical order.
        Collections.sort(nquads);
        
        // 5) Return the hash that results from passing the sorted, joined
        // nquads through the hash algorithm.
        String hash = NormalizeUtils.sha256HashnQuads(nquads);
        info.put("hash", new ArrayList<Object>() {{ add(hash); }});
        return hash;
    }

    private Map<String, Object> hashNDegreeQuads(IdentifierIssuer issuer, String id) {
        /*
         * 1) Create a hash to related blank nodes map for storing hashes that
         * identify related blank nodes.
         * Note: 2) and 3) handled within `createHashToRelated`
         */
        Map<String, List<String>> hashToRelated = this.createHashToRelated(issuer, id);

        /*
         * 4) Create an empty string, data to hash.
         */
        StringBuilder mdString = new StringBuilder();

        /*
         * 5) For each related hash to blank node list mapping in hash to
         * related blank nodes map, sorted lexicographically by related hash:
         */
        NormalizeUtils.sortMapKeys(hashToRelated);
        for (String hash : hashToRelated.keySet()) {
            List<String> blankNodes = hashToRelated.get(hash);
            // 5.1) Append the related hash to the data to hash.
            mdString.append(hash);

            // 5.2) Create a string chosen path.
            String chosenPath = "";

            // 5.3) Create an unset chosen issuer variable.
            IdentifierIssuer chosenIssuer = null;

            // 5.4) For each permutation of blank node list:
            String path = "";
            List<String> recursionList = null;
            IdentifierIssuer issuerCopy = null;
            boolean skipToNextPerm = false;
            NormalizeUtils.Permutator permutator = new NormalizeUtils.Permutator(blankNodes);

            while (permutator.hasNext()) {
                List<String> permutation = permutator.next();
                // 5.4.1) Create a copy of issuer, issuer copy.
                issuerCopy = (IdentifierIssuer) issuer.clone();

                // 5.4.2) Create a string path.
                path = "";

                /*
                 * 5.4.3) Create a recursion list, to store blank node
                 * identifiers that must be recursively processed by this
                 * algorithm.
                 */
                recursionList = new ArrayList<>();

                // 5.4.4) For each related in permutation:
                for (String related : permutation) {
                    /*
                     * 5.4.4.1) If a canonical identifier has been issued for
                     * related, append it to path.
                     */
                    if (this.canonicalIssuer.hasID(related)) {
                        path += this.canonicalIssuer.getId(related);
                    }
                    // 5.4.4.2) Otherwise:
                    else {
                        /*
                         * 5.4.4.2.1) If issuer copy has not issued an
                         * identifier for related, append related to recursion
                         * list.
                         */
                        if (!issuerCopy.hasID(related)) {
                            recursionList.add(related);
                        }

                        /*
                         * 5.4.4.2.2) Use the Issue Identifier algorithm,
                         * passing issuer copy and related and append the result
                         * to path.
                         */
                        path += issuerCopy.getId(related);
                    }

                    /*
                     * 5.4.4.3) If chosen path is not empty and the length of
                     * path is greater than or equal to the length of chosen
                     * path and path is lexicographically greater than chosen
                     * path, then skip to the next permutation.
                     */
                    if (chosenPath.length() != 0 && path.length() >= chosenPath.length()
                            && path.compareTo(chosenPath) > 0) {
                        skipToNextPerm = true;
                        break;
                    }
                }

                if (skipToNextPerm) {
                    skipToNextPerm = false;
                    continue;
                }

                // 5.4.5) For each related in recursion list:
                for (String related : recursionList) {
                    /*
                     * 5.4.5.1) Set result to the result of recursively
                     * executing the Hash N-Degree Quads algorithm, passing
                     * related for identifier and issuer copy for path
                     * identifier issuer.
                     */
                    Map<String, Object> result = hashNDegreeQuads(issuerCopy, related);

                    /*
                     * 5.4.5.2) Use the Issue Identifier algorithm, passing
                     * issuer copy and related and append the result to path.
                     */
                    path += "<" + result.get("hash") + ">";

                    /*
                     * 5.4.5.4) Set issuer copy to the identifier issuer in
                     * result.
                     */
                    issuerCopy = (IdentifierIssuer) result.get("issuer");

                    /*
                     * 5.4.5.5) If chosen path is not empty and the length of
                     * path is greater than or equal to the length of chosen
                     * path and path is lexicographically greater than chosen
                     * path, then skip to the next permutation.
                     */
                    if (chosenPath.length() != 0 && path.length() >= chosenPath.length()
                            && path.compareTo(chosenPath) > 0) {
                        skipToNextPerm = true;
                        break;
                    }
                }

                if (skipToNextPerm) {
                    skipToNextPerm = false;
                    continue;
                }

                /*
                 * 5.4.6) If chosen path is empty or path is lexicographically
                 * less than chosen path, set chosen path to path and chosen
                 * issuer to issuer copy.
                 */
                if (chosenPath.length() == 0 || path.compareTo(chosenPath) < 0) {
                    chosenPath = path;
                    chosenIssuer = issuerCopy;
                }
            }

            // 5.5) Append chosen path to data to hash.
            mdString.append(chosenPath);

            // 5.6) Replace issuer, by reference, with chosen issuer.
            issuer = chosenIssuer;
        }

        /*
         * 6) Return issuer and the hash that results from passing data to hash
         * through the hash algorithm.
         */
        Map<String, Object> hashQuad = new HashMap<>();
        hashQuad.put("hash", NormalizeUtils.sha256Hash(mdString.toString().getBytes()));
        hashQuad.put("issuer", issuer);

        return hashQuad;
    }

    private Map<String, List<String>> createHashToRelated(IdentifierIssuer issuer, String id) {
        /*
         * 1) Create a hash to related blank nodes map for storing hashes that
         * identify related blank nodes.
         */
        @SuppressWarnings("unchecked")
        List<Object> quads = (List<Object>) this.blankNodeInfo.get(id).get("quads");

        Map<String, List<String>> hashToRelated = new HashMap<>();

        /*
         * 2) Get a reference, quads, to the list of quads in the blank node to
         * quads map for the key identifier.
         * Already in parameter
         */

        // 3) For each quad in quads:
        for (Object quadObj : quads) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, String>> quad = (Map<String, Map<String, String>>) quadObj;

            /*
             * 3.1) For each component in quad, if component is the subject,
             * object, and graph name and it is a blank node that is not
             * identified by identifier:
             */
            for (String key : quad.keySet()) {
                Map<String, String> component = quad.get(key);
                if (!key.equals("predicate") && "blank node".equals(component.get("type"))
                        && !component.get("value").equals(id)) {

                    /*
                     * 3.1.1) Set hash to the result of the Hash Related Blank
                     * Node algorithm, passing the blank node identifier for
                     * component as related, quad, path identifier issuer as
                     * issuer, and position as either s, o, or g based on
                     * whether component is a subject, object, graph name,
                     * respectively.
                     */
                    String related = component.get("value");
                    String position = QUAD_POSITIONS.get(key);

                    String hash = hashRelateBlankNode(related, quad, issuer, position);

                    if (hashToRelated.containsKey(hash)) {
                        hashToRelated.get(hash).add(related);
                    } else {
                        List<String> relatedList = new ArrayList<>();
                        relatedList.add(related);
                        hashToRelated.put(hash, relatedList);
                    }
                }
            }
        }

        return hashToRelated;
    }

    private String hashRelateBlankNode(String related, Map<String, Map<String, String>> quad,
            IdentifierIssuer issuer, String position) {
        /*
         * 1) Set the identifier to use for related, preferring first the
         * canonical identifier for related if issued, second the identifier
         * issued by issuer if issued, and last, if necessary, the result of
         * the Hash First Degree Quads algorithm, passing related.
         */
        String id;
        if (this.canonicalIssuer.hasID(related)) {
            id = this.canonicalIssuer.getId(related);
        } else if (issuer.hasID(related)) {
            id = issuer.getId(related);
        } else {
            id = hashFirstDegreeQuads(related);
        }

        /*
         * 2) Initialize a string input to the value of position.
         */
        if (!position.equals("g")) {
            String predicate = "<" + quad.get("predicate").get("value") + ">";
            return NormalizeUtils.sha256Hash((position + predicate + id).getBytes());
        } else {
            return NormalizeUtils.sha256Hash((position + id).getBytes());
        }
    }

    private static Map<String, String> modifyFirstDegreeComponent(Map<String, String> component, String id) {
        if (!"blank node".equals(component.get("type"))) {
            return component;
        }
        Map<String, String> componentClone = new HashMap<>(component);
        if (componentClone.get("value").equals(id)) {
            componentClone.put("value", "_:a");
        } else {
            componentClone.put("value", "_:z");
        }
        return componentClone;
    }
}


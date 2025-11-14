package com.github.jsonldjava.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An IdentifierIssuer issues unique identifiers, keeping track of any
 * previously issued identifiers. Used by the URDNA2015 normalization algorithm.
 */
public class IdentifierIssuer implements Cloneable {

    private String prefix;
    private int counter;
    private Map<String, String> existing;
    private List<String> order;

    public IdentifierIssuer(String prefix) {
        this.prefix = prefix;
        this.counter = 0;
        this.existing = new HashMap<>();
        this.order = new ArrayList<>();
    }

    /**
     * Gets the new identifier for the given old identifier, where if no old
     * identifier is given a new identifier will be generated.
     * 
     * @param old the old identifier to get the new identifier for (can be null)
     * @return the new identifier
     */
    public String getId() {
        return this.getId(null);
    }

    public String getId(String old) {
        if (old != null && existing.containsKey(old)) {
            return existing.get(old);
        }

        String id = this.prefix + Integer.toString(counter);
        this.counter += 1;

        if (old != null) {
            this.existing.put(old, id);
            this.order.add(old);
        }

        return id;
    }

    /**
     * Returns True if the given old identifier has already been assigned a
     * new identifier.
     * 
     * @param old the old identifier to check
     * @return True if the old identifier has been assigned a new identifier,
     *         False if not
     */
    public boolean hasID(String old) {
        return this.existing.containsKey(old);
    }

    public List<String> getOrder() {
        return this.order;
    }

    public String getPrefix() {
        return this.prefix;
    }

    @Override
    public Object clone() {
        try {
            IdentifierIssuer cloned = (IdentifierIssuer) super.clone();
            cloned.existing = new HashMap<>(this.existing);
            cloned.order = new ArrayList<>(this.order);
            return cloned;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}


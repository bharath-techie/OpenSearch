package com.parquet.parquetdataformat.converter;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Represents a field type for Parquet-based document fields.
 * 
 * <p>This class encapsulates the field name and Arrow type information
 * required for proper type mapping between OpenSearch fields and Parquet
 * column definitions. It serves as the intermediate representation used
 * throughout the Parquet processing pipeline.
 * 
 * <p>The Arrow type system provides a rich set of data types that can
 * accurately represent various field types from OpenSearch, ensuring
 * proper data serialization and deserialization.
 * 
 * <p>Key features:
 * <ul>
 *   <li>Field name preservation for schema mapping</li>
 *   <li>Arrow type integration for precise data representation</li>
 *   <li>Simple mutable structure for field definition building</li>
 * </ul>
 */
public class ParquetFieldType {
    private String name;
    private ArrowType type;

    public ParquetFieldType(String name, ArrowType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrowType getType() {
        return type;
    }

    public void setType(ArrowType type) {
        this.type = type;
    }
}

package io.debezium.connector.yugabytedb;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;
import io.debezium.relational.*;
import io.debezium.relational.mapping.ColumnMapper;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Strings;

public class YugabyteDBTableSchemaBuilder extends TableSchemaBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBTableSchemaBuilder.class);

    private final SchemaNameAdjuster schemaNameAdjuster;
    // private final ValueConverterProvider valueConverterProvider;
    private final Schema sourceInfoSchema;
    // private final FieldNameSelector.FieldNamer<Column> fieldNamer;
    // private final CustomConverterRegistry customConverterRegistry;

    /**
     * Create a new instance of the builder.
     *  @param valueConverterProvider the provider for obtaining {@link ValueConverter}s and {@link SchemaBuilder}s; may not be
     *            null
     * @param schemaNameAdjuster the adjuster for schema names; may not be null
     * @param customConverterRegistry
     * @param sourceInfoSchema
     * @param sanitizeFieldNames
     */
    public YugabyteDBTableSchemaBuilder(ValueConverterProvider valueConverterProvider, SchemaNameAdjuster schemaNameAdjuster,
                                        CustomConverterRegistry customConverterRegistry, Schema sourceInfoSchema,
                                        boolean sanitizeFieldNames) {
        super(valueConverterProvider, schemaNameAdjuster, customConverterRegistry, sourceInfoSchema, sanitizeFieldNames);
        this.schemaNameAdjuster = schemaNameAdjuster;
        this.sourceInfoSchema = sourceInfoSchema;
    }

    public TableSchema create(String schemaPrefix, String envelopSchemaName, Table table, Tables.ColumnNameFilter filter, ColumnMappers mappers,
                              Key.KeyMapper keysMapper) {
        if (schemaPrefix == null) {
            schemaPrefix = "";
        }

        // Build the schemas ...
        final TableId tableId = table.id();
        final String tableIdStr = tableSchemaName(tableId);
        final String schemaNamePrefix = schemaPrefix + tableIdStr;
        LOGGER.info("Mapping table '{}' to schemas under '{}'", tableId, schemaNamePrefix);
        SchemaBuilder valSchemaBuilder = SchemaBuilder.struct().name(schemaNameAdjuster.adjust(schemaNamePrefix + ".Value"));
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct().name(schemaNameAdjuster.adjust(schemaNamePrefix + ".Key"));
        AtomicBoolean hasPrimaryKey = new AtomicBoolean(false);

        Key tableKey = new Key.Builder(table).customKeyMapper(keysMapper).build();
        tableKey.keyColumns().forEach(column -> {
            addField(keySchemaBuilder, table, column, null);
            hasPrimaryKey.set(true);
        });

        table.columns()
                .stream()
                .filter(column -> filter == null || filter.matches(tableId.catalog(), tableId.schema(), tableId.table(), column.name()))
                .forEach(column -> {
                    ColumnMapper mapper = mappers == null ? null : mappers.mapperFor(tableId, column);
                    addField(valSchemaBuilder, table, column, mapper);
                });

        Schema valSchema = valSchemaBuilder.optional().build();
        Schema keySchema = hasPrimaryKey.get() ? keySchemaBuilder.build() : null;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Mapped primary key for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(keySchema));
            LOGGER.debug("Mapped columns for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(valSchema));
        }

        Envelope envelope = Envelope.defineSchema()
                .withName(schemaNameAdjuster.adjust(envelopSchemaName))
                .withRecord(valSchema)
                .withSource(sourceInfoSchema)
                .build();

        // Create the generators ...
        StructGenerator keyGenerator = createKeyGenerator(keySchema, tableId, tableKey.keyColumns());
        StructGenerator valueGenerator = createValueGenerator(valSchema, tableId, table.columns(), filter, mappers);

        // And the table schema ...
        return new TableSchema(tableId, keySchema, keyGenerator, envelope, valSchema, valueGenerator);
    }

    private String tableSchemaName(TableId tableId) {
        if (Strings.isNullOrEmpty(tableId.catalog())) {
            if (Strings.isNullOrEmpty(tableId.schema())) {
                return tableId.table();
            }
            else {
                return tableId.schema() + "." + tableId.table();
            }
        }
        else if (Strings.isNullOrEmpty(tableId.schema())) {
            return tableId.catalog() + "." + tableId.table();
        }
        // When both catalog and schema is present then only schema is used
        else {
            return tableId.schema() + "." + tableId.table();
        }
    }
}

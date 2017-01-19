/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.conf.AccumuloTableProperties;
import com.facebook.presto.accumulo.index.IndexLookup;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.AccumuloView;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.AccumuloRange;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_EXISTS;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.io.PrestoBatchWriter.ROW_ID_COLUMN;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is the main access point for the Presto connector to interact with Accumulo.
 * It is responsible for creating tables, dropping tables, retrieving table metadata, and getting the ConnectorSplits from a table.
 */
public class AccumuloClient
{
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private final ZooKeeperMetadataManager metaManager;
    private final Authorizations auths;
    private final AccumuloTableManager tableManager;
    private final Connector connector;
    private final IndexLookup indexLookup;
    private final String username;

    @Inject
    public AccumuloClient(
            Connector connector,
            AccumuloConfig config,
            ZooKeeperMetadataManager metaManager,
            AccumuloTableManager tableManager,
            IndexLookup indexLookup)
            throws AccumuloException, AccumuloSecurityException
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.username = requireNonNull(config, "config is null").getUsername();
        this.metaManager = requireNonNull(metaManager, "metaManager is null");
        this.tableManager = requireNonNull(tableManager, "tableManager is null");
        this.indexLookup = requireNonNull(indexLookup, "indexLookup is null");
        this.auths = connector.securityOperations().getUserAuthorizations(username);
    }

    public AccumuloTable createTable(ConnectorTableMetadata meta)
    {
        Map<String, Object> tableProperties = meta.getProperties();
        String rowIdColumn = getRowIdColumn(meta);

        // Get the list of column handles
        List<AccumuloColumnHandle> columns = getColumnHandles(meta, rowIdColumn);

        // Create the AccumuloTable object
        AccumuloTable table = new AccumuloTable(
                meta.getTable().getSchemaName(),
                meta.getTable().getTableName(),
                columns,
                rowIdColumn,
                AccumuloTableProperties.isExternal(tableProperties),
                AccumuloTableProperties.getSerializerClass(tableProperties),
                AccumuloTableProperties.getScanAuthorizations(tableProperties),
                Optional.of(AccumuloTableProperties.getMetricsStorageClass(tableProperties)),
                AccumuloTableProperties.isTruncateTimestampsEnabled(tableProperties),
                AccumuloTableProperties.getIndexColumns(tableProperties));

        // Validate the DDL is something we can handle
        validateCreateTable(table, meta);

        // First, create the metadata
        metaManager.createTableMetadata(table);

        // Make sure the namespace exists
        tableManager.ensureNamespace(table.getSchema());

        // Create the Accumulo table if it does not exist (for 'external' table)
        if (!tableManager.exists(table.getFullTableName())) {
            tableManager.createAccumuloTable(table.getFullTableName());
        }

        // Set any locality groups on the data table
        setLocalityGroups(tableProperties, table);

        // Create index tables, if appropriate
        createIndexTables(table);

        return table;
    }

    /**
     * Validates the given metadata for a series of conditions to ensure the table is well-formed.
     *
     * @param table AccumuloTable
     * @param meta Presto table metadata
     */
    private void validateCreateTable(AccumuloTable table, ConnectorTableMetadata meta)
    {
        validateColumns(table, meta);
        validateLocalityGroups(meta);
        if (!table.isExternal()) {
            validateInternalTable(table);
        }
    }

    private static void validateColumns(AccumuloTable table, ConnectorTableMetadata meta)
    {
        // Check all the column types, and throw an exception if the types of a map are complex
        // While it is a rare case, this is not supported by the Accumulo connector
        ImmutableSet.Builder<String> columnNameBuilder = ImmutableSet.builder();
        for (ColumnMetadata column : meta.getColumns()) {
            if (Types.isMapType(column.getType())) {
                if (Types.isMapType(Types.getKeyType(column.getType()))
                        || Types.isMapType(Types.getValueType(column.getType()))
                        || Types.isArrayType(Types.getKeyType(column.getType()))
                        || Types.isArrayType(Types.getValueType(column.getType()))) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, "Key/value types of a MAP column must be plain types");
                }
            }
            else if (Types.isRowType(column.getType())) {
                if (column.getType().getTypeParameters().stream().filter(type -> Types.isArrayType(type) || Types.isRowType(type) || Types.isMapType(type)).count() > 0) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, "Fields of a ROW type must be plain types");
                }

                if (table.getParsedIndexColumns().stream().flatMap(index -> index.getColumns().stream()).filter(name -> name.equals(column.getName())).count() > 0) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, "Indexing not supported for ROW types");
                }
            }

            columnNameBuilder.add(column.getName().toLowerCase(Locale.ENGLISH));
        }

        // Validate the columns are distinct
        if (columnNameBuilder.build().size() != table.getColumns().size()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Duplicate column names are not supported");
        }

        Optional<Map<String, Pair<String, String>>> columnMapping = AccumuloTableProperties.getColumnMapping(meta.getProperties());
        if (columnMapping.isPresent()) {
            // Validate there are no duplicates in the column mapping
            long distinctMappings = columnMapping.get().values().stream().distinct().count();
            if (distinctMappings != columnMapping.get().size()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Duplicate column family/qualifier pair detected in column mapping, check the value of " + AccumuloTableProperties.COLUMN_MAPPING);
            }

            // Validate no column is mapped to the reserved entry
            String reservedRowIdColumn = ROW_ID_COLUMN.toString();
            if (columnMapping.get().values().stream()
                    .filter(pair -> pair.getKey().equals(reservedRowIdColumn) && pair.getValue().equals(reservedRowIdColumn))
                    .count() > 0) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, format("Column family/qualifier mapping of %s:%s is reserved", reservedRowIdColumn, reservedRowIdColumn));
            }
        }
        else if (table.isExternal()) {
            // Column mapping is not defined (i.e. use column generation) and table is external
            // But column generation is for internal tables only
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Column generation for external tables is not supported, must specify " + AccumuloTableProperties.COLUMN_MAPPING);
        }
    }

    private static void validateLocalityGroups(ConnectorTableMetadata meta)
    {
        // Validate any configured locality groups
        Optional<Map<String, Set<String>>> groups = AccumuloTableProperties.getLocalityGroups(meta.getProperties());
        if (!groups.isPresent()) {
            return;
        }

        String rowIdColumn = getRowIdColumn(meta);

        // For each locality group
        for (Map.Entry<String, Set<String>> g : groups.get().entrySet()) {
            if (g.getValue().contains(rowIdColumn)) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Row ID column cannot be in a locality group");
            }

            // Validate the specified column names exist in the table definition,
            // incrementing a counter for each matching column
            int matchingColumns = 0;
            for (ColumnMetadata column : meta.getColumns()) {
                if (g.getValue().contains(column.getName().toLowerCase(Locale.ENGLISH))) {
                    ++matchingColumns;

                    // Break out early if all columns are found
                    if (matchingColumns == g.getValue().size()) {
                        break;
                    }
                }
            }

            // If the number of matched columns does not equal the defined size,
            // then a column was specified that does not exist
            // (or there is a duplicate column in the table DDL, which is also an issue but has been checked before in validateColumns).
            if (matchingColumns != g.getValue().size()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Unknown Presto column defined for locality group " + g.getKey());
            }
        }
    }

    private void validateInternalTable(AccumuloTable table)
    {
        if (tableManager.exists(table.getFullTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, "Cannot create internal table when an Accumulo table already exists");
        }

        if (table.isIndexed()) {
            if (tableManager.exists(table.getIndexTableName()) || table.getMetricsStorageInstance(connector).exists(table.getSchemaTableName())) {
                throw new PrestoException(ACCUMULO_TABLE_EXISTS, "Internal table is indexed, but the index table and/or index metrics table(s) already exist");
            }
        }
    }

    /**
     * Gets the row ID based on a table properties or the first column name.
     *
     * @param meta ConnectorTableMetadata
     * @return Lowercase Presto column name mapped to the Accumulo row ID
     */
    private static String getRowIdColumn(ConnectorTableMetadata meta)
    {
        Optional<String> rowIdColumn = AccumuloTableProperties.getRowId(meta.getProperties());
        return rowIdColumn.orElse(meta.getColumns().get(0).getName()).toLowerCase(Locale.ENGLISH);
    }

    private static List<AccumuloColumnHandle> getColumnHandles(ConnectorTableMetadata meta, String rowIdColumn)
    {
        // Get the column mappings from the table property or auto-generate columns if not defined
        Map<String, Pair<String, String>> mapping = AccumuloTableProperties.getColumnMapping(meta.getProperties()).orElse(autoGenerateMapping(meta.getColumns(), AccumuloTableProperties.getLocalityGroups(meta.getProperties())));

        // And now we parse the configured columns and create handles for the metadata manager
        ImmutableList.Builder<AccumuloColumnHandle> cBuilder = ImmutableList.builder();
        for (int ordinal = 0; ordinal < meta.getColumns().size(); ++ordinal) {
            ColumnMetadata cm = meta.getColumns().get(ordinal);

            // Special case if this column is the row ID
            if (cm.getName().equalsIgnoreCase(rowIdColumn)) {
                cBuilder.add(
                        new AccumuloColumnHandle(
                                rowIdColumn,
                                Optional.empty(),
                                Optional.empty(),
                                cm.getType(),
                                ordinal,
                                "Accumulo row ID"));
            }
            else {
                if (!mapping.containsKey(cm.getName())) {
                    throw new InvalidParameterException(format("Misconfigured mapping for presto column %s", cm.getName()));
                }

                // Get the mapping for this column
                Pair<String, String> famqual = mapping.get(cm.getName());
                String comment = format("Accumulo column %s:%s", famqual.getLeft(), famqual.getRight());

                // Create a new AccumuloColumnHandle object
                cBuilder.add(
                        new AccumuloColumnHandle(
                                cm.getName(),
                                Optional.of(famqual.getLeft()),
                                Optional.of(famqual.getRight()),
                                cm.getType(),
                                ordinal,
                                comment));
            }
        }

        return cBuilder.build();
    }

    private void setLocalityGroups(Map<String, Object> tableProperties, AccumuloTable table)
    {
        Optional<Map<String, Set<String>>> groups = AccumuloTableProperties.getLocalityGroups(tableProperties);
        if (!groups.isPresent()) {
            LOG.debug("No locality groups to set");
            return;
        }

        ImmutableMap.Builder<String, Set<Text>> localityGroupsBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Set<String>> g : groups.get().entrySet()) {
            ImmutableSet.Builder<Text> familyBuilder = ImmutableSet.builder();
            // For each configured column for this locality group
            for (String col : g.getValue()) {
                // Locate the column family mapping via the Handle
                // We already validated this earlier, so it'll exist
                AccumuloColumnHandle handle = table.getColumns()
                        .stream()
                        .filter(x -> x.getName().equals(col))
                        .collect(Collectors.toList())
                        .get(0);
                familyBuilder.add(new Text(handle.getFamily().get()));
            }

            localityGroupsBuilder.put(g.getKey(), familyBuilder.build());
        }

        Map<String, Set<Text>> localityGroups = localityGroupsBuilder.build();
        LOG.debug("Setting locality groups: {}", localityGroups);
        tableManager.setLocalityGroups(table.getFullTableName(), localityGroups);
    }

    /**
     * Creates the index tables from the given Accumulo table. No op if
     * {@link AccumuloTable#isIndexed()} is false.
     *
     * @param table Table to create index tables
     */
    private void createIndexTables(AccumuloTable table)
    {
        // Early-out if table is not indexed
        if (!table.isIndexed()) {
            return;
        }

        // Create index table if it does not exist (for 'external' table)
        if (!tableManager.exists(table.getIndexTableName())) {
            tableManager.createAccumuloTable(table.getIndexTableName());
        }

        // Set locality groups on index table
        tableManager.setLocalityGroups(table.getIndexTableName(), Indexer.getLocalityGroups(table));

        // Create the index metrics storage
        table.getMetricsStorageInstance(connector).create(table);
    }

    /**
     * Auto-generates the mapping of Presto column name to Accumulo family/qualifier, respecting the locality groups (if any).
     *
     * @param columns Presto columns for the table
     * @param groups Mapping of locality groups to a set of Presto columns, or null if none
     * @return Column mappings
     */
    private static Map<String, Pair<String, String>> autoGenerateMapping(List<ColumnMetadata> columns, Optional<Map<String, Set<String>>> groups)
    {
        Map<String, Pair<String, String>> mapping = new HashMap<>();
        for (ColumnMetadata column : columns) {
            Optional<String> family = getColumnLocalityGroup(column.getName(), groups);
            mapping.put(column.getName(), Pair.of(family.orElse(column.getName()), column.getName()));
        }
        return mapping;
    }

    /**
     * Searches through the given locality groups to find if this column has a locality group.
     *
     * @param columnName Column name to get the locality group of
     * @param groups Optional locality group configuration
     * @return Optional string containing the name of the locality group, if present
     */
    private static Optional<String> getColumnLocalityGroup(String columnName, Optional<Map<String, Set<String>>> groups)
    {
        if (groups.isPresent()) {
            for (Map.Entry<String, Set<String>> group : groups.get().entrySet()) {
                if (group.getValue().contains(columnName.toLowerCase(Locale.ENGLISH))) {
                    return Optional.of(group.getKey());
                }
            }
        }

        return Optional.empty();
    }

    public void dropTable(AccumuloTable table)
    {
        SchemaTableName tableName = new SchemaTableName(table.getSchema(), table.getTable());

        // Remove the table metadata from Presto
        if (metaManager.getTable(tableName) != null) {
            metaManager.deleteTableMetadata(tableName);
        }

        if (!table.isExternal()) {
            // delete the table and index tables
            String fullTableName = table.getFullTableName();
            if (tableManager.exists(fullTableName)) {
                tableManager.deleteAccumuloTable(fullTableName);
            }

            if (table.isIndexed()) {
                String indexTableName = Indexer.getIndexTableName(tableName);
                if (tableManager.exists(indexTableName)) {
                    tableManager.deleteAccumuloTable(indexTableName);
                }

                table.getMetricsStorageInstance(connector).drop(table);
            }
        }
    }

    public void renameTable(SchemaTableName oldName, SchemaTableName newName)
    {
        if (!oldName.getSchemaName().equals(newName.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Accumulo does not support renaming tables to different namespaces (schemas)");
        }

        AccumuloTable oldTable = getTable(oldName);
        if (oldTable == null) {
            throw new TableNotFoundException(oldName);
        }

        AccumuloTable newTable = new AccumuloTable(
                oldTable.getSchema(),
                newName.getTableName(),
                oldTable.getColumns(),
                oldTable.getRowId(),
                oldTable.isExternal(),
                oldTable.getSerializerClassName(),
                oldTable.getScanAuthorizations(),
                oldTable.getMetricsStorageClass(),
                oldTable.isTruncateTimestamps(),
                oldTable.getIndexColumns());

        // Validate table existence
        if (!tableManager.exists(oldTable.getFullTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, format("Table %s does not exist", oldTable.getFullTableName()));
        }

        if (tableManager.exists(newTable.getFullTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, format("Table %s already exists", newTable.getFullTableName()));
        }

        // Rename index tables (which will also validate table existence)
        renameIndexTables(oldTable, newTable);

        // Rename the Accumulo table
        tableManager.renameAccumuloTable(oldTable.getFullTableName(), newTable.getFullTableName());

        // We'll then create the metadata
        metaManager.deleteTableMetadata(oldTable.getSchemaTableName());
        metaManager.createTableMetadata(newTable);
    }

    /**
     * Renames the index tables (if applicable) for the old table to the new table.
     *
     * @param oldTable Old AccumuloTable
     * @param newTable New AccumuloTable
     */
    private void renameIndexTables(AccumuloTable oldTable, AccumuloTable newTable)
    {
        if (!oldTable.isIndexed()) {
            return;
        }

        if (!tableManager.exists(oldTable.getIndexTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, format("Table %s does not exist", oldTable.getIndexTableName()));
        }

        if (tableManager.exists(newTable.getIndexTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, format("Table %s already exists", newTable.getIndexTableName()));
        }

        // Metric storage instance wouldn't have changed
        MetricsStorage metricsStorage = oldTable.getMetricsStorageInstance(connector);

        if (!metricsStorage.exists(oldTable.getSchemaTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, format("Metrics storage exists for %s", oldTable.getFullTableName()));
        }

        if (metricsStorage.exists(newTable.getSchemaTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, format("Metrics storage exists for %s", newTable.getFullTableName()));
        }

        tableManager.renameAccumuloTable(oldTable.getIndexTableName(), newTable.getIndexTableName());
        metricsStorage.rename(oldTable, newTable);
    }

    public void createView(SchemaTableName viewName, String viewData)
    {
        if (getSchemaNames().contains(viewName.getSchemaName())) {
            if (getViewNames(viewName.getSchemaName()).contains(viewName.getTableName())) {
                throw new PrestoException(ALREADY_EXISTS, "View already exists");
            }

            if (getTableNames(viewName.getSchemaName()).contains(viewName.getTableName())) {
                throw new PrestoException(INVALID_VIEW, "View already exists as data table");
            }
        }

        metaManager.createViewMetadata(new AccumuloView(viewName.getSchemaName(), viewName.getTableName(), viewData));
    }

    public void createOrReplaceView(SchemaTableName viewName, String viewData)
    {
        if (getView(viewName) != null) {
            metaManager.deleteViewMetadata(viewName);
        }

        metaManager.createViewMetadata(new AccumuloView(viewName.getSchemaName(), viewName.getTableName(), viewData));
    }

    public void dropView(SchemaTableName viewName)
    {
        metaManager.deleteViewMetadata(viewName);
    }

    public void renameColumn(AccumuloTable table, String source, String target)
    {
        if (table.getColumns().stream().noneMatch(columnHandle -> columnHandle.getName().equalsIgnoreCase(source))) {
            throw new PrestoException(NOT_FOUND, format("Failed to find source column %s to rename to %s", source, target));
        }

        // Copy existing column list, replacing the old column name with the new
        ImmutableList.Builder<AccumuloColumnHandle> newColumnList = ImmutableList.builder();
        for (AccumuloColumnHandle columnHandle : table.getColumns()) {
            if (columnHandle.getName().equalsIgnoreCase(source)) {
                newColumnList.add(new AccumuloColumnHandle(
                        target,
                        columnHandle.getFamily(),
                        columnHandle.getQualifier(),
                        columnHandle.getType(),
                        columnHandle.getOrdinal(),
                        columnHandle.getComment()));
            }
            else {
                newColumnList.add(columnHandle);
            }
        }

        // Create new table metadata
        AccumuloTable newTable = new AccumuloTable(
                table.getSchema(),
                table.getTable(),
                newColumnList.build(),
                table.getRowId().equalsIgnoreCase(source) ? target : table.getRowId(),
                table.isExternal(),
                table.getSerializerClassName(),
                table.getScanAuthorizations(),
                table.getMetricsStorageClass(),
                table.isTruncateTimestamps(),
                table.getIndexColumns());

        // Replace the table metadata
        metaManager.deleteTableMetadata(new SchemaTableName(table.getSchema(), table.getTable()));
        metaManager.createTableMetadata(newTable);
    }

    public Set<String> getSchemaNames()
    {
        return metaManager.getSchemaNames();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return metaManager.getTableNames(schema);
    }

    public AccumuloTable getTable(SchemaTableName table)
    {
        requireNonNull(table, "schema table name is null");
        return metaManager.getTable(table);
    }

    public Set<String> getViewNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return metaManager.getViewNames(schema);
    }

    public AccumuloView getView(SchemaTableName viewName)
    {
        requireNonNull(viewName, "schema table name is null");
        return metaManager.getView(viewName);
    }

    /**
     * Fetches the TabletSplitMetadata for a query against an Accumulo table.
     * <p>
     * Does a whole bunch of fun stuff! Splitting on row ID ranges, applying secondary indexes, column pruning,
     * all sorts of sweet optimizations. What you have here is an important method.
     *
     * @param session Current session
     * @param table Table metadata
     * @param rowIdDomain Domain for the row ID
     * @param constraints Column constraints for the query
     * @return List of TabletSplitMetadata objects for Presto
     */
    public List<TabletSplitMetadata> getTabletSplits(
            ConnectorSession session,
            AccumuloTable table,
            Optional<Domain> rowIdDomain,
            List<AccumuloColumnConstraint> constraints)
    {
        try {
            String tableName = table.getFullTableName();
            LOG.debug("Getting tablet splits for table %s", tableName);

            // Get the initial Range based on the row ID domain
            Collection<AccumuloRange> rowIdRanges = getRangesFromDomain(rowIdDomain, table.getSerializerInstance());
            List<TabletSplitMetadata> tabletSplits = new ArrayList<>();

            // Use the secondary index, if enabled
            if (AccumuloSessionProperties.isOptimizeIndexEnabled(session)) {
                // Get the scan authorizations to query the index
                Authorizations auths = getScanAuthorizations(session, table);

                // Check the secondary index based on the column constraints
                // If this returns true, return the tablet splits to Presto
                if (indexLookup.applyIndex(session, table, constraints, rowIdRanges, tabletSplits, auths)) {
                    return tabletSplits;
                }
            }

            // If we can't (or shouldn't) use the secondary index, we will just use the Range from the row ID domain

            // Split the ranges on tablet boundaries, if enabled
            Collection<Range> splitRanges;
            if (AccumuloSessionProperties.isOptimizeSplitRangesEnabled(session)) {
                splitRanges = splitByTabletBoundaries(tableName, rowIdRanges.stream().map(AccumuloRange::getRange).collect(Collectors.toList()));
            }
            else {
                // if not enabled, just use the same collection
                splitRanges = rowIdRanges.stream().map(AccumuloRange::getRange).collect(Collectors.toList());
            }

            // Create TabletSplitMetadata objects for each range
            boolean fetchTabletLocations = AccumuloSessionProperties.isOptimizeLocalityEnabled(session);

            LOG.debug("Fetching tablet locations: %s", fetchTabletLocations);

            for (Range range : splitRanges) {
                // If locality is enabled, then fetch tablet location
                if (fetchTabletLocations) {
                    tabletSplits.add(new TabletSplitMetadata(getTabletLocation(tableName, range.getStartKey()), ImmutableList.of(range)));
                }
                else {
                    // else, just use the default location
                    tabletSplits.add(new TabletSplitMetadata(Optional.empty(), ImmutableList.of(range)));
                }
            }

            // Log some fun stuff and return the tablet splits
            LOG.debug("Number of splits for table %s is %d with %d ranges", tableName, tabletSplits.size(), splitRanges.size());
            return tabletSplits;
        }
        catch (Exception e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to get splits from Accumulo", e);
        }
    }

    /**
     * Gets the scan authorizations to use for scanning tables.
     * <p>
     * In order of priority: session username authorizations, then table property, then the default connector auths.
     *
     * @param session Current session
     * @param table Table metadata
     * @return Scan authorizations
     * @throws AccumuloException If a generic Accumulo error occurs
     * @throws AccumuloSecurityException If a security exception occurs
     */
    private Authorizations getScanAuthorizations(ConnectorSession session, AccumuloTable table)
            throws AccumuloException, AccumuloSecurityException
    {
        String sessionScanUser = AccumuloSessionProperties.getScanUsername(session);
        if (sessionScanUser != null) {
            Authorizations scanAuths = connector.securityOperations().getUserAuthorizations(sessionScanUser);
            LOG.debug("Using session scan auths for user %s: %s", sessionScanUser, scanAuths);
            return scanAuths;
        }

        Optional<String> strAuths = table.getScanAuthorizations();
        if (strAuths.isPresent()) {
            Authorizations scanAuths = new Authorizations(Iterables.toArray(COMMA_SPLITTER.split(strAuths.get()), String.class));
            LOG.debug("scan_auths table property set, using: %s", scanAuths);
            return scanAuths;
        }

        LOG.debug("scan_auths table property not set, using connector auths: %s", this.auths);
        return this.auths;
    }

    private Collection<Range> splitByTabletBoundaries(String tableName, Collection<Range> ranges)
            throws org.apache.accumulo.core.client.TableNotFoundException, AccumuloException, AccumuloSecurityException
    {
        ImmutableSet.Builder<Range> rangeBuilder = ImmutableSet.builder();
        for (Range range : ranges) {
            // if start and end key are equivalent, no need to split the range
            if (range.getStartKey() != null && range.getEndKey() != null && range.getStartKey().equals(range.getEndKey())) {
                rangeBuilder.add(range);
            }
            else {
                // Call out to Accumulo to split the range on tablets
                rangeBuilder.addAll(connector.tableOperations().splitRangeByTablets(tableName, range, Integer.MAX_VALUE));
            }
        }
        return rangeBuilder.build();
    }

    /**
     * Gets the TabletServer hostname for where the given key is located in the given table
     *
     * @param table Fully-qualified table name
     * @param key Key to locate
     * @return The tablet location, or DUMMY_LOCATION if an error occurs
     */
    private Optional<String> getTabletLocation(String table, Key key)
    {
        Scanner scanner = null;
        try {
            // Get the Accumulo table ID so we can scan some fun stuff
            String tableId = connector.tableOperations().tableIdMap().get(table);

            // Create our scanner against the metadata table, fetching 'loc' family
            scanner = connector.createScanner("accumulo.metadata", auths);
            scanner.fetchColumnFamily(new Text("loc"));

            // Set the scan range to just this table, from the table ID to the default tablet
            // row, which is the last listed tablet
            Key defaultTabletRow = new Key(tableId + '<');
            Key start = new Key(tableId);
            Key end = defaultTabletRow.followingKey(PartialKey.ROW);
            scanner.setRange(new Range(start, end));

            Optional<String> location = Optional.empty();
            if (key == null) {
                // if the key is null, then it is -inf, so get first tablet location
                Iterator<Entry<Key, Value>> iter = scanner.iterator();
                if (iter.hasNext()) {
                    location = Optional.of(iter.next().getValue().toString());
                }
            }
            else {
                // Else, we will need to scan through the tablet location data and find the location

                // Create some text objects to do comparison for what we are looking for
                Text splitCompareKey = new Text();
                key.getRow(splitCompareKey);
                Text scannedCompareKey = new Text();

                // Scan the table!
                for (Entry<Key, Value> entry : scanner) {
                    // Get the bytes of the key
                    byte[] keyBytes = entry.getKey().getRow().copyBytes();

                    // If the last byte is <, then we have hit the default tablet, so use this location
                    if (keyBytes[keyBytes.length - 1] == '<') {
                        location = Optional.of(entry.getValue().toString());
                        break;
                    }
                    else {
                        // Chop off some magic nonsense
                        scannedCompareKey.set(keyBytes, 3, keyBytes.length - 3);

                        // Compare the keys, moving along the tablets until the location is found
                        if (scannedCompareKey.getLength() > 0) {
                            int compareTo = splitCompareKey.compareTo(scannedCompareKey);
                            if (compareTo <= 0) {
                                location = Optional.of(entry.getValue().toString());
                            }
                            else {
                                // all future tablets will be greater than this key
                                break;
                            }
                        }
                    }
                }
            }

            // If we were unable to find the location for some reason, return the default tablet
            // location
            return location.isPresent() ? location : getDefaultTabletLocation(table);
        }
        catch (Exception e) {
            // Swallow this exception so the query does not fail due to being unable
            // to locate the tablet server for the provided Key.
            // This is purely an optimization, but we will want to log the error.
            LOG.error("Failed to get tablet location, returning dummy location", e);
            return Optional.empty();
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private Optional<String> getDefaultTabletLocation(String fulltable)
    {
        Scanner scanner = null;

        try {
            String tableId = connector.tableOperations().tableIdMap().get(fulltable);

            // Create a scanner over the metadata table, fetching the 'loc' column of the default tablet row
            scanner = connector.createScanner("accumulo.metadata", connector.securityOperations().getUserAuthorizations(username));
            scanner.fetchColumnFamily(new Text("loc"));
            scanner.setRange(new Range(tableId + '<'));

            // scan the entry
            Optional<String> location = Optional.empty();
            for (Entry<Key, Value> entry : scanner) {
                if (location.isPresent()) {
                    throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Scan for default tablet returned more than one entry");
                }

                location = Optional.of(entry.getValue().toString());
            }

            return location;
        }
        catch (Exception e) {
            // Swallow this exception so the query does not fail due to being unable to locate the tablet server for the default tablet.
            // This is purely an optimization, but we will want to log the error.
            LOG.error("Failed to get tablet location, returning dummy location", e);
            return Optional.empty();
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    /**
     * Gets a collection of Accumulo Range objects from the given Presto domain.
     * This maps the column constraints of the given Domain to an Accumulo Range scan.
     *
     * @param domain Domain, can be null (returns (-inf, +inf) Range)
     * @param serializer Instance of an {@link AccumuloRowSerializer}
     * @return A collection of Accumulo Range objects
     * @throws TableNotFoundException If the Accumulo table is not found
     */
    public static Collection<AccumuloRange> getRangesFromDomain(Optional<Domain> domain, AccumuloRowSerializer serializer)
            throws TableNotFoundException
    {
        // if we have no predicate pushdown, use the full range
        if (!domain.isPresent()) {
            return ImmutableSet.of(new AccumuloRange());
        }

        ImmutableSet.Builder<AccumuloRange> rangeBuilder = ImmutableSet.builder();
        // If this domain is from an ANY clause
        if (domain.get().getValues().isAny()) {
            // Get the domain type
            Type t = domain.get().getType();
            if (Types.isArrayType(t)) {
                // If it is an array type, then each value is in the domain is an array
                for (Object arrayBlock : domain.get().getValues().getDiscreteValues().getValues()) {
                    // Get the elements of the array block and add them to the ranges
                    Type elementType = Types.getElementType(t);
                    for (Object o : AccumuloRowSerializer.getArrayFromBlock(elementType, (Block) arrayBlock)) {
                        // Find all locations like this and work on it
                        rangeBuilder.add(new AccumuloRange(serializer.encode(elementType, o)));
                    }
                }
            }
            else {
                // If it is not an array type, then this is just a list of values
                for (Object o : domain.get().getValues().getDiscreteValues().getValues()) {
                    rangeBuilder.add(new AccumuloRange(serializer.encode(t, o)));
                }
            }
        }
        else {
            // This isn't an ANY clause, so convert the Presto Range to an Accumulo Range
            for (com.facebook.presto.spi.predicate.Range range : domain.get().getValues().getRanges().getOrderedRanges()) {
                rangeBuilder.add(getRangeFromPrestoRange(range, serializer));
            }
        }

        return rangeBuilder.build();
    }

    public static AccumuloRange getRangeFromPrestoRange(com.facebook.presto.spi.predicate.Range prestoRange, AccumuloRowSerializer serializer)
            throws TableNotFoundException
    {
        AccumuloRange accumuloRange;
        if (prestoRange.isAll()) {
            accumuloRange = new AccumuloRange();
        }
        else if (prestoRange.isSingleValue()) {
            accumuloRange = new AccumuloRange(serializer.encode(prestoRange.getType(), prestoRange.getSingleValue()));
        }
        else {
            if (prestoRange.getLow().isLowerUnbounded()) {
                // If low is unbounded, then create a range from (-inf, value), checking inclusivity
                boolean inclusive = prestoRange.getHigh().getBound() == Bound.EXACTLY;
                byte[] split = serializer.encode(prestoRange.getType(), prestoRange.getHigh().getValue());
                accumuloRange = new AccumuloRange(null, false, split, inclusive);
            }
            else if (prestoRange.getHigh().isUpperUnbounded()) {
                // If high is unbounded, then create a range from (value, +inf), checking inclusivity
                boolean inclusive = prestoRange.getLow().getBound() == Bound.EXACTLY;
                byte[] split = serializer.encode(prestoRange.getType(), prestoRange.getLow().getValue());
                accumuloRange = new AccumuloRange(split, inclusive, null, false);
            }
            else {
                // If high is unbounded, then create a range from low to high, checking inclusivity
                boolean startKeyInclusive = prestoRange.getLow().getBound() == Bound.EXACTLY;
                byte[] startSplit = serializer.encode(prestoRange.getType(), prestoRange.getLow().getValue());

                boolean endKeyInclusive = prestoRange.getHigh().getBound() == Bound.EXACTLY;
                byte[] endSplit = serializer.encode(prestoRange.getType(), prestoRange.getHigh().getValue());
                accumuloRange = new AccumuloRange(startSplit, startKeyInclusive, endSplit, endKeyInclusive);
            }
        }

        return accumuloRange;
    }
}

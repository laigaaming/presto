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
package com.facebook.presto.accumulo.index;

import com.facebook.presto.accumulo.AccumuloClient;
import com.facebook.presto.accumulo.AccumuloQueryRunner;
import com.facebook.presto.accumulo.AccumuloTableManager;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.conf.AccumuloTableProperties;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.accumulo.model.RowSchema;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingNodeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slices;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.io.PrestoBatchWriter.toMutation;
import static com.facebook.presto.accumulo.metadata.AccumuloTable.getFullTableName;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TestColumnCardinalityCache
{
    private static final AccumuloConfig CONFIG = new AccumuloConfig();
    private static final String SCHEMA = "schema";
    private static final String TABLE = TpchTable.LINE_ITEM.getTableName();
    private static final List<IndexQueryParameters> EMPTY_INDEX_QUERY_PARAMETERS = ImmutableList.of();
    private static final Authorizations AUTHS = new Authorizations();
    private static final long EARLY_RETURN_THRESHOLD = 1000;
    private static final Duration POLLING_DURATION = new Duration(1, TimeUnit.SECONDS);
    private static final AccumuloRowSerializer SERIALIZER = new LexicoderRowSerializer();
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date();
    private static final ConnectorSession SESSION = new TestingConnectorSession(new AccumuloSessionProperties().getSessionProperties());

    private AccumuloClient client;
    private Connector connector;
    private MetricsStorage storage;

    @BeforeClass
    public void setup()
            throws Exception
    {
        CONFIG.setUsername("root");
        CONFIG.setPassword("secret");

        connector = AccumuloQueryRunner.getAccumuloConnector();
        CONFIG.setZooKeepers(connector.getInstance().getZooKeepers());

        storage = MetricsStorage.getDefault(connector);

        client = new AccumuloClient(connector, CONFIG, new ZooKeeperMetadataManager(CONFIG, new TypeRegistry()), new AccumuloTableManager(connector), new IndexLookup(connector, CONFIG, new TestingNodeManager()));
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));
        writeTestData();
    }

    private RowSchema fromColumns(List<TpchColumn<LineItem>> columns)
    {
        RowSchema schema = new RowSchema();
        schema.addColumn("UUID", Optional.empty(), Optional.empty(), VARCHAR);
        for (TpchColumn<?> column : columns) {
            schema.addColumn(column.getColumnName(), Optional.of(column.getColumnName()), Optional.of(column.getColumnName()), getPrestoType(column.getType()));
        }
        return schema;
    }

    private List<AccumuloColumnHandle> fromTpchColumns(List<TpchColumn<LineItem>> columns)
    {
        int ordinal = 0;
        ImmutableList.Builder<AccumuloColumnHandle> builder = ImmutableList.builder();
        builder.add(new AccumuloColumnHandle("UUID", Optional.empty(), Optional.empty(), VARCHAR, ordinal++, ""));
        for (TpchColumn<?> column : columns) {
            builder.add(new AccumuloColumnHandle(column.getColumnName(), Optional.of(column.getColumnName()), Optional.of(column.getColumnName()), getPrestoType(column.getType()), ordinal++, ""));
        }
        return builder.build();
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, TpchTable<LineItem> tpchTable)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (TpchColumn<?> column : tpchTable.getColumns()) {
            columns.add(new ColumnMetadata(column.getColumnName(), getPrestoType(column.getType())));
        }

        String indexColumns = StringUtils.join(tpchTable.getColumns().stream().filter(x -> !x.getColumnName().equals(tpchTable.getColumns().get(0).getColumnName())).map(TpchColumn::getColumnName).collect(Collectors.toList()), ",");
        Map<String, Object> properties = new HashMap<>();
        new AccumuloTableProperties().getTableProperties().forEach(meta -> properties.put(meta.getName(), meta.getDefaultValue()));
        properties.put("index_columns", indexColumns);
        SchemaTableName tableName = new SchemaTableName(schemaName, tpchTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns.build(), properties);
    }

    private void writeTestData()
            throws Exception
    {
        AccumuloTable table = client.createTable(getTableMetadata(SCHEMA, TpchTable.LINE_ITEM));
        RowSchema schema = fromColumns(TpchTable.LINE_ITEM.getColumns());

        List<AccumuloColumnHandle> columns = fromTpchColumns(TpchTable.LINE_ITEM.getColumns());
        AccumuloRowSerializer serializer = new LexicoderRowSerializer();
        String tableName = getFullTableName(SCHEMA, TpchTable.LINE_ITEM.getTableName());
        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter writer = multiTableBatchWriter.getBatchWriter(tableName);
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter);

        for (LineItem item : TpchTable.LINE_ITEM.createGenerator(.01f, 1, 1)) {
            String line = item.toLine();
            line = UUID.randomUUID() + "|" + item.toLine().substring(0, line.length() - 1);
            Row row = Row.fromString(schema, line, '|');
            Mutation data = toMutation(row, 0, columns, serializer);
            writer.addMutation(data);
            indexer.index(data);
        }

        metricsWriter.close();
        multiTableBatchWriter.close();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullDuration()
            throws Exception
    {
        new ColumnCardinalityCache(0, null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullSchema()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        cache.getCardinalities(SESSION, null, TABLE, EMPTY_INDEX_QUERY_PARAMETERS, AUTHS, EARLY_RETURN_THRESHOLD, storage);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullTable()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        cache.getCardinalities(SESSION, SCHEMA, null, EMPTY_INDEX_QUERY_PARAMETERS, AUTHS, EARLY_RETURN_THRESHOLD, storage);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullConstraints()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        cache.getCardinalities(SESSION, SCHEMA, TABLE, null, AUTHS, EARLY_RETURN_THRESHOLD, storage);
    }

    @Test
    public void testEmptyConstraints()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, EMPTY_INDEX_QUERY_PARAMETERS, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullAuths()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        cache.getCardinalities(SESSION, SCHEMA, TABLE, EMPTY_INDEX_QUERY_PARAMETERS, null, EARLY_RETURN_THRESHOLD, storage);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullStorage()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        cache.getCardinalities(SESSION, SCHEMA, TABLE, EMPTY_INDEX_QUERY_PARAMETERS, AUTHS, EARLY_RETURN_THRESHOLD, null);
    }

    @Test
    public void testExactRange()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range range = Range.equal(DATE, tld("1998-01-01"));
        List<IndexQueryParameters> queryParameters = ImmutableList.of(iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(range), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 1);

        Collection<IndexQueryParameters> card = cardinalities.get(29L);
        assertNotNull(card);
        assertEquals(card.size(), 1);
        assertEquals(queryParameters.get(0), card.iterator().next());
    }

    @Test
    public void testMultipleExactRanges()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range range1 = Range.equal(DATE, tld("1998-01-01"));
        Range range2 = Range.equal(DATE, tld("1998-01-02"));
        List<IndexQueryParameters> queryParameters = ImmutableList.of(iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(range1, range2), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 1);

        Collection<IndexQueryParameters> card = cardinalities.get(50L);
        assertNotNull(card);
        assertEquals(card.size(), 1);
        assertEquals(queryParameters.get(0), card.iterator().next());
    }

    @Test
    public void testRange()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range range = Range.range(DATE, tld("1998-01-01"), false, tld("1998-01-03"), false);
        List<IndexQueryParameters> queryParameters = ImmutableList.of(iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(range), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 1);

        Collection<IndexQueryParameters> card = cardinalities.get(21L);
        assertNotNull(card);
        assertEquals(card.size(), 1);
        assertEquals(queryParameters.get(0), card.iterator().next());
    }

    @Test
    public void testMultipleRanges()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range range1 = Range.range(DATE, tld("1998-01-01"), false, tld("1998-01-03"), false);
        Range range2 = Range.range(DATE, tld("1998-01-10"), true, tld("1998-01-13"), true);
        List<IndexQueryParameters> queryParameters = ImmutableList.of(iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(range1, range2), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 1);

        Collection<IndexQueryParameters> card = cardinalities.get(127L);
        assertNotNull(card);
        assertEquals(card.size(), 1);
        assertEquals(queryParameters.get(0), card.iterator().next());
    }

    @Test
    public void testManyColumnExactRange()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range rdRange = Range.equal(DATE, tld("1998-01-01"));
        Range lnRange = Range.equal(INTEGER, 7L);
        List<IndexQueryParameters> queryParameters =
                ImmutableList.of(
                        iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(rdRange), false)),
                        iqp("linenumber_linenumber", Domain.create(ValueSet.ofRanges(lnRange), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 2);

        Iterator<Entry<Long, Collection<IndexQueryParameters>>> iterator = cardinalities.asMap().entrySet().iterator();
        Entry<Long, Collection<IndexQueryParameters>> card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 29L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(0), card.getValue().iterator().next());

        card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 2173L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(1), card.getValue().iterator().next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testManyColumnMultipleExactRanges()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range rdRange1 = Range.equal(DATE, tld("1998-01-01"));
        Range rdRange2 = Range.equal(DATE, tld("1998-01-02"));
        Range lnRange1 = Range.equal(INTEGER, 7L);
        Range lnRange2 = Range.equal(INTEGER, 6L);

        List<IndexQueryParameters> queryParameters =
                ImmutableList.of(
                        iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(rdRange1, rdRange2), false)),
                        iqp("linenumber_linenumber", Domain.create(ValueSet.ofRanges(lnRange1, lnRange2), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 2);

        Iterator<Entry<Long, Collection<IndexQueryParameters>>> iterator = cardinalities.asMap().entrySet().iterator();
        Entry<Long, Collection<IndexQueryParameters>> card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 50L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(0), card.getValue().iterator().next());

        card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 6493L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(1), card.getValue().iterator().next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testManyColumnRange()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range rdRange = Range.range(DATE, tld("1998-01-01"), false, tld("1998-01-03"), false);
        Range lnRange = Range.range(INTEGER, 6L, false, 8L, false);

        List<IndexQueryParameters> queryParameters =
                ImmutableList.of(
                        iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(rdRange), false)),
                        iqp("linenumber_linenumber", Domain.create(ValueSet.ofRanges(lnRange), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 2);

        Iterator<Entry<Long, Collection<IndexQueryParameters>>> iterator = cardinalities.asMap().entrySet().iterator();
        Entry<Long, Collection<IndexQueryParameters>> card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 21L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(0), card.getValue().iterator().next());

        card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 2173L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(1), card.getValue().iterator().next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testManyColumnMultipleRanges()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range rdRange1 = Range.range(DATE, tld("1998-01-01"), false, tld("1998-01-03"), false);
        Range rdRange2 = Range.range(DATE, tld("1998-01-10"), true, tld("1998-01-13"), true);
        Range lnRange1 = Range.range(INTEGER, 6L, true, 8L, false);
        Range lnRange2 = Range.range(INTEGER, 4L, false, 5L, true);

        List<IndexQueryParameters> queryParameters =
                ImmutableList.of(
                        iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(rdRange1, rdRange2), false)),
                        iqp("linenumber_linenumber", Domain.create(ValueSet.ofRanges(lnRange1, lnRange2), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 2);

        Iterator<Entry<Long, Collection<IndexQueryParameters>>> iterator = cardinalities.asMap().entrySet().iterator();
        Entry<Long, Collection<IndexQueryParameters>> card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 127);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(0), card.getValue().iterator().next());

        card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 12930L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(1), card.getValue().iterator().next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testManyColumnMixedRanges()
            throws Exception
    {
        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range rdRange = Range.range(DATE, tld("1970-01-01"), true, tld("2016-01-01"), true);
        Range lnRange = Range.equal(INTEGER, 7L);

        List<IndexQueryParameters> queryParameters =
                ImmutableList.of(
                        iqp("receiptdate_receiptdate", Domain.create(ValueSet.ofRanges(rdRange), false)),
                        iqp("linenumber_linenumber", Domain.create(ValueSet.ofRanges(lnRange), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, SCHEMA, TABLE, queryParameters, AUTHS, EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 2);

        Iterator<Entry<Long, Collection<IndexQueryParameters>>> iterator = cardinalities.asMap().entrySet().iterator();
        Entry<Long, Collection<IndexQueryParameters>> card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 2173L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(1), card.getValue().iterator().next());

        card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 60169L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(0), card.getValue().iterator().next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSameRowMultipleVisibilities()
            throws Exception
    {
        List<ColumnMetadata> columns = ImmutableList.of(
                new ColumnMetadata("a", VARCHAR),
                new ColumnMetadata("b", VARCHAR),
                new ColumnMetadata("c", VARCHAR)
        );
        Map<String, Object> properties = new HashMap<>();
        new AccumuloTableProperties().getTableProperties().forEach(meta -> properties.put(meta.getName(), meta.getDefaultValue()));
        properties.put("index_columns", "b,c");

        SchemaTableName tableName = new SchemaTableName("default", "samerowmultiplevisibilities");
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(tableName, columns, properties);

        AccumuloTable table = client.createTable(metadata);

        MultiTableBatchWriter multiTableBatchWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter writer = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter);

        Mutation m = new Mutation("1");
        m.put("___ROW___", "___ROW___", "1");
        m.put("b", "b", "2");
        m.put("c", "c", "3");
        writer.addMutation(m);
        indexer.index(m);

        m = new Mutation("2");
        m.put("___ROW___", "___ROW___", new ColumnVisibility("private"), "2");
        m.put("b", "b", new ColumnVisibility("private"), "2");
        m.put("c", "c", new ColumnVisibility("private"), "3");
        writer.addMutation(m);
        indexer.index(m);
        metricsWriter.close();
        multiTableBatchWriter.close();

        ColumnCardinalityCache cache = new ColumnCardinalityCache(CONFIG.getCardinalityCacheSize(), CONFIG.getCardinalityCacheExpiration());
        Range range = Range.equal(VARCHAR, Slices.copiedBuffer("2", UTF_8));

        List<IndexQueryParameters> queryParameters = ImmutableList.of(iqp("b_b", Domain.create(ValueSet.ofRanges(range), false)));

        Multimap<Long, IndexQueryParameters> cardinalities = cache.getCardinalities(SESSION, tableName.getSchemaName(), tableName.getTableName(), queryParameters, new Authorizations("private"), EARLY_RETURN_THRESHOLD, storage);
        assertEquals(cardinalities.size(), 1);

        Iterator<Entry<Long, Collection<IndexQueryParameters>>> iterator = cardinalities.asMap().entrySet().iterator();
        Entry<Long, Collection<IndexQueryParameters>> card = iterator.next();
        assertNotNull(card);
        assertEquals(card.getKey().longValue(), 2L);
        assertEquals(card.getValue().size(), 1);
        assertEquals(queryParameters.get(0), card.getValue().iterator().next());
        assertFalse(iterator.hasNext());
    }

    /**
     * Converts the given date string to number of days as a long
     *
     * @param date Date string
     * @return Number of days as long
     */
    private static long tld(String date)
    {
        return TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseDateTime(date).getMillis());
    }

    /**
     * Creates an AccumuloColumnConstraint from the given Presto column name and domain
     *
     * @param name Presto column name
     * @param domain Presto Domain
     * @return Constraint
     */
    private static IndexQueryParameters iqp(String name, Domain domain)
    {
        IndexQueryParameters parameters = new IndexQueryParameters(new IndexColumn(ImmutableList.of(name)));
        parameters.appendColumn(name.getBytes(UTF_8), AccumuloClient.getRangesFromDomain(Optional.of(domain), SERIALIZER), false);
        return parameters;
    }
}

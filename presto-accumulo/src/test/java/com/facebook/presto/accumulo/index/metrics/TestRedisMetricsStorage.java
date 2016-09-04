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
package com.facebook.presto.accumulo.index.metrics;

import com.facebook.presto.accumulo.AccumuloQueryRunner;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.util.Optional;

import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRedisMetricsStorage
        extends TestAbstractMetricStorage
{
    private RedisServer server;
    private Jedis jedis;
    private int port;

    @Override
    public MetricsStorage getMetricsStorage(AccumuloConfig config)
    {
        config.setRedisPort(port);
        return new RedisMetricsStorage(AccumuloQueryRunner.getAccumuloConnector(), config);
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = new RedisServer();
        server.start();
        port = server.getPort();
        jedis = new Jedis("localhost", port);
    }

    @AfterClass
    public void cleanupClass()
            throws Exception
    {
        jedis.close();
        server.stop();
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        jedis.flushAll();
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_create");
        storage.create(table);
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_create"));
    }

    @Test
    public void testCreateTableAlreadyExists()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_create_already_exists");
        jedis.sadd("pacc::tables", "default.test_redis_metric_storage_create_already_exists");
        storage.create(table);
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_create_already_exists"));
    }

    @Test
    public void testDropTable()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_drop_table");
        storage.create(table);
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_table"));
        storage.drop(table);
        assertFalse(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_table"));
    }

    @Test
    public void testDropTableDoesNotExist()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_drop_table_does_not_exist");
        storage.drop(table);
        assertFalse(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_table_does_not_exist"));
    }

    @Test
    public void testDropTableAllKeys()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_drop_table_all_keys");
        table2.setTable("test_redis_metric_storage_drop_table_all_keys2");

        // Create first table, write data
        storage.create(table);
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_table_all_keys"));
        MetricsWriter writer = storage.newWriter(table);
        writer.incrementRowCount();
        writer.incrementCardinality(wrap("foo".getBytes(UTF_8)), wrap("bar".getBytes(UTF_8)), new ColumnVisibility(), false);
        writer.flush();

        // Create second table, write data
        storage.create(table2);
        writer = storage.newWriter(table2);
        writer.incrementRowCount();
        writer.incrementCardinality(wrap("foo".getBytes(UTF_8)), wrap("bar".getBytes(UTF_8)), new ColumnVisibility(), false);
        writer.flush();

        // Drop table and validate all keys for it are gone, but the others exist
        storage.drop(table);
        assertFalse(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_table_all_keys"));
        assertEquals(jedis.keys("pacc::*::default::test_redis_metric_storage_drop_table_all_keys::*").size(), 0);

        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_table_all_keys2"));
        assertEquals(jedis.keys("pacc::*::default::test_redis_metric_storage_drop_table_all_keys2::*").size(), 4);
    }

    @Test
    public void testDropExternalTable()
            throws Exception
    {
        externalTable.setTable("test_redis_metric_storage_drop_external_table");

        // Create first table, write data
        storage.create(externalTable);
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_external_table"));
        MetricsWriter writer = storage.newWriter(externalTable);
        writer.incrementRowCount();
        writer.incrementCardinality(wrap("foo".getBytes(UTF_8)), wrap("bar".getBytes(UTF_8)), new ColumnVisibility(), false);
        writer.flush();

        storage.drop(externalTable);
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_drop_external_table"));
        assertEquals(jedis.keys("pacc::*::default::test_redis_metric_storage_drop_external_table::*").size(), 4);
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_rename_table");
        table2.setTable("test_redis_metric_storage_rename_table2");

        storage.create(table);
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_rename_table"));
        storage.rename(table, table2);
        assertFalse(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_rename_table"));
        assertTrue(jedis.sismember("pacc::tables", "default.test_redis_metric_storage_rename_table2"));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testRenameTableDoesNotExist()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_rename_table_does_not_exist");
        table2.setTable("test_redis_metric_storage_rename_table_does_not_exist2");
        storage.rename(table, table2);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testRenameTableNewTableExists()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_rename_new_table_exists");
        table2.setTable("test_redis_metric_storage_rename_new_table_exists2");
        storage.create(table);
        jedis.sadd("pacc::tables", "default.test_redis_metric_storage_rename_new_table_exists2");
        storage.rename(table, table2);
    }

    @Test
    public void testExists()
            throws Exception
    {
        table.setTable("test_redis_metric_storage_exists");
        assertFalse(storage.exists(table.getSchemaTableName()));
        storage.create(table);
        assertTrue(storage.exists(table.getSchemaTableName()));
    }

    @Override
    public void testTimestampInserts()
            throws Exception
    {
        AccumuloTable table = new AccumuloTable(
                "default",
                "test_redis_timestamp_inserts",
                ImmutableList.of(c1, new AccumuloColumnHandle("time", Optional.of("cf"), Optional.of("time"), TIMESTAMP_TYPE.get(), 1, "", true)),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                null,
                Optional.of(storage.getClass().getCanonicalName()),
                true);

        storage.create(table);

        assertTrue(storage.exists(table.getSchemaTableName()));

        MetricsWriter writer = storage.newWriter(table);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(getJedisHashValue("pacc::tscardshs::default::test_redis_timestamp_inserts::cf_time::", SECOND_TIMESTAMP), 1L);
        assertEquals(getJedisHashValue("pacc::tscardmhs::default::test_redis_timestamp_inserts::cf_time::", MINUTE_TIMESTAMP), 1L);
        assertEquals(getJedisHashValue("pacc::tscardhhs::default::test_redis_timestamp_inserts::cf_time::", HOUR_TIMESTAMP), 1L);
        assertEquals(getJedisHashValue("pacc::tscarddhs::default::test_redis_timestamp_inserts::cf_time::", DAY_TIMESTAMP), 1L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(getJedisHashValue("pacc::tscardshs::default::test_redis_timestamp_inserts::cf_time::", SECOND_TIMESTAMP), 2L);
        assertEquals(getJedisHashValue("pacc::tscardmhs::default::test_redis_timestamp_inserts::cf_time::", MINUTE_TIMESTAMP), 2L);
        assertEquals(getJedisHashValue("pacc::tscardhhs::default::test_redis_timestamp_inserts::cf_time::", HOUR_TIMESTAMP), 2L);
        assertEquals(getJedisHashValue("pacc::tscarddhs::default::test_redis_timestamp_inserts::cf_time::", DAY_TIMESTAMP), 2L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.flush();

        assertEquals(getJedisHashValue("pacc::tscardshs::default::test_redis_timestamp_inserts::cf_time::", SECOND_TIMESTAMP), 3L);
        assertEquals(getJedisHashValue("pacc::tscardmhs::default::test_redis_timestamp_inserts::cf_time::", MINUTE_TIMESTAMP), 3L);
        assertEquals(getJedisHashValue("pacc::tscardhhs::default::test_redis_timestamp_inserts::cf_time::", HOUR_TIMESTAMP), 3L);
        assertEquals(getJedisHashValue("pacc::tscarddhs::default::test_redis_timestamp_inserts::cf_time::", DAY_TIMESTAMP), 3L);

        writer.incrementCardinality(bb(TIMESTAMP), bb("cf_time"), new ColumnVisibility(), true);
        writer.close();

        assertEquals(getJedisHashValue("pacc::tscardshs::default::test_redis_timestamp_inserts::cf_time::", SECOND_TIMESTAMP), 4L);
        assertEquals(getJedisHashValue("pacc::tscardmhs::default::test_redis_timestamp_inserts::cf_time::", MINUTE_TIMESTAMP), 4L);
        assertEquals(getJedisHashValue("pacc::tscardhhs::default::test_redis_timestamp_inserts::cf_time::", HOUR_TIMESTAMP), 4L);
        assertEquals(getJedisHashValue("pacc::tscarddhs::default::test_redis_timestamp_inserts::cf_time::", DAY_TIMESTAMP), 4L);
    }

    private long getJedisHashValue(String key, Long field)
    {
        return Long.parseLong(new String(jedis.hget(b(key), serializer.encode(TIMESTAMP_TYPE.get(), field)), UTF_8));
    }
}

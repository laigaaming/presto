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

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Bytes;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RedisMetricsStorage
        extends MetricsStorage
{
    public static final String TABLE_KEY_SET = "pacc::tables";
    private static final Logger READER_LOG = Logger.get(RedisMetricsReader.class);
    private static final byte[] NEG_INFINITY = "-".getBytes(UTF_8);
    private static final byte[] INFINITY = "+".getBytes(UTF_8);
    private static final byte[] INCLUSIVE = "[".getBytes(UTF_8);
    private static final byte[] EXCLUSIVE = "(".getBytes(UTF_8);

    private final Optional<JedisPool> pool;
    private final Optional<JedisSentinelPool> sentinelPool;
    private static final AtomicBoolean scriptsLoaded = new AtomicBoolean(false);
    private static byte[] getExactCardinalitySha1;
    private static byte[] getRangeCardinalitySha1;

    public RedisMetricsStorage(Connector connector, AccumuloConfig config)
    {
        super(connector, config);

        if (config.getRedisSentinelMaster().isPresent()) {
            JedisSentinelPool tempPool = new JedisSentinelPool(config.getRedisSentinelMaster().get(), ImmutableSet.copyOf(config.getRedisSentinels()));
            Runtime.getRuntime().addShutdownHook(new Thread(tempPool::destroy));
            this.sentinelPool = Optional.of(tempPool);
            this.pool = Optional.empty();
        }
        else {
            JedisPool tempPool = new JedisPool(config.getRedisHost(), config.getRedisPort());
            Runtime.getRuntime().addShutdownHook(new Thread(tempPool::destroy));
            this.sentinelPool = Optional.empty();
            this.pool = Optional.of(tempPool);
        }

        synchronized (scriptsLoaded) {
            if (!scriptsLoaded.get()) {
                getExactCardinalitySha1 = loadScriptFromResource("/get_exact_cardinality.lua");
                getRangeCardinalitySha1 = loadScriptFromResource("/get_range_cardinality.lua");
                scriptsLoaded.set(true);
            }
        }
    }

    /**
     * Loads a Lua script into Redis via SCRIPT LOAD and returns the SHA1 received from the server
     *
     * @param path Path of classpath resource
     * @return SHA1 hash to be used in EVALSHA
     */
    private byte[] loadScriptFromResource(String path)
    {
        try {
            byte[] bytes = IOUtils.toByteArray(RedisMetricsStorage.class.getResourceAsStream(path));
            try (BinaryJedis jedis = getJedis()) {
                return jedis.scriptLoad(bytes);
            }
        }
        catch (NullPointerException | IOException e) {
            throw new PrestoException(INTERNAL_ERROR, "Failed to get resource " + path);
        }
    }

    @Override
    public void create(AccumuloTable table)
    {
        try (Jedis jedis = getJedis()) {
            jedis.sadd(TABLE_KEY_SET, getTableValue(table.getSchema(), table.getTable()));
        }
    }

    @Override
    public void rename(AccumuloTable oldTable, AccumuloTable newTable)
    {
        if (!exists(oldTable.getSchemaTableName())) {
            throw new PrestoException(VALIDATION, format("Rename failed, no redis member for %s", oldTable.getSchemaTableName()));
        }

        if (exists(newTable.getSchemaTableName())) {
            throw new PrestoException(VALIDATION, format("Rename failed, redis member for %s already exists", newTable.getSchemaTableName()));
        }

        try (Jedis jedis = getJedis()) {
            Transaction transaction = jedis.multi();
            transaction.srem(TABLE_KEY_SET, getTableValue(oldTable.getSchema(), oldTable.getTable()));
            transaction.sadd(TABLE_KEY_SET, getTableValue(newTable.getSchema(), newTable.getTable()));
            transaction.exec();
        }
    }

    @Override
    public boolean exists(SchemaTableName table)
    {
        try (Jedis jedis = getJedis()) {
            return jedis.sismember(TABLE_KEY_SET, getTableValue(table.getSchemaName(), table.getTableName()));
        }
    }

    @Override
    public void drop(AccumuloTable table)
    {
        if (table.isExternal()) {
            return;
        }

        try (Jedis jedis = getJedis()) {
            Pipeline pipeline = jedis.pipelined();
            jedis.keys(format("pacc::*::%s::%s::*", table.getSchema(), table.getTable())).forEach(pipeline::del);
            pipeline.srem(TABLE_KEY_SET, getTableValue(table.getSchema(), table.getTable()));
            pipeline.sync();
        }
    }

    @Override
    public MetricsWriter newWriter(AccumuloTable table)
    {
        return new RedisMetricsWriter(config, table);
    }

    @Override
    public MetricsReader newReader()
    {
        return new RedisMetricsReader(config);
    }

    private Jedis getJedis()
    {
        if (sentinelPool.isPresent()) {
            return sentinelPool.get().getResource();
        }
        else {
            return pool.get().getResource();
        }
    }

    private static String getTableValue(String schema, String table)
    {
        return format("%s.%s", schema, table);
    }

    private static byte[] getSortedSetKey(String schema, String table, byte[] column, byte[] visibility)
    {
        return getSortedSetKey(schema, table, new String(column, UTF_8), new String(visibility, UTF_8));
    }

    private static byte[] getSortedSetKey(String schema, String table, String column, String visibility)
    {
        return format("pacc::cardss::%s::%s::%s::%s", schema, table, column, visibility).getBytes(UTF_8);
    }

    private static byte[] getHashKey(String schema, String table, byte[] column, byte[] visibility)
    {
        return getHashKey(schema, table, new String(column, UTF_8), new String(visibility, UTF_8));
    }

    private static byte[] getHashKey(String schema, String table, String column, String visibility)
    {
        return format("pacc::cardhs::%s::%s::%s::%s", schema, table, column, visibility).getBytes(UTF_8);
    }

    private static byte[] getTimestampHashKey(TimestampPrecision level, String schema, String table, byte[] column, byte[] visibility)
    {
        return format("pacc::tscard%shs::%s::%s::%s::%s", level.getBrief(), schema, table, new String(column, UTF_8), new String(visibility, UTF_8)).getBytes(UTF_8);
    }

    private class RedisMetricsWriter
            extends MetricsWriter
    {
        public RedisMetricsWriter(AccumuloConfig config, AccumuloTable table)
        {
            super(config, table);
        }

        @Override
        public void flush()
        {
            try (Jedis jedis = getJedis()) {
                Pipeline pipeline = jedis.pipelined();
                for (Entry<CardinalityKey, AtomicLong> entry : metrics.entrySet()) {
                    if (entry.getValue().get() > 0) {
                        // Lexicographically encoded values in a sorted set with the same score
                        // Enables range scans

                        // Key name: pacc::cardss::schema::table::family::qualifier::visibility
                        // Type: Sorted set
                        // Member: Lexicographically encoded column value
                        // Score: 0

                        byte[] sortedSetKey = getSortedSetKey(table.getSchema(), table.getTable(), entry.getKey().column.array(), entry.getKey().visibility.getExpression());
                        byte[] sortedSetMember = entry.getKey().value.array();

                        // Hash for storing the metric value

                        // Key name: pacc::cardhs::schema::table::family::qualifier::visibility
                        // Type: Hash
                        // Field: Lexicographically encoded column value
                        // Value: Integer value representing the number of rows in the data table that have this value ID: Column value

                        byte[] hashKey = getHashKey(table.getSchema(), table.getTable(), entry.getKey().column.array(), entry.getKey().visibility.getExpression());
                        byte[] hashField = entry.getKey().value.array();
                        long hashValue = entry.getValue().get();

                        pipeline.zadd(sortedSetKey, 0, sortedSetMember);
                        pipeline.hincrBy(hashKey, hashField, hashValue);
                    }
                }

                for (Entry<TimestampTruncateKey, AtomicLong> entry : timestampMetrics.entrySet()) {
                    if (entry.getValue().get() > 0) {
                        byte[] hashKey = getTimestampHashKey(entry.getKey().level, table.getSchema(), table.getTable(), entry.getKey().column.array(), entry.getKey().visibility.getExpression());
                        byte[] hashField = entry.getKey().value.array();
                        long hashValue = entry.getValue().get();
                        pipeline.hincrBy(hashKey, hashField, hashValue);
                    }
                }

                metrics.clear();
                timestampMetrics.clear();
                pipeline.sync();
            }
        }
    }

    private class RedisMetricsReader
            extends MetricsReader
    {
        public RedisMetricsReader(AccumuloConfig config)
        {
            super(config);
        }

        @Override
        public long getCardinality(MetricCacheKey key)
                throws Exception
        {
            String columnFamily = super.getColumnFamily(key);
            Text tmp = new Text();

            try (BinaryJedis jedis = getJedis()) {
                byte[] auths = key.auths.toString().getBytes(UTF_8);
                Text row = new Text();

                if (isExact(key.range)) {
                    byte[] hashKeyPattern = getHashKey(key.schema, key.table, columnFamily, "*");
                    return (long) jedis.evalsha(getExactCardinalitySha1, 1, hashKeyPattern, auths, key.range.getStartKey().getRow(row).copyBytes());
                }
                else {
                    if (!key.truncateTimestamps) {
                        byte[] min = getMin(key.range, tmp);
                        byte[] max = getMax(key.range, tmp);
                        byte[] sortedSetKeyPattern = getSortedSetKey(key.schema, key.table, columnFamily, "*");
                        return (long) jedis.evalsha(getRangeCardinalitySha1, 1, sortedSetKeyPattern, key.schema.getBytes(UTF_8), key.table.getBytes(UTF_8), columnFamily.getBytes(UTF_8), auths, min, max);
                    }
                    else {
                        // For timestamp columns
                        final AtomicLong sum = new AtomicLong(0);
                        Multimap<TimestampPrecision, Range> splitRanges = MetricsStorage.splitTimestampRange(key.range);
                        for (Entry<TimestampPrecision, Collection<Range>> entry : splitRanges.asMap().entrySet()) {
                            switch (entry.getKey()) {
                                case MILLISECOND:
                                    entry.getValue().forEach(range -> {
                                        byte[] min = getMin(range, tmp);
                                        byte[] max = getMax(range, tmp);
                                        byte[] sortedSetKeyPattern = getSortedSetKey(key.schema, key.table, columnFamily, "*");
                                        sum.addAndGet((long) jedis.evalsha(getRangeCardinalitySha1, 1, sortedSetKeyPattern, key.schema.getBytes(UTF_8), key.table.getBytes(UTF_8), columnFamily.getBytes(UTF_8), auths, min, max));
                                    });
                                    break;
                                case SECOND:
                                case MINUTE:
                                case HOUR:
                                case DAY:
                                    entry.getValue().forEach(range -> {
                                        if (isExact(range)) {
                                            byte[] hashKeyPattern = getTimestampHashKey(entry.getKey(), key.schema, key.table, columnFamily.getBytes(UTF_8), "*".getBytes(UTF_8));
                                            sum.addAndGet((long) jedis.evalsha(getExactCardinalitySha1, 1, hashKeyPattern, auths, range.getStartKey().getRow(row).copyBytes()));
                                        }
                                        else {
                                            byte[] min = getMin(range, tmp);
                                            byte[] max = getMax(range, tmp);
                                            byte[] sortedSetKeyPattern = getSortedSetKey(key.schema, key.table, columnFamily, "*");
                                            sum.addAndGet((long) jedis.evalsha(getRangeCardinalitySha1, 1, sortedSetKeyPattern, key.schema.getBytes(UTF_8), key.table.getBytes(UTF_8), columnFamily.getBytes(UTF_8), auths, min, max));
                                        }
                                    });
                                    break;
                            }
                        }
                        return sum.get();
                    }
                }
            }
        }

        @Override
        public Map<MetricCacheKey, Long> getCardinalities(Collection<MetricCacheKey> keys)
        {
            if (keys.isEmpty()) {
                return ImmutableMap.of();
            }

            READER_LOG.debug("Loading %s exact ranges from Redis", keys.size());

            MetricCacheKey anyKey = super.getAnyKey(keys);
            String columnFamily = super.getColumnFamily(anyKey);
            ImmutableMap.Builder<MetricCacheKey, Long> cardinalities = ImmutableMap.builder();
            Text row = new Text();
            try (Jedis jedis = getJedis()) {
                // For each key in the collection, get the cardinality and add it to the map
                for (MetricCacheKey key : keys) {
                    // Get the exact value of the range
                    key.range.getStartKey().getRow(row);
                    byte[] hashKeyPattern = getHashKey(key.schema, key.table, columnFamily, "*");
                    long sum = (long) jedis.evalsha(getExactCardinalitySha1, 1, hashKeyPattern, key.auths.toString().getBytes(UTF_8), row.copyBytes());
                    cardinalities.put(key, sum);
                }
            }

            return cardinalities.build();
        }

        /**
         * Gets the maximum value of the Range within the key, accounting
         * for an infinite start key
         *
         * @param range Range
         * @param tmp Temporary Text to prevent excessive object creation
         * @return bytes
         */
        private byte[] getMin(Range range, Text tmp)
        {
            if (range.isInfiniteStartKey()) {
                return NEG_INFINITY;
            }
            else {
                byte[] min = range.getStartKey().getRow(tmp).copyBytes();
                if (range.isStartKeyInclusive()) {
                    return Bytes.concat(INCLUSIVE, min);
                }
                else {
                    return Bytes.concat(EXCLUSIVE, min);
                }
            }
        }

        /**
         * Gets the maximum value of the Range within the key, accounting
         * for an infinite end key
         *
         * @param range Range
         * @param tmp Temporary Text to prevent excessive object creation
         * @return bytes
         */
        private byte[] getMax(Range range, Text tmp)
        {
            if (range.isInfiniteStopKey()) {
                return INFINITY;
            }
            else {
                byte[] max = range.getEndKey().getRow(tmp).copyBytes();
                if (range.isEndKeyInclusive()) {
                    return Bytes.concat(INCLUSIVE, max);
                }
                else {
                    return Bytes.concat(EXCLUSIVE, max);
                }
            }
        }

        @Override
        public void close()
                throws Exception
        {
            // noop
        }
    }
}

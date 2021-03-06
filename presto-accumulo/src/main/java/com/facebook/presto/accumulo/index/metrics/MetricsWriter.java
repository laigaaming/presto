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

import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import org.apache.accumulo.core.security.ColumnVisibility;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROWS_COLUMN;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.METRICS_TABLE_ROW_ID;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Abstract class used to write metrics regarding the table index.
 * Implementors should respect column visibility labels for more fine-grained metrics
 */
public abstract class MetricsWriter
        implements AutoCloseable
{
    protected final Map<CardinalityKey, AtomicLong> metrics = new HashMap<>();
    protected final AccumuloTable table;
    protected final LexicoderRowSerializer serializer = new LexicoderRowSerializer();

    private static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();

    /**
     * Creates a new instance of {@link MetricsWriter}
     *
     * @param table Accumulo table metadata for the writer
     */
    public MetricsWriter(AccumuloTable table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    /**
     * Increment the number of rows in the table by one
     */
    public void incrementRowCount()
    {
        incrementCardinality(METRICS_TABLE_ROW_ID, METRICS_TABLE_ROWS_COLUMN, EMPTY_VISIBILITY);
    }

    /**
     * Increment the cardinality of the given value and column, accounting for the visibility of the column
     *
     * @param value Cell's value
     * @param column Column of the row
     * @param visibility Row's visibility
     */
    public void incrementCardinality(ByteBuffer value, ByteBuffer column, ColumnVisibility visibility)
    {
        metrics.computeIfAbsent(new CardinalityKey(value, column, visibility), key -> new AtomicLong(0L)).incrementAndGet();
    }

    /**
     * Decrements the number of rows in the table by one
     */
    public void decrementRowCount()
    {
        decrementCardinality(METRICS_TABLE_ROW_ID, METRICS_TABLE_ROWS_COLUMN, EMPTY_VISIBILITY);
    }

    /**
     * Decrement the cardinality of the given value and column, accounting for the visibility of the column
     *
     * @param value Cell's value
     * @param column Column of the row
     * @param visibility Row's visibility
     */
    public void decrementCardinality(ByteBuffer value, ByteBuffer column, ColumnVisibility visibility)
    {
        metrics.computeIfAbsent(new CardinalityKey(value, column, visibility), key -> new AtomicLong(0L)).decrementAndGet();
    }

    /**
     * Flush all current metrics
     */
    public abstract void flush();

    /**
     * Closes the writer
     */
    @Override
    public void close()
    {
        flush();
    }

    protected static class CardinalityKey
    {
        public ByteBuffer value;
        public ByteBuffer column;
        public ColumnVisibility visibility;
        private static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();

        /**
         * Creates a new instance of {@link CardinalityKey}
         *
         * @param value The value of the cell
         * @param column The column of the row
         * @param visibility The column visibility
         */
        public CardinalityKey(ByteBuffer value, ByteBuffer column, ColumnVisibility visibility)
        {
            this.value = requireNonNull(value, "value is null");
            this.column = requireNonNull(column, "column is null");
            requireNonNull(visibility, "visibility is null");
            this.visibility = visibility.getExpression() != null ? visibility : EMPTY_VISIBILITY;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            CardinalityKey other = (CardinalityKey) obj;
            return Objects.equals(this.value, other.value)
                    && Objects.equals(this.column, other.column)
                    && Objects.equals(this.visibility, other.visibility);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value, column, visibility);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", new String(value.array(), UTF_8))
                    .add("column", new String(column.array(), UTF_8))
                    .add("visibility", visibility.toString())
                    .toString();
        }
    }
}

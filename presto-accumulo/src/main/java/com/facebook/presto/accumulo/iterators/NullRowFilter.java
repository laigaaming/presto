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
package com.facebook.presto.accumulo.iterators;

import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public class NullRowFilter
        extends RowFilter
        implements OptionDescriber
{
    protected static final String FAMILY = "family";
    protected static final String QUALIFIER = "qualifier";

    private Text columnFamily;
    private Text columnQualifier;

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator)
            throws IOException
    {
        while (rowIterator.hasTop()) {
            Key key = rowIterator.getTopKey();
            if (key.compareColumnFamily(columnFamily) == 0 && key.compareColumnQualifier(columnQualifier) == 0) {
                return false;
            }
            rowIterator.next();
        }
        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException
    {
        super.init(source, options, env);
        columnFamily = new Text(options.get(FAMILY));
        columnQualifier = new Text(options.get(QUALIFIER));
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env)
    {
        NullRowFilter copy = new NullRowFilter();
        copy.columnFamily = new Text(this.columnFamily);
        copy.columnQualifier = new Text(this.columnQualifier);
        return copy;
    }

    @Override
    public IteratorOptions describeOptions()
    {
        return new IteratorOptions(
                "singlecolumnvaluefilter",
                "Filter accepts or rejects each Key/Value pair based on the lexicographic comparison of a value stored in a single column family/qualifier",
                ImmutableMap.of(FAMILY, "column family to match on, required", QUALIFIER, "column qualifier to match on, required"),
                null);
    }

    @Override
    public boolean validateOptions(Map<String, String> options)
    {
        checkNotNull(FAMILY, options);
        checkNotNull(QUALIFIER, options);
        return true;
    }

    private void checkNotNull(String opt, Map<String, String> options)
    {
        if (options.get(opt) == null) {
            throw new IllegalArgumentException("Option " + opt + " is required");
        }
    }

    public static Map<String, String> getProperties(String family, String qualifier)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(FAMILY, family);
        opts.put(QUALIFIER, qualifier);
        return opts;
    }

    @Override
    public String toString()
    {
        return format("NullRowFilter{columnFamily=%s,columnQualifier=%s}", columnFamily, columnQualifier);
    }
}

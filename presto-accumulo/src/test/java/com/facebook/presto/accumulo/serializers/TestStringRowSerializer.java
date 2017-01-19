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
package com.facebook.presto.accumulo.serializers;

public class TestStringRowSerializer
        extends AbstractTestAccumuloRowSerializer
{
    public TestStringRowSerializer()
    {
        super(StringRowSerializer.class);
    }

    @Override
    public void testArray()
            throws Exception
    {
        // Arrays are not supported by StringRowSerializer
    }

    @Override
    public void testMap()
            throws Exception
    {
        // Maps are not supported by StringRowSerializer
    }

    @Override
    public void testRow()
            throws Exception
    {
        // Rows are not supported by StringRowSerializer
    }
}

/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.mergetree.compact.aggregate;

/** An aggregator which will set null for retraction messages. */
public class FieldSetNullRetractAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;
    private final FieldAggregator aggregator;

    public FieldSetNullRetractAgg(FieldAggregator aggregator) {
        super(aggregator.fieldType);
        this.aggregator = aggregator;
    }

    @Override
    public String name() {
        return aggregator.name();
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        return aggregator.agg(accumulator, inputField);
    }

    @Override
    public Object aggForOldSequence(Object accumulator, Object inputField) {
        return aggregator.aggForOldSequence(accumulator, inputField);
    }

    @Override
    public void reset() {}

    @Override
    public Object retract(Object accumulator, Object retractField) {
        return null;
    }
}

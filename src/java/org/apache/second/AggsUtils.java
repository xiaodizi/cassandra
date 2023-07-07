/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.second;

import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.StringTermsAggregate;
import org.opensearch.client.opensearch._types.aggregations.StringTermsBucket;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggsUtils {

    /**
     * Terms 聚合结果
     */
    public static Map<String,Object> termsAggs(Aggregate aggs){
        Map<String,Object> maps=new HashMap<>();
        StringTermsAggregate sterms = aggs.sterms();
        List<StringTermsBucket> array = sterms.buckets().array();
        array.stream().forEach(a->{
            maps.put(a.key(),a.docCount());
        });
        return maps;
    }
}

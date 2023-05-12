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

package org.apache.cassandra.audit.es;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CassandraUtil {

    public static Map<String,Boolean> syncTablesInfo=new HashMap<>();

    static {

    }

    public static void getTableParams(String tableName,Boolean syncEs){
        syncTablesInfo.put(tableName,syncEs);
    }

    public static String matchSqlTableName(String sql){
        Matcher matcher = null;
        //SELECT 列名称 FROM 表名称
        //SELECT * FROM 表名称
        if( sql.startsWith("select") ){
            matcher = Pattern.compile("select\\s.+from\\s(.+)where\\s(.*)").matcher(sql);
            if(matcher.find()){
                return matcher.group(1).trim();
            }
        }
        //INSERT INTO 表名称 VALUES (值1, 值2,....)
        //INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
        if( sql.startsWith("insert") ){
            matcher = Pattern.compile("insert\\sinto\\s(.+)\\(.*\\)\\s.*").matcher(sql);
            if(matcher.find()){
                return matcher.group(1).trim();
            }
        }
        //UPDATE 表名称 SET 列名称 = 新值 WHERE 列名称 = 某值
        if( sql.startsWith("update") ){
            matcher = Pattern.compile("update\\s(.+)set\\s.*").matcher(sql);
            if(matcher.find()){
                return matcher.group(1).trim();
            }
        }
        //DELETE FROM 表名称 WHERE 列名称 = 值
        if( sql.startsWith("delete") ){
            matcher = Pattern.compile("delete\\sfrom\\s(.+)where\\s(.*)").matcher(sql);
            if(matcher.find()){
                return matcher.group(1).trim();
            }
        }
        return null;
    }
}
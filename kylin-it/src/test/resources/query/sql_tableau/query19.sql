--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--


 
 
 SELECT "TableauSQL"."LSTG_FORMAT_NAME" AS "none_LSTG_FORMAT_NAME_nk", SUM("TableauSQL"."GMV_CNT") AS "sum_GMV_CNT_qk"
 FROM ( select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(price) as GMV_CNT
 from test_kylin_fact 
 group by test_kylin_fact.lstg_format_name ) "TableauSQL" 
 GROUP BY "TableauSQL"."LSTG_FORMAT_NAME"
;{"scanRowCount":300,"scanBytes":0,"scanFiles":1,"cuboidId":[14336]}
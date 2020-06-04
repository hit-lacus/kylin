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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.util.Objects;
import java.util.Set;
import java.util.Locale;
import java.util.Collections;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.IInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.SparkCreatingFlatTable;
import org.apache.kylin.engine.spark.SparkExecutable;
import org.apache.kylin.engine.spark.SparkExecutableFactory;
import org.apache.kylin.engine.spark.SparkSqlBatch;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.util.FlatTableSqlQuoteUtils;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class HiveInputBase {

    private static final Logger logger = LoggerFactory.getLogger(HiveInputBase.class);

    public static class BaseBatchCubingInputSide implements IInput.IBatchCubingInputSide {

        final protected IJoinedFlatTableDesc flatDesc;
        final protected String flatTableDatabase;
        final protected String hdfsWorkingDir;

        List<String> hiveViewIntermediateTables = Lists.newArrayList();

        public BaseBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            this.flatDesc = flatDesc;
            this.flatTableDatabase = config.getHiveDatabaseForIntermediateTable();
            this.hdfsWorkingDir = config.getHdfsWorkingDirectory();
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
            final KylinConfig cubeConfig = cubeInstance.getConfig();

            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);

            // create flat table first
            addStepPhase1_DoCreateFlatTable(jobFlow);

            // create global dict
            KylinConfig dictConfig = (flatDesc.getSegment()).getConfig();
            String[] mrHiveDictColumns = dictConfig.getMrHiveDictColumnsExcludeRefColumns();
            if (Objects.nonNull(mrHiveDictColumns) && mrHiveDictColumns.length > 0
                    && !"".equals(mrHiveDictColumns[0])) {
                String globalDictDatabase = dictConfig.getMrHiveDictDB();
                if (null == globalDictDatabase) {
                    throw new IllegalArgumentException("Mr-Hive Global dict database is null.");
                }
                String globalDictTable = cubeName + dictConfig.getMrHiveDictTableSuffix();
                addStepPhase1_DoCreateMrHiveGlobalDict(jobFlow, mrHiveDictColumns, globalDictDatabase, globalDictTable);
            }

            // then count and redistribute
            if (cubeConfig.isHiveRedistributeEnabled()) {
                final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                //jobFlow.addTask(createRedistributeFlatHiveTableStep(hiveInitStatements, cubeName, flatDesc, cubeInstance.getDescriptor()));
                if (kylinConfig.isLivyEnabled() && cubeInstance.getEngineType() == IEngineAware.ID_SPARK) {
                    jobFlow.addTask(createRedistributeFlatHiveTableByLivyStep(hiveInitStatements, cubeName, flatDesc,
                            cubeInstance.getDescriptor()));
                } else {
                    jobFlow.addTask(createRedistributeFlatHiveTableStep(hiveInitStatements, cubeName, flatDesc,
                            cubeInstance.getDescriptor()));
                }
            }

            // special for hive
            addStepPhase1_DoMaterializeLookupTable(jobFlow);
        }

        @Override
        public void addStepPhase_ReplaceFlatTableGlobalColumnValue(DefaultChainedExecutable jobFlow) {
            KylinConfig dictConfig = (flatDesc.getSegment()).getConfig();
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            String[] mrHiveDictColumnsExcludeRefCols = dictConfig.getMrHiveDictColumnsExcludeRefColumns();
            Map<String, String> dictRef = dictConfig.getMrHiveDictRefColumns();
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);

            String globalDictDatabase = dictConfig.getMrHiveDictDB();
            if (null == globalDictDatabase) {
                throw new IllegalArgumentException("Mr-Hive Global dict database is null.");
            }
            String globalDictTable = cubeName + dictConfig.getMrHiveDictTableSuffix();
            if(Objects.nonNull(mrHiveDictColumnsExcludeRefCols) && mrHiveDictColumnsExcludeRefCols.length > 0) {
                //merge to dict table step
                jobFlow.addTask(createHiveGlobalDictMergeGlobalDict(flatDesc, hiveInitStatements, cubeName, mrHiveDictColumnsExcludeRefCols, globalDictDatabase, globalDictTable));

                for (String item : mrHiveDictColumnsExcludeRefCols) {
                    dictRef.put(item, "");
                }
            }
            //toDo add replace step
        }

        protected void addStepPhase1_DoCreateMrHiveGlobalDict(DefaultChainedExecutable jobFlow,
                String[] mrHiveDictColumns, String globalDictDatabase, String globalDictTable) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);

            //Crete tables for global dict and extract distinct value
            jobFlow.addTask(createMrHiveGlobalDictExtractStep(flatDesc, hiveInitStatements, cubeName,
                    mrHiveDictColumns, globalDictDatabase, globalDictTable, jobFlow.getId()));

        }

        protected static AbstractExecutable createMrHiveGlobalDictExtractStep(IJoinedFlatTableDesc flatDesc,
                String hiveInitStatements, String cubeName, String[] mrHiveDictColumns,
                String globalDictDatabase, String globalDictTable, String jobId) {
            // Firstly, determine if the global dict hive table of cube is exists.
            String createGlobalDictTableHql = "CREATE TABLE IF NOT EXISTS " + globalDictDatabase + "." + globalDictTable
                    + "\n" + "( dict_key STRING COMMENT '', \n" + "dict_val INT COMMENT '' \n" + ") \n"
                    + "COMMENT '' \n" + "PARTITIONED BY (dict_column string) \n" + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' \n" + "STORED AS TEXTFILE; \n";

            final String dropDictIntermediateTableHql = MRHiveDictUtil.generateDropTableStatement(flatDesc);
            final String createDictIntermediateTableHql = MRHiveDictUtil.generateCreateTableStatement(flatDesc);
            final String groupByTable = flatDesc.getTableName() + flatDesc.getSegment().getConfig().getMrHiveDictIntermediateTTableSuffix();
            final String globalDictIntermediateTable = MRHiveDictUtil.getMRHiveFlatTableGlobalDictTableName(flatDesc);
            final String dropGlobalDictIntermediateTableHql = MRHiveDictUtil.generateDropTableStatement(globalDictIntermediateTable);
            final String createGlobalDictIntermediateTableHql = MRHiveDictUtil.generateCreateGlobalDicIntermediateTableStatement(globalDictIntermediateTable);

            String maxAndDistinctCountSql = "INSERT OVERWRITE TABLE  " + groupByTable + " PARTITION (DICT_COLUMN = '" + BatchConstants.CFG_GLOBAL_DICT_STATS_PARTITION_VALUE + "') "
                    + "\n" + "SELECT  CONCAT_WS(',', tc.dict_column, cast(tc.total_distinct_val AS String), if(tm.max_dict_val is null, '0', cast(max_dict_val as string))) "
                    + "\n" + "FROM ("
                    + "\n" + "    SELECT  dict_column,count(1) total_distinct_val FROM "
                    + "\n" + groupByTable + " where DICT_COLUMN != '" + BatchConstants.CFG_GLOBAL_DICT_STATS_PARTITION_VALUE + "' group by dict_column) tc "
                    + "\n" + "LEFT JOIN (\n"
                    + "\n" + "    SELECT  dict_column,if(max(dict_val) is null, 0, max(dict_val)) as max_dict_val FROM "
                    + "\n" + globalDictDatabase + "." + globalDictTable + " group by dict_column) tm "
                    + "\n" + "ON  tc.dict_column = tm.dict_column;";

            StringBuilder insertDataToDictIntermediateTableSql = new StringBuilder();
            for (String dictColumn : mrHiveDictColumns) {
                insertDataToDictIntermediateTableSql
                        .append(MRHiveDictUtil.generateInsertDataStatement(flatDesc, dictColumn, globalDictDatabase, globalDictTable));
            }
            String set = "set hive.exec.compress.output=false;set hive.mapred.mode=unstrict;";
            CreateMrHiveDictStep step = new CreateMrHiveDictStep();
            step.setInitStatement(hiveInitStatements);
            step.setCreateTableStatement(set + createGlobalDictTableHql + dropDictIntermediateTableHql
                    + createDictIntermediateTableHql + dropGlobalDictIntermediateTableHql + createGlobalDictIntermediateTableHql + insertDataToDictIntermediateTableSql.toString() + maxAndDistinctCountSql);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_GLOBAL_DICT_MRHIVE_EXTRACT_DICTVAL);
            step.setIsLock(true);
            step.setIsUnLock(false);
            step.setLockPathName(cubeName);
            return step;
        }

        protected static AbstractExecutable createHiveGlobalDictMergeGlobalDict(IJoinedFlatTableDesc flatDesc,
                                                                                String hiveInitStatements, String cubeName, String[] mrHiveDictColumns,
                                                                                String globalDictDatabase, String globalDictTable) {

            String globalDictItermediateTable = MRHiveDictUtil.getMRHiveFlatTableGlobalDictTableName(flatDesc);

            StringBuffer addPartition = new StringBuffer();
            Map<String, String> maxDictValMap = new HashMap<>();
            Map<String, String> dictHqlMap = new HashMap<>();
            for (String dictColumn : mrHiveDictColumns) {
                try {
                    addPartition.append("alter table ").append(globalDictItermediateTable)
                            .append(" add  IF NOT EXISTS partition (dict_column='").append(dictColumn)
                            .append("');").append(" \n");

                    String dictHql = "INSERT OVERWRITE TABLE " + globalDictDatabase + "." + globalDictTable + " \n"
                            + "PARTITION (dict_column = '" + dictColumn + "') \n"
                            + "SELECT dict_key, dict_val FROM "
                            + globalDictDatabase + "." + globalDictTable + " \n" + "WHERE dict_column = '" + dictColumn
                            + "' \n" + flatDesc.getDataModel().getConfig().getHiveUnionStyle() + " \n"
                            + "SELECT dict_key, dict_val FROM "
                            + globalDictItermediateTable + " \n" + " WHERE dict_column = '" + dictColumn + "' ;\n";
                    dictHqlMap.put(dictColumn, dictHql);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            String hiveInitStatementForUnstrict = "set hive.mapred.mode=unstrict;";
            CreateMrHiveDictStep step = new CreateMrHiveDictStep();
            step.setInitStatement(hiveInitStatements + hiveInitStatementForUnstrict + addPartition);
            step.setCreateTableStatementMap(dictHqlMap);
            step.setMaxDictStatementMap(maxDictValMap);
            step.setIsLock(false);
            step.setIsUnLock(false);
            step.setLockPathName(cubeName);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_GLOBAL_DICT_MRHIVE_BUILD_DICTVAL);
            return step;
        }

        protected void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);

            if (cubeInstance.getEngineType() == IEngineAware.ID_SPARK) {
                if (kylinConfig.isLivyEnabled()) {
                    jobFlow.addTask(createFlatHiveTableByLivyStep(hiveInitStatements,
                            jobWorkingDir, cubeName, flatDesc));
                } else {
                    if (kylinConfig.isSparCreateHiveTableViaSparkEnable()) {
                        jobFlow.addTask(createFlatHiveTableBySparkSql(hiveInitStatements,
                                jobWorkingDir, cubeName, flatDesc));
                    } else {
                        jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, flatDesc));
                    }
                }
            } else {
                jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, flatDesc));
            }
            //jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, flatDesc));
        }

        protected void addStepPhase1_DoMaterializeLookupTable(DefaultChainedExecutable jobFlow) {
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            AbstractExecutable task = createLookupHiveViewMaterializationStep(hiveInitStatements, jobWorkingDir,
                    flatDesc, hiveViewIntermediateTables, jobFlow.getId());
            if (task != null) {
                jobFlow.addTask(task);
            }
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            org.apache.kylin.source.hive.GarbageCollectionStep step = new org.apache.kylin.source.hive.GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_HIVE_CLEANUP);

            List<String> deleteTables = new ArrayList<>();
            deleteTables.add(getIntermediateTableIdentity());

            // mr-hive dict and inner table do not need delete hdfs
            String[] mrHiveDicts = flatDesc.getSegment().getConfig().getMrHiveDictColumns();
            if (Objects.nonNull(mrHiveDicts) && mrHiveDicts.length > 0) {
                String dictDb = flatDesc.getSegment().getConfig().getMrHiveDictDB();
                String tableName = dictDb + "." + flatDesc.getTableName() + "_"
                        + MRHiveDictUtil.DictHiveType.GroupBy.getName();
                deleteTables.add(tableName);
            }
            step.setIntermediateTables(deleteTables);

            step.setExternalDataPaths(Collections.singletonList(JoinedFlatTable.getTableDir(flatDesc, jobWorkingDir)));
            step.setHiveViewIntermediateTableIdentities(StringUtil.join(hiveViewIntermediateTables, ","));
            jobFlow.addTask(step);
        }

        protected String getIntermediateTableIdentity() {
            return flatTableDatabase + "." + flatDesc.getTableName();
        }
    }

    // ===== static methods ======

    protected static String getTableNameForHCat(TableDesc table, String uuid) {
        String tableName = (table.isView()) ? table.getMaterializedName(uuid) : table.getName();
        String database = (table.isView()) ? KylinConfig.getInstanceFromEnv().getHiveDatabaseForIntermediateTable()
                : table.getDatabase();
        return String.format(Locale.ROOT, "%s.%s", database, tableName).toUpperCase(Locale.ROOT);
    }

    protected static AbstractExecutable createFlatHiveTableStep(String hiveInitStatements, String jobWorkingDir,
            String cubeName, IJoinedFlatTableDesc flatDesc) {
        //from hive to hive
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, jobWorkingDir);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);

        CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
        step.setInitStatement(hiveInitStatements);
        step.setCreateTableStatement(dropTableHql + createTableHql + insertDataHqls);
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static AbstractExecutable createFlatHiveTableByLivyStep(String hiveInitStatements, String jobWorkingDir,
            String cubeName, IJoinedFlatTableDesc flatDesc) {
        //from hive to hive
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, jobWorkingDir);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);

        CreateFlatHiveTableByLivyStep step = new CreateFlatHiveTableByLivyStep();
        step.setInitStatement(hiveInitStatements);
        step.setCreateTableStatement(dropTableHql + createTableHql + insertDataHqls);
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static AbstractExecutable createFlatHiveTableBySparkSql(String hiveInitStatements,
            String jobWorkingDir, String cubeName, IJoinedFlatTableDesc flatDesc) {
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc,
                jobWorkingDir);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);

        KylinConfig config = flatDesc.getSegment().getConfig();
        final SparkExecutable sparkExecutable = SparkExecutableFactory.instance(config);
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_TABLE_WITH_SPARK);
        sparkExecutable.setClassName(SparkCreatingFlatTable.class.getName());

        sparkExecutable.setParam(SparkSqlBatch.OPTION_CUBE_NAME.getOpt(), cubeName);
        sparkExecutable.setParam(SparkSqlBatch.OPTION_STEP_NAME.getOpt(),
                base64EncodeStr(ExecutableConstants.STEP_NAME_CREATE_FLAT_TABLE_WITH_SPARK));
        sparkExecutable.setParam(SparkSqlBatch.OPTION_SEGMENT_ID.getOpt(),
                flatDesc.getSegment().getName());
        sparkExecutable.setParam(SparkSqlBatch.OPTION_SQL_COUNT.getOpt(),
                String.valueOf(SparkCreatingFlatTable.SQL_COUNT));

        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(0).getOpt(),
                base64EncodeStr(hiveInitStatements));
        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(1).getOpt(),
                base64EncodeStr(dropTableHql));

        // createTableHql include create table sql and alter table sql
        String[] sqlArr = createTableHql.trim().split(";");
        if (2 != sqlArr.length) {
            throw new RuntimeException("create table hql should combined by a create table sql " +
                    "and a alter sql, but got: " + createTableHql);
        }
        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(2).getOpt(),
                base64EncodeStr(sqlArr[0]));
        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(3).getOpt(),
                base64EncodeStr(sqlArr[1]));

        sparkExecutable.setParam(SparkCreatingFlatTable.getSqlOption(4).getOpt(),
                base64EncodeStr(insertDataHqls));

        StringBuilder jars = new StringBuilder();
        StringUtil.appendWithSeparator(jars, config.getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());

        return sparkExecutable;
    }

    private static String base64EncodeStr(String str) {
        return new String(
                Base64.getEncoder().encode(str.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8
        );
    }

    protected static AbstractExecutable createRedistributeFlatHiveTableStep(String hiveInitStatements, String cubeName,
            IJoinedFlatTableDesc flatDesc, CubeDesc cubeDesc) {
        RedistributeFlatHiveTableStep step = new RedistributeFlatHiveTableStep();
        step.setInitStatement(hiveInitStatements);
        step.setIntermediateTable(flatDesc.getTableName());
        step.setRedistributeDataStatement(JoinedFlatTable.generateRedistributeFlatTableStatement(flatDesc, cubeDesc));
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static AbstractExecutable createRedistributeFlatHiveTableByLivyStep(String hiveInitStatements,
            String cubeName, IJoinedFlatTableDesc flatDesc, CubeDesc cubeDesc) {
        RedistributeFlatHiveTableByLivyStep step = new RedistributeFlatHiveTableByLivyStep();
        step.setInitStatement(hiveInitStatements);
        step.setIntermediateTable(flatDesc.getTableName());
        step.setRedistributeDataStatement(JoinedFlatTable.generateRedistributeFlatTableStatement(flatDesc, cubeDesc));
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setName(ExecutableConstants.STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE);
        return step;
    }

    protected static ShellExecutable createLookupHiveViewMaterializationStep(String hiveInitStatements,
            String jobWorkingDir, IJoinedFlatTableDesc flatDesc, List<String> intermediateTables, String uuid) {
        ShellExecutable step = new ShellExecutable();
        step.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_HIVE_VIEW_IN_LOOKUP);

        KylinConfig kylinConfig = flatDesc.getSegment().getConfig();
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(kylinConfig);
        final Set<TableDesc> lookupViewsTables = Sets.newHashSet();

        String prj = flatDesc.getDataModel().getProject();
        for (JoinTableDesc lookupDesc : flatDesc.getDataModel().getJoinTables()) {
            TableDesc tableDesc = metadataManager.getTableDesc(lookupDesc.getTable(), prj);
            if (lookupDesc.getKind() == DataModelDesc.TableKind.LOOKUP && tableDesc.isView()) {
                lookupViewsTables.add(tableDesc);
            }
        }

        if (lookupViewsTables.size() == 0) {
            return null;
        }

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.overwriteHiveProps(kylinConfig.getHiveConfigOverride());
        hiveCmdBuilder.addStatement(hiveInitStatements);
        for (TableDesc lookUpTableDesc : lookupViewsTables) {
            String identity = FlatTableSqlQuoteUtils.quoteTableIdentity(lookUpTableDesc.getDatabase(), lookUpTableDesc.getName(), null);
            if (lookUpTableDesc.isView()) {
                String intermediate = lookUpTableDesc.getMaterializedName(uuid);
                String materializeViewHql = materializeViewHql(intermediate, identity, jobWorkingDir);
                hiveCmdBuilder.addStatement(materializeViewHql);
                intermediateTables.add(intermediate);
            }
        }

        step.setCmd(hiveCmdBuilder.build());
        return step;
    }

    // each append must be a complete hql.
    protected static String materializeViewHql(String viewName, String tableName, String jobWorkingDir) {
        StringBuilder createIntermediateTableHql = new StringBuilder();
        createIntermediateTableHql.append("DROP TABLE IF EXISTS `" + viewName + "`;\n");
        createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS `" + viewName + "` LIKE " + tableName
                + " LOCATION '" + jobWorkingDir + "/" + viewName + "';\n");
        createIntermediateTableHql.append("ALTER TABLE `" + viewName + "` SET TBLPROPERTIES('auto.purge'='true');\n");
        createIntermediateTableHql
                .append("INSERT OVERWRITE TABLE `" + viewName + "` SELECT * FROM " + tableName + ";\n");
        return createIntermediateTableHql.toString();
    }

    protected static String getJobWorkingDir(DefaultChainedExecutable jobFlow, String hdfsWorkingDir) {

        String jobWorkingDir = JobBuilderSupport.getJobWorkingDir(hdfsWorkingDir, jobFlow.getId());
        if (KylinConfig.getInstanceFromEnv().getHiveTableDirCreateFirst()) {
            // Create work dir to avoid hive create it,
            // the difference is that the owners are different.
            checkAndCreateWorkDir(jobWorkingDir);
        }
        return jobWorkingDir;
    }

    protected static void checkAndCreateWorkDir(String jobWorkingDir) {
        try {
            Path path = new Path(jobWorkingDir);
            FileSystem fileSystem = HadoopUtil.getFileSystem(path);
            if (!fileSystem.exists(path)) {
                logger.info("Create jobWorkDir : " + jobWorkingDir);
                fileSystem.mkdirs(path);
            }
        } catch (IOException e) {
            logger.error("Could not create lookUp table dir : " + jobWorkingDir);
        }
    }

}

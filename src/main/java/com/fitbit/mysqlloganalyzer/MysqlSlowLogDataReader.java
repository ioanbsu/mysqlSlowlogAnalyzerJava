package com.fitbit.mysqlloganalyzer;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ivanbahdanau
 */
public class MysqlSlowLogDataReader {

    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("YYddMM HH:mm:ss");

    private static final String TIME_INDICATOR = "# Time: ";
    private static final String USER_HOST_INDICATOR = "# User@Host: ";
    private static final String THREAD_ID_INDICATOR = "# Thread_id: ";
    private static final String QUERY_TIME_INDICATOR = "# Query_time: ";
    private static final String BYTES_SENT_INDICATOR = "# Bytes_sent: ";
    private static final String QC_HIT_INDICATOR = "# QC_Hit:";
    private static final String FILE_SORT_INDICATOR = "# Filesort: ";

    public Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> readFile(final File mysqlSlowLogFile) throws IOException {
        return Files.readLines(mysqlSlowLogFile, Charsets.UTF_8, new LineProcessor<Map<String, List<MysqlSlowLogObject.MysqlLogRecord>>>() {

            private MysqlSlowLogObject.MysqlLogRecord.Builder builder;
            Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> collectedData = Maps.newTreeMap();

            @Override
            public boolean processLine(String line) throws IOException {
                if (line.startsWith("/usr/sbin/mysqld") || line.startsWith("Tcp port:") ||
                        line.startsWith("Time ")) {
                    return true;
                }
                if (line.contains(TIME_INDICATOR)) {
                    if (builder != null) {
                        addToMap(builder.build());
                    }
                    builder = MysqlSlowLogObject.MysqlLogRecord.newBuilder();
                    builder.setEventDateTime(DATE_TIME_FORMAT.parseDateTime(line.substring(TIME_INDICATOR.length()).replaceAll("  ", " ")).getMillis());

                } else if (line.contains(USER_HOST_INDICATOR)) {
                    builder.setUserAndHost(line.substring(USER_HOST_INDICATOR.length()));
                } else if (line.contains(THREAD_ID_INDICATOR)) {
                    String threadInfo[] = line.split(" ");
                    builder.setThreadId(threadInfo[2])
                            .setSchema(threadInfo[5])
                            .setLastErrno(threadInfo[8])
                            .setKilled(threadInfo[11]);
                } else if (line.contains(QUERY_TIME_INDICATOR)) {
                    String queryTimes[] = line.split(" ");

                    builder.setQueryTime(Float.valueOf(queryTimes[2]))
                            .setLockTime(Float.valueOf(queryTimes[5]))
                            .setRowsSent(Long.valueOf(queryTimes[8]))
                            .setRowsExamined(Long.valueOf(queryTimes[11]))
                            .setRowsAffected(Long.valueOf(queryTimes[14]))
                            .setRowsRead(Long.valueOf(queryTimes[17]));
                } else if (line.contains(BYTES_SENT_INDICATOR)) {
                    String bytesStats[] = line.split(" ");
                    builder.setBytesSent(Long.valueOf(bytesStats[2]));
                    if (bytesStats.length >= 5) {
                        builder.setTmpTables(Long.valueOf(bytesStats[5]))
                                .setTmpDistTables(Long.valueOf(bytesStats[8]))
                                .setTmpTablesSizes(Long.valueOf(bytesStats[11]));
                    }
                } else if (line.contains(QC_HIT_INDICATOR)) {
                    String qcStats[] = line.split(" ");
                    builder.setQcHit(qcStats[2])
                            .setFullScan(qcStats[5])
                            .setFullJoin(qcStats[8])
                            .setTmpTable(qcStats[11])
                            .setTmpTableOnDisk(qcStats[14]);
                } else if (line.startsWith(FILE_SORT_INDICATOR)) {
                    String fileSort[] = line.split(" ");
                    builder.setFilesort(fileSort[2])
                            .setFilesortOnDisk(fileSort[5])
                            .setMergePases(Long.valueOf(fileSort[8]));
                } else if (!line.startsWith("SET timestamp") && !line.startsWith("use no_shard_db;")) {
                    builder.setSqlQuery(line);
                }
                return true;

            }

            @Override
            public Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> getResult() {
                addToMap(builder.build());
                return collectedData;
            }

            private void addToMap(MysqlSlowLogObject.MysqlLogRecord objectToAdd) {
                String sqlQuery = objectToAdd.getSqlQuery().replaceAll("\\.*(=|<|>)(\\d+|('([-@\\.0-9a-zA-Z: ]*)'))", "");
                if (!collectedData.containsKey(sqlQuery)) {
                    collectedData.put(sqlQuery, new ArrayList<>());
                }
                collectedData.get(sqlQuery).add(objectToAdd);

            }
        });
    }
}

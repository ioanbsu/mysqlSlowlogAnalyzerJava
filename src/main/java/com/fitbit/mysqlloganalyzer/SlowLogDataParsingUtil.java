package com.fitbit.mysqlloganalyzer;

import java.util.List;

/**
 * @author ivanbahdanau
 */
public class SlowLogDataParsingUtil {

    private SlowLogDataParsingUtil() {

    }

    public static int calculateTotalTime(final List<MysqlSlowLogObject.MysqlLogRecord> mysqlLogRecords) {
        float totalTimeSpent = 0;
        for (MysqlSlowLogObject.MysqlLogRecord mysqlLogRecord : mysqlLogRecords) {
            totalTimeSpent += mysqlLogRecord.getQueryTime();
        }
        return (int) totalTimeSpent;
    }

    public static float slowestQuery(final List<MysqlSlowLogObject.MysqlLogRecord> mysqlLogRecords) {
        float slowestQuery = 0;
        for (MysqlSlowLogObject.MysqlLogRecord mysqlLogRecord : mysqlLogRecords) {
            if (mysqlLogRecord.getQueryTime() > slowestQuery) {
                slowestQuery = mysqlLogRecord.getQueryTime();
            }
        }
        return slowestQuery;
    }

    public static int queryPayload(final List<MysqlSlowLogObject.MysqlLogRecord> queries) {
        int maxQuesryPayload = 0;
        for (MysqlSlowLogObject.MysqlLogRecord query : queries) {
            if (query.getSqlQuery().length() > maxQuesryPayload) {
                maxQuesryPayload = query.getSqlQuery().length();
            }
        }
        return maxQuesryPayload;
    }

    public static String querySample(final List<MysqlSlowLogObject.MysqlLogRecord> queries) {
        String sampleQuery = queries.get(0).getSqlQuery();
        return sampleQuery.substring(0, Math.min(500, sampleQuery.length()));
    }

}

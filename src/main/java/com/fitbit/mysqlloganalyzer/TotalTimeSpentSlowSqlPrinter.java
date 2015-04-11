package com.fitbit.mysqlloganalyzer;

import com.google.common.collect.Maps;

import java.text.MessageFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author ivanbahdanau
 */
public class TotalTimeSpentSlowSqlPrinter implements SlowSqlPrinter {

    private Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> sortedSqlQueries;

    public TotalTimeSpentSlowSqlPrinter(final Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> processedData) {
        sortedSqlQueries = Maps.newTreeMap(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int compared = SlowLogDataParsingUtil.calculateTotalTime(processedData.get(o2)) -
                        SlowLogDataParsingUtil.calculateTotalTime(processedData.get(o1));
                if (compared == 0) {
                    return o1.compareTo(o2);
                } else {
                    return compared;
                }
            }
        });
        sortedSqlQueries.putAll(processedData);
    }

    @Override
    public void printSorted() {
        for (Map.Entry<String, List<MysqlSlowLogObject.MysqlLogRecord>> stringListEntry : sortedSqlQueries.entrySet()) {
            List<MysqlSlowLogObject.MysqlLogRecord> queries = stringListEntry.getValue();
            float totalTime = SlowLogDataParsingUtil.calculateTotalTime(queries);
            float slowestQuery = SlowLogDataParsingUtil.slowestQuery(queries);
            System.out.println(
                    MessageFormat.format("Total time:{0,number,#}, Queries: {1}, Average per query(seconds): {2}," +
                                    " Slowest query: {3}, Query sample: {4}",
                            totalTime, queries.size(), totalTime / queries.size(), slowestQuery,
                            SlowLogDataParsingUtil.querySample(queries)));


        }
    }
}

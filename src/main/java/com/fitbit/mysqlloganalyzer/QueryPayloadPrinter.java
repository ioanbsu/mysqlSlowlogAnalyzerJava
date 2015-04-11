package com.fitbit.mysqlloganalyzer;

import com.google.common.collect.Maps;

import java.text.MessageFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author ivanbahdanau
 */
public class QueryPayloadPrinter implements SlowSqlPrinter {


    private Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> sortedSqlQueries;

    public QueryPayloadPrinter(final Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> processedData) {
        sortedSqlQueries = Maps.newTreeMap(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int compared = SlowLogDataParsingUtil.queryPayload(processedData.get(o2)) -
                        SlowLogDataParsingUtil.queryPayload(processedData.get(o1));
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
            System.out.println(
                    MessageFormat.format("Max length:{0,number,#}, Queries: {1}, Average per query(seconds): {2}," +
                                    "Query sample: {3}",
                            SlowLogDataParsingUtil.queryPayload(queries), queries.size(), totalTime / queries.size(),
                            SlowLogDataParsingUtil.querySample(queries)));

        }
    }
}

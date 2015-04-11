package com.fitbit.mysqlloganalyzer;

import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author ivanbahdanau
 */
public class NumberOfCallsSlowSqlSortedPrinter implements SlowSqlPrinter {


    private Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> sortedByNumberOfExec;

    public NumberOfCallsSlowSqlSortedPrinter(final Map<String, List<MysqlSlowLogObject.MysqlLogRecord>> processedData) {
        sortedByNumberOfExec = Maps.newTreeMap(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int compared = processedData.get(o2).size() - processedData.get(o1).size();
                if (compared == 0) {
                    return o1.compareTo(o2);
                } else {
                    return compared;
                }
            }
        });
        sortedByNumberOfExec.putAll(processedData);
    }

    public void printSorted() {
        for (Map.Entry<String, List<MysqlSlowLogObject.MysqlLogRecord>> stringListEntry : sortedByNumberOfExec.entrySet()) {
            System.out.println(stringListEntry.getValue().size() + ": " + stringListEntry.getKey().substring(0, Math.min(500, stringListEntry.getKey().length())));
        }
    }


}

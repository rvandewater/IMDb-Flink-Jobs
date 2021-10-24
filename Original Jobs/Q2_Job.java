package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Q2_Job {
    private static SaveExecutionPlan Saver;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String DataDirectory = "hdfs://cloud-7:44000/user/francesco-v/IMDBK/3GB/";
//        String DataDirectory ="C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //        TITLE.AKAS.TSV
        //        titleId (string) - a tconst, an alphanumeric unique identifier of the title.
        //        ordering (integer) – a number to uniquely identify rows for a given titleId.
        //        title (string) – the localized title.
        //        region (string) - the region for this version of the title.
        //        language (string) - the language of the title.
        //        types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd","festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning.
        //        attributes (array) - Additional terms to describe this alternative title, not enumerated.
        //        isOriginalTitle (boolean) – 0: not original title; 1: original title.

        DataSource<Tuple8<String, String, String, String, String, String, String, String>> title_akas = env.readCsvFile(DataDirectory + "title.akas.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class, String.class, String.class);

        // QUERY: Get all sorted unoriginal transliterated greek titles, merged into one list by the amount of entries they have.
        SortPartitionOperator<Tuple2<Integer, ArrayList<String>>> query = title_akas
                //Map to desired attributes
                .map(item -> new Tuple5<String, String, String, String, String>(item.f1, item.f2, item.f3, item.f6, item.f7)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING))
                //Filter null values
                .filter(item -> !item.f1.equals("\\N") && !item.f2.equals("\\N"))
                // Greek titles
                .filter(item -> item.f2.equals("GR"))
                //Unoriginal titles
                .filter(item -> item.f4.equals("0"))
                //Split properties
                .map(item -> new Tuple5<String, String, String, String[], String>(item.f0, item.f1, item.f2, item.f3.split(" "), item.f4)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.OBJECT_ARRAY(Types.STRING), Types.STRING))
                //Get transliterated
                .filter(item -> Arrays.stream(item.f3).anyMatch(x -> x.equals("transliterated")))
                //Size down to tuple2, adjust for merging titles
                .map(item -> new Tuple2<Integer, ArrayList<String>>(Integer.parseInt(item.f0), new ArrayList<String>(Arrays.asList(item.f1)))).returns(Types.TUPLE(Types.INT, Types.LIST(Types.STRING)))
                //Group by entry size
                .groupBy(item -> item.f0)
                //Reduce to one list of titles for each entry number
                .reduce((item1, item2) -> {
                    ArrayList<String> res = new ArrayList<String>();
                    res.addAll(item1.f1);
                    res.addAll(item2.f1);
                    return new Tuple2<Integer,
                            ArrayList<String>>(item1.f0, res);
                }).returns(Types.TUPLE(Types.INT, Types.LIST(Types.STRING)))
                //Sort
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

//         collected = query.collect();
//        collected.forEach(System.out::println);
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q2_Job", env);
    }
}

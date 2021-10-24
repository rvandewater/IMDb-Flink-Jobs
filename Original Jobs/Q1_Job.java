package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import scala.Console;
import java.io.*;

import java.util.List;

public class Q1_Job {

    public static void main(String[] args) throws Exception {

//        final ParameterTool params = ParameterTool.fromArgs(args);

        String DataDirectory = "hdfs://cloud-7:44000/user/francesco-v/IMDBK/3GB/";
//        String DataDirectory ="C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //        NAME.BASICS.TSV
        //        nconst (string) - alphanumeric unique identifier of the name/person.
        //        primaryName (string)– name by which the person is most often credited.
        //        birthYear – in YYYY format.
        //        deathYear – in YYYY format if applicable, else .
        //        primaryProfession (array of strings)– the top-3 professions of the person.
        //        knownForTitles (array of tconsts) – titles the person is known for.

        DataSource<Tuple6<String, String, String, String, String, String>> name_basics = env.readCsvFile(DataDirectory+ "name.basics.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class);

        // QUERY: The sorted last name, birthyear, deathyear, age of all actors that have aged between 20 and 30

        SortPartitionOperator<Tuple4<String, Integer, Integer, Integer>> query = name_basics
                // Map to name, birth_year, death_year
                .map(item -> new Tuple3<String, String, String>(item.f1, item.f2, item.f3)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                //Filter out the null lines
                .filter(item -> !item.f1.equals("\\N") && !item.f2.equals("\\N"))
                //Filter out header
                .filter(item -> !item.f1.equals("birthYear") && !item.f2.equals("deathYear"))
                //Parse birth/death_year to int
                .map(item -> new Tuple3<String, Integer, Integer>(item.f0, Integer.parseInt(item.f1), Integer.parseInt(item.f2))).returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                //Filter death_age between 20/30
                .filter(item -> item.f2 - item.f1 >= 20 && item.f2 - item.f1 <= 30)
                //Get last name, birth year, death year, age of death
                .map(item -> new Tuple4<String, Integer, Integer, Integer>(item.f0.split(" ")[item.f0.split(" ").length - 1], item.f1, item.f2, item.f2 - item.f1)).returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.INT))
                // Sort
                .sortPartition(0, Order.ASCENDING).setParallelism(1);

//         collected = query.collect();
//        collected.forEach(System.out::println);
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q1_Job", env);

    }

}

package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Q3_Job {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

//        String DataDirectory ="C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\";
        String DataDirectory = "hdfs://cloud-7:44000/user/francesco-v/IMDBK/3GB/";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //        NAME.BASICS.TSV
        //        nconst (string) - alphanumeric unique identifier of the name/person.
        //        primaryName (string)– name by which the person is most often credited.
        //        birthYear – in YYYY format.
        //        deathYear – in YYYY format if applicable, else .
        //        primaryProfession (array of strings)– the top-3 professions of the person.
        //        knownForTitles (array of tconsts) – titles the person is known for.
        DataSource<Tuple6<String, String, String, String, String, String>> name_basics = env.readCsvFile(DataDirectory + "name.basics.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class);

        //        TITLE.PRINCIPALS.TSV
        //        tconst (string) - alphanumeric unique identifier of the title.
        //        ordering (integer) – a number to uniquely identify rows for a given titleId.
        //        nconst (string) - alphanumeric unique identifier of the name/person.
        //        category (string) - the category of job that person was in.
        //        job (string) - the specific job title if applicable, else.
        //        characters (string) - the name of the character played if applicable, else.
        DataSource<Tuple6<String, String, String, String, String, String>> title_principals = env.readCsvFile(DataDirectory + "title.principals.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class);

        // All the occurrences of actors from the 1960's
        FilterOperator<Tuple6<String, String, Integer, Integer, String[], String>> name_basics_filtered = name_basics
                .filter(item -> !item.f2.equals("\\N") && !item.f3.equals("\\N"))
                .filter(item -> item.f2.equals("1960"))
//                .filter(item -> item.f3.equals("2000"))
                .map(item -> new Tuple6<String, String, Integer, Integer, String[], String>(item.f0, item.f1, Integer.parseInt(item.f2), Integer.parseInt(item.f3), item.f4.split(","), item.f5)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT, Types.OBJECT_ARRAY(Types.STRING), Types.STRING))
//                .filter(item -> item.f3<1990)
                .filter(item -> Arrays.stream(item.f4).anyMatch(x -> x.equals("actor")));

        JoinOperator.ProjectJoin<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, Integer, Integer, String[], String>, Tuple> join = title_principals
                .join(name_basics_filtered)
                .where(item -> item.f2)
                .equalTo(item -> item.f0)
                .projectSecond(1).projectFirst(5);//.returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)));

        // QUERY: Get all the roles from actors, as well as the number of roles played
        //join.map(item -> new Tuple2<String, String>(item.getField(0), item.getField(1))).returns(Types.TUPLE(Types.STRING, Types.STRING));
        ReduceOperator<Tuple3<String, ArrayList<String>, Integer>> query = join.map(item -> new Tuple3<String, String, Integer>(item.getField(0), item.getField(1), 1)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
                .filter(item -> !item.f1.equals("\\N"))
                .map(item -> new Tuple3<String, ArrayList<String>, Integer>(item.f0, new ArrayList<String>(Arrays.asList(item.f1)), item.f2)).returns(Types.TUPLE(Types.STRING, Types.LIST(Types.STRING), Types.INT))
                .groupBy(0)
                .reduce((item1, item2) -> {
                    ArrayList<String> res = new ArrayList<String>();
                    res.addAll(item1.f1);
                    res.addAll(item2.f1);
                    return new Tuple3<String, ArrayList<String>, Integer>(item1.f0, res, item1.f2 + item2.f2);
                });


//         collected = query.collect();
//        collected.forEach(System.out::println);
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q3_Job", env);
    }
}

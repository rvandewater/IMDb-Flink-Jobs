package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.Arrays;

public class Q9_Job {
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

        //        TITLE.BASICS.TSV
        //        tconst (string) - alphanumeric unique identifier of the title.
        //        titleType (string) – the type/format of the title (e.g. movie, short,tvseries, tvepisode, video, etc).
        //        primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release.
        //        originalTitle (string) - original title, in the original language.
        //        isAdult (boolean) - 0: non-adult title; 1: adult title.
        //        startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year.
        //        endYear (YYYY) – TV Series end year. for all other title types.
        //        runtimeMinutes – primary runtime of the title, in minutes.
        //        genres (string array) – includes up to three genres associated with the title.

        DataSource<Tuple9<String, String, String, String, String, String, String, String, String>> title_basics= env.readCsvFile(DataDirectory+"title.basics.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class, String.class, String.class,  String.class);


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
                .projectSecond(1).projectFirst(5, 0);//.returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)));

        // QUERY: Get all the roles from actors and in which movies they played this role, as well as the number of roles played
        //join.map(item -> new Tuple2<String, String>(item.getField(0), item.getField(1))).returns(Types.TUPLE(Types.STRING, Types.STRING));
        MapOperator<Tuple4<String, String, String, Integer>, Tuple4<String, ArrayList<String>, String, Integer>> intermediate_map = join.map(item -> new Tuple4<String, String, String, Integer>(item.getField(0), item.getField(1), item.getField(2), 1)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.INT))
                .filter(item -> !item.f1.equals("\\N"))
                .map(item -> new Tuple4<String, ArrayList<String>, String, Integer>(item.f0, new ArrayList<String>(Arrays.asList(item.f1)), item.f2, item.f3)).returns(Types.TUPLE(Types.STRING, Types.LIST(Types.STRING), Types.STRING, Types.INT));

        JoinOperator.ProjectJoin<Tuple4<String, ArrayList<String>, String, Integer>, Tuple9<String, String, String, String, String, String, String, String, String>, Tuple> join2 = intermediate_map
                .join(title_basics)
                .where(item -> item.f2)
                .equalTo(item -> item.f0)
                .projectFirst(0, 1, 3)
                .projectSecond(3);

        MapOperator<Tuple, Tuple4<String, ArrayList<String>, Integer, ArrayList<String>>> intermediate_map2 = join2.map(item -> new Tuple4<String, ArrayList<String>, Integer, ArrayList<String>>(item.getField(0), item.getField(1), item.getField(2), new ArrayList<String>(Arrays.asList(new String[]{item.getField(3)})))).returns(Types.TUPLE(Types.STRING, Types.LIST(Types.STRING), Types.INT, Types.LIST(Types.STRING)));

        ReduceOperator<Tuple4<String, ArrayList<String>, Integer, ArrayList<String>>> query = intermediate_map2
                .groupBy(0)
                .reduce((item1, item2) -> {
                    ArrayList<String> res1 = new ArrayList<String>();
                    res1.addAll(item1.f1);
                    res1.addAll(item2.f1);

                    ArrayList<String> res2 = new ArrayList<String>();
                    res2.addAll(item1.f3);
                    res2.addAll(item2.f3);

                    return new Tuple4<String, ArrayList<String>, Integer, ArrayList<String>>(item1.f0, res1, item1.f2 + item2.f2, res2);
                });


//         collected = query.collect();
//        collected.forEach(System.out::println);

        // Collect output and plan information
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q9_Job", env);
    }
}

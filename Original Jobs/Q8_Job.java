package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.Arrays;

public class Q8_Job {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

//        String DataDirectory ="C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\";
        String DataDirectory = "hdfs://cloud-7:44000/user/francesco-v/IMDBK/3GB/";


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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

        //        TITLE.RATINGS.TSV
        //        tconst (string) - alphanumeric unique identifier of the title.
        //        averageRating – weighted average of all the individual user ratings.
        //        numVotes - number of votes the title has received.

        DataSource<Tuple3<String, String, String>> title_ratings = env.readCsvFile(DataDirectory+"title.ratings.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class);

        //        NAME.BASICS.TSV
        //        nconst (string) - alphanumeric unique identifier of the name/person.
        //        primaryName (string)– name by which the person is most often credited.
        //        birthYear – in YYYY format.
        //        deathYear – in YYYY format if applicable, else .
        //        primaryProfession (array of strings)– the top-3 professions of the person.
        //        knownForTitles (array of tconsts) – titles the person is known for.
        DataSource<Tuple6<String, String, String, String, String, String>> name_basics = env.readCsvFile(DataDirectory + "name.basics.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class);

        //QUERY: Actors that are primarily known for films/series produced before 1970 with a rating of at least 8.5, with more than 10 reviews
        // Parse and filter
        FilterOperator<Tuple9<String, String, String, String, String, Integer, String, String, String>> title_basics_filtered = title_basics
                // Filter to remove unparsable lines
                .filter(item -> !item.f5.equals("startYear") && !item.f5.equals("\\N"))
                // Parse to int
                .map(item -> new Tuple9<String, String, String, String, String, Integer, String, String, String>(item.f0, item.f1, item.f2, item.f3, item.f4, Integer.parseInt(item.f5), item.f6, item.f7, item.f8)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING, Types.STRING))
                // Filter to startyear less than 1950
                .filter(item -> item.f5 < 1970);


        //Parse and Filter
        FilterOperator<Tuple3<String, Float, Integer>> title_ratings_filtered = title_ratings
                //Filter out unparsable
                .filter(item -> !item.f0.equals("tconst"))
                // Parse to float and int
                .map(item -> new Tuple3<String, Float, Integer>(item.f0, Float.parseFloat(item.f1), Integer.parseInt(item.f2))).returns(Types.TUPLE(Types.STRING, Types.FLOAT, Types.INT))
                // Rating at least 8.5, more than 10 reviews
                .filter(item -> item.f1 > 8.5 && item.f2 > 10);

        MapOperator<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, Integer, Integer, ArrayList<String>, ArrayList<String>>> name_basics_mapped = name_basics
                .filter(item -> !item.f0.equals("nconst") && !item.f4.equals("\\N") && !item.f2.equals("\\N") && !item.f3.equals("\\N"))
                .map(item -> new Tuple6<String, String, Integer, Integer, ArrayList<String>, ArrayList<String>>(item.f0, item.f1, Integer.parseInt(item.f2), Integer.parseInt(item.f3), new ArrayList<String>(Arrays.asList(item.f4.split(","))), new ArrayList<String>(Arrays.asList(item.f5.split(","))))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT, Types.LIST(Types.STRING), Types.LIST(Types.STRING)));

        //Join tables
        MapOperator<Tuple, Tuple4<String, String, Float, Integer>> join = title_basics_filtered
                .join(title_ratings_filtered)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                // Get Movie/Show title
                .projectFirst(0, 2)
                // Get rating and number of ratings
                .projectSecond(1, 2)
                .map(item -> new Tuple4<String, String, Float, Integer>(item.getField(0), item.getField(1), item.getField(2), item.getField(3))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.FLOAT, Types.INT));

        JoinOperator.ProjectJoin<Tuple4<String, String, Float, Integer>, Tuple6<String, String, Integer, Integer, ArrayList<String>, ArrayList<String>>, Tuple> join2 = join
                .join(name_basics_mapped)
                .where(item -> item.f0)
                .equalTo(item -> item.f5.get(0))
                .projectFirst(1, 2, 3)
                .projectSecond(1, 2, 3, 4);

//         collected = join2.collect();
//        collected.forEach(System.out::println);

        // Collect output and plan information
        JoinOperator.ProjectJoin<Tuple4<String, String, Float, Integer>, Tuple6<String, String, Integer, Integer, ArrayList<String>, ArrayList<String>>, Tuple> query = join2;
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q8_Job", env);
    }
}

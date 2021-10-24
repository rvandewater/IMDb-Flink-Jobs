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

public class Q5_Job {
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

        //        TITLE.AKAS.TSV
        //        titleId (string) - a tconst, an alphanumeric unique identifier of the title.
        //        ordering (integer) – a number to uniquely identify rows for a given titleId.
        //        title (string) – the localized title.
        //        region (string) - the region for this version of the title.
        //        language (string) - the language of the title.
        //        types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd","festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning.
        //        attributes (array) - Additional terms to describe this alternative title, not enumerated.
        //        isOriginalTitle (boolean) – 0: not original title; 1: original title.

        DataSource<Tuple8<String, String, String, String, String, String, String, String>> title_akas = env.readCsvFile(DataDirectory+"title.akas.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class, String.class, String.class);


        //QUERY: Get titles after 1950, with a rating of at least 8.5, with more than 10 reviews that are german.

        // Parse and filter
        FilterOperator<Tuple9<String, String, String, String, String, Integer, String, String, String>> title_basics_filtered = title_basics
                // Filter to remove unparsable lines
                .filter(item -> !item.f5.equals("startYear") && !item.f5.equals("\\N"))
                // Parse to int
                .map(item -> new Tuple9<String, String, String, String, String, Integer, String, String, String>(item.f0, item.f1, item.f2, item.f3, item.f4, Integer.parseInt(item.f5), item.f6, item.f7, item.f8)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING, Types.STRING))
                // Filter to startyear less than 1950
                .filter(item -> item.f5 > 1950);


        //Parse and Filter
        FilterOperator<Tuple3<String, Float, Integer>> title_ratings_filtered = title_ratings
                //Filter out unparsable
                .filter(item -> !item.f0.equals("tconst"))
                // Parse to float and int
                .map(item -> new Tuple3<String, Float, Integer>(item.f0, Float.parseFloat(item.f1), Integer.parseInt(item.f2))).returns(Types.TUPLE(Types.STRING, Types.FLOAT, Types.INT))
                // Rating at least 8.5, more than 10 reviews
                .filter(item -> item.f1 > 8.5 && item.f2 > 10);


        //Join tables
        MapOperator<Tuple, Tuple4<String, String, Float, Integer>> join = title_basics_filtered
                .join(title_ratings_filtered)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                // Get Movie/Show title
                .projectFirst(0, 2)
                // Get rating and number of ratings
                .projectSecond(1, 2)
                .map(item -> new Tuple4<String, String, Float, Integer>(item.getField(0), item.getField(1), item.getField(2), item.getField(3))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.FLOAT, Types.INT));//.returns(Types.TUPLE(Types.STRING, Types.OBJECT_ARRAY(Types.STRING)));

        JoinOperator.ProjectJoin<Tuple4<String, String, Float, Integer>, Tuple8<String, String, String, String, String, String, String, String>, Tuple> join2 = join
                .join(title_akas)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                .projectFirst(1, 2, 3)
                .projectSecond(4);

        FilterOperator<Tuple4<String, Float, Integer, String>> query = join2.map(item -> new Tuple4<String, Float, Integer, String>(item.getField(0), item.getField(1), item.getField(2), item.getField(3))).returns(Types.TUPLE(Types.STRING, Types.FLOAT, Types.INT, Types.STRING))
                .filter(item -> item.f3.equals("de"));

//         collected = result.collect();
//        collected.forEach(System.out::println);

        // Collect output and plan information
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q5_Job", env);
    }
}

package de.tuberlin.dima.aim3.exercises;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;

public class Q10_Job {
    private static SaveExecutionPlan Saver;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

//        String DataDirectory ="C:\\Users\\Robin\\Documents\\experimentdata\\IMDB\\";
        String DataDirectory = "hdfs://cloud-7:44000/user/francesco-v/IMDBK/3GB/";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //        TITLE.PRINCIPALS.TSV
        //        tconst (string) - alphanumeric unique identifier of the title.
        //        ordering (integer) – a number to uniquely identify rows for a given titleId.
        //        nconst (string) - alphanumeric unique identifier of the name/person.
        //        category (string) - the category of job that person was in.
        //        job (string) - the specific job title if applicable, else.
        //        characters (string) - the name of the character played if applicable, else.
        DataSource<Tuple6<String, String, String, String, String, String>> title_principals = env.readCsvFile(DataDirectory + "title.principals.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class);

        //        TITLE.RATINGS.TSV
        //        tconst (string) - alphanumeric unique identifier of the title.
        //        averageRating – weighted average of all the individual user ratings.
        //        numVotes - number of votes the title has received.

        DataSource<Tuple3<String, String, String>> title_ratings = env.readCsvFile(DataDirectory + "title.ratings.tsv")
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

        DataSource<Tuple9<String, String, String, String, String, String, String, String, String>> title_basics= env.readCsvFile(DataDirectory +"\\title.basics.tsv")
                .fieldDelimiter("\t").types(String.class, String.class, String.class, String.class, String.class,String.class, String.class, String.class,  String.class);

        // QUERY: Get all the actors details of german movies that contain archive footage, with the ratings, sorted by the title.
        // Example: (tt1439063,Folge #3.5,de,\N,tvEpisode,Episode #3.5,Episode #3.5,6.7,6,10,nm1400170,actress,\N,["Heidi Klum","Anna Heesch","Tanja Seifert"],Martina Hill,1974,\N,actress,writer,soundtrack,tt1441143,tt0115088,tt2119785,tt0982599)
        FilterOperator<Tuple8<String, String, String, String, String, String, String, String>> title_akas_filtered = title_akas
                .filter(item -> item.f4.equals("de"));
//                .filter( item -> item.f0.equals("tt1439063"));

        MapOperator<Tuple, Tuple7<String, String, String, String, String, String, String>> join = title_akas_filtered
                .join(title_basics)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                .projectFirst(0, 2, 4, 5).projectSecond(1, 2, 3)
                .map(item -> new Tuple7<String, String, String, String, String, String, String>(item.getField(0), item.getField(1), item.getField(2), item.getField(3), item.getField(4), item.getField(5), item.getField(6))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING));

        MapOperator<Tuple, Tuple9<String, String, String, String, String, String, String, String, String>> join2 = join.join(title_ratings)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                .projectFirst(0, 1, 2, 3, 4, 5, 6)
                .projectSecond(1, 2)
                .map(item -> new Tuple9<String, String, String, String, String, String, String, String, String>(item.getField(0), item.getField(1), item.getField(2), item.getField(3), item.getField(4), item.getField(5), item.getField(6), item.getField(7), item.getField(8))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING));


        MapOperator<Tuple, Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>> join3 = join2.join(title_principals)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                .projectFirst(0, 1, 2, 3, 4, 5, 6, 7, 8)
                .projectSecond(1, 2, 3, 4, 5)
                .map(item -> new Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>(item.getField(0), item.getField(1), item.getField(2), item.getField(3), item.getField(4), item.getField(5), item.getField(6), item.getField(7), item.getField(8), item.getField(9), item.getField(10), item.getField(11), item.getField(12), item.getField(13))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING));

        SortPartitionOperator<Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>> query = join3
                .filter(item -> item.f11.equals("archive_footage"))
                .sortPartition(0, Order.DESCENDING).setParallelism(1);



//         collected = query.collect();
//        collected.forEach(System.out::println);

        // Collect output and plan information
        query.output(new DiscardingOutputFormat<>());
         Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q10_Job", env);
    }
}

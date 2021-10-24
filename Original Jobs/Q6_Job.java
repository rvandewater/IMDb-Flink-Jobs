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

public class Q6_Job {
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

        // QUERY: Get all the actors of german movies, with the types of movie, ratings, year, years born/death , roles, jobs. 4 joins involved.
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


        JoinOperator.ProjectJoin<Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>, Tuple> join4 = join3.join(name_basics)
                .where(item -> item.f10)
                .equalTo(item -> item.f0)
                .projectFirst(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
                .projectSecond(1, 2, 3, 4, 5);


//         collected = join4.collect();
//        collected.forEach(System.out::println);
        JoinOperator.ProjectJoin<Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>, Tuple> query = join4;

        // Collect output and plan information
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q6_Job", env);
    }
}

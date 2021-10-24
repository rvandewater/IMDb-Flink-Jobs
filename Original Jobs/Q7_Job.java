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

public class Q7_Job {
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

        // QUERY: Get the movie title, role, rating, name, year of birth/death of people that participated in a movie of at least rating 7
        // Example: (tt1439063,Folge #3.5,de,\N,tvEpisode,Episode #3.5,Episode #3.5,6.7,6,10,nm1400170,actress,\N,["Heidi Klum","Anna Heesch","Tanja Seifert"],Martina Hill,1974,\N,actress,writer,soundtrack,tt1441143,tt0115088,tt2119785,tt0982599)
        FilterOperator<Tuple6<String, String, Integer, Integer, ArrayList<String>, String>> name_basics_filtered = name_basics
                .filter(item -> !item.f0.equals("nconst") && !item.f4.equals("\\N") && !item.f2.equals("\\N") && !item.f3.equals("\\N"))
                .map(item -> new Tuple6<String, String, Integer, Integer, ArrayList<String>, String>(item.f0, item.f1, Integer.parseInt(item.f2), Integer.parseInt(item.f3), new ArrayList<String>(Arrays.asList(item.f4.split(","))), item.f5)).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT, Types.LIST(Types.STRING), Types.STRING))
                .filter(item -> item.f4.contains("actor"));

        FilterOperator<Tuple3<String, Float, Integer>> title_ratings_filtered = title_ratings
                //Filter out unparsable
                .filter(item -> !item.f0.equals("tconst"))
                // Parse to float and int
                .map(item -> new Tuple3<String, Float, Integer>(item.f0, Float.parseFloat(item.f1), Integer.parseInt(item.f2))).returns(Types.TUPLE(Types.STRING, Types.FLOAT, Types.INT))
                // Rating at least 7
                .filter(item -> item.f1 > 7.0);

        FilterOperator<Tuple6<String, String, String, String, String, String>> title_principals_filtered = title_principals.filter(item -> item.f3.equals("actor"));

        MapOperator<Tuple, Tuple8<String, String, String, String, String, String, Float, Integer>> join = title_principals_filtered
                .join(title_ratings_filtered)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                .projectFirst(0, 1, 2, 3, 4, 5)
                .projectSecond(1, 2)
                .map(item -> new Tuple8<String, String, String, String, String, String, Float, Integer>(item.getField(0), item.getField(1), item.getField(2), item.getField(3), item.getField(4), item.getField(5), item.getField(6), item.getField(7))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.FLOAT, Types.INT));

        MapOperator<Tuple, Tuple8<String, String, Float, Integer, String, String, Integer, Integer>> join2 = join
                .join(name_basics_filtered)
                .where(item -> item.f2)
                .equalTo(item -> item.f0)
                .projectFirst(0, 5, 6, 7)
                .projectSecond(0, 1, 2, 3)
                .map(item -> new Tuple8<String, String, Float, Integer, String, String, Integer, Integer>(item.getField(0), item.getField(1), item.getField(2), item.getField(3), item.getField(4), item.getField(5), item.getField(6), item.getField(7))).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.FLOAT, Types.INT, Types.STRING, Types.STRING, Types.INT, Types.INT));

        MapOperator<Tuple8<String, String, String, String, String, String, String, String>, Tuple2<String, String>> title_akas_mapped = title_akas.map(item -> new Tuple2<String, String>(item.f0, item.f2)).returns(Types.TUPLE(Types.STRING, Types.STRING));

        JoinOperator.ProjectJoin<Tuple8<String, String, Float, Integer, String, String, Integer, Integer>, Tuple2<String, String>, Tuple> join3 = join2.join(title_akas_mapped)
                .where(item -> item.f0)
                .equalTo(item -> item.f0)
                .projectSecond(1)
                .projectFirst(1, 2, 5, 6, 7);



//         collected = join3.collect();
//        collected.forEach(System.out::println);

        // Collect output and plan information
        JoinOperator.ProjectJoin<Tuple8<String, String, Float, Integer, String, String, Integer, Integer>, Tuple2<String, String>, Tuple> query = join3;
        query.output(new DiscardingOutputFormat<>());
        SaveExecutionPlan Saver = new SaveExecutionPlan();
        Saver.GetExecutionPlan(env);
        Saver.SaveExecutionPlan("Q7_Job", env);
    }
}

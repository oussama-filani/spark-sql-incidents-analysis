package net.oussama;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

/**
 * Application Spark SQL pour analyser les incidents
 */
public class AnalyseIncidents {

    public static void main(String[] args) {
        // Initialisation de la session Spark
        SparkSession spark = SparkSession.builder()
                .appName("Analyse des Incidents Industriels")
                .master("local[*]")
                .getOrCreate();

        // Définition du schéma pour le CSV
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("Id", DataTypes.IntegerType, false),
                DataTypes.createStructField("titre", DataTypes.StringType, false),
                DataTypes.createStructField("description", DataTypes.StringType, false),
                DataTypes.createStructField("service", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.DateType, false)
        });

        // Chemin du fichier CSV - à remplacer par le chemin réel
        String cheminFichier = "incidents.csv";

        // Chargement des données
        Dataset<Row> incidentsDF = spark.read()
                .option("header", "true")
                .option("delimiter", ",")
                .option("quote", "\"")
                .option("dateFormat", "yyyy-MM-dd")
                .schema(schema)
                .csv(cheminFichier);

        incidentsDF.printSchema();

        // Création d'une vue temporaire pour les requêtes SQL
        incidentsDF.createOrReplaceTempView("incidents");

        // 1. Afficher le nombre d'incidents par service
        System.out.println("=== Nombre d'incidents par service ===");
        Dataset<Row> incidentsParService = spark.sql(
                "SELECT service, COUNT(*) AS nombre_incidents " +
                        "FROM incidents " +
                        "GROUP BY service " +
                        "ORDER BY nombre_incidents DESC");

        incidentsParService.show();

        // 2. Afficher les deux années où il y avait plus d'incidents
        System.out.println("=== Les deux années avec le plus d'incidents ===");
        Dataset<Row> incidentsParAnnee = spark.sql(
                "SELECT YEAR(date) AS annee, COUNT(*) AS nombre_incidents " +
                        "FROM incidents " +
                        "GROUP BY annee " +
                        "ORDER BY nombre_incidents DESC " +
                        "LIMIT 2");

        incidentsParAnnee.show();

        // Méthode alternative avec l'API Dataset
        System.out.println("=== Les deux années avec le plus d'incidents (via API) ===");
        Dataset<Row> incidentsParAnneeDFAPI = incidentsDF
                .withColumn("annee", year(col("date")))
                .groupBy("annee")
                .count()
                .orderBy(desc("count"))
                .limit(2);

        incidentsParAnneeDFAPI.show();

        // Arrêt de la session Spark
        spark.stop();
    }
}
"""Même pipeline que `transform.py`, écrit en PySpark.

Les règles sont identiques ; un test vérifie que les deux moteurs produisent
les mêmes chiffres. DuckDB suffit pour l'Île-de-France sur un portable,
la version Spark montre comment le même traitement passe à l'échelle
(toute la France, ou un cluster Databricks).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from functools import reduce
from pathlib import Path

from . import config

COLONNES = {
    "id_mutation": "string",
    "date_mutation": "date",
    "numero_disposition": "string",
    "nature_mutation": "string",
    "valeur_fonciere": "double",
    "code_commune": "string",
    "nom_commune": "string",
    "code_departement": "string",
    "id_parcelle": "string",
    "lot1_numero": "string",
    "code_type_local": "string",
    "type_local": "string",
    "surface_reelle_bati": "double",
    "longitude": "double",
    "latitude": "double",
}

SQL_COMMUNES = """
WITH centres AS (
    SELECT code_commune,
           percentile(longitude, 0.5) AS longitude,
           percentile(latitude, 0.5)  AS latitude
    FROM ventes
    WHERE longitude IS NOT NULL AND latitude IS NOT NULL
    GROUP BY code_commune
),
agregats AS (
    SELECT
        code_commune,
        max(nom_commune)                                   AS nom_commune,
        code_departement,
        annee,
        coalesce(type_bien, 'Tous')                        AS type_bien,
        count(*)                                           AS nb_ventes,
        CAST(round(percentile(prix_m2, 0.5)) AS INT)       AS prix_m2_median,
        CAST(round(percentile(prix_m2, 0.25)) AS INT)      AS prix_m2_q1,
        CAST(round(percentile(prix_m2, 0.75)) AS INT)      AS prix_m2_q3,
        CAST(round(percentile(surface, 0.5)) AS INT)       AS surface_mediane
    FROM ventes
    GROUP BY GROUPING SETS (
        (code_commune, code_departement, annee, type_bien),
        (code_commune, code_departement, annee)
    )
    HAVING count(*) >= {min_ventes}
)
SELECT a.*, round(c.latitude, 5) AS latitude, round(c.longitude, 5) AS longitude
FROM agregats a
LEFT JOIN centres c USING (code_commune)
ORDER BY code_commune, annee, type_bien
"""

SQL_DEPARTEMENTS = """
SELECT
    coalesce(code_departement, 'Ensemble')             AS code_departement,
    annee,
    coalesce(type_bien, 'Tous')                        AS type_bien,
    count(*)                                           AS nb_ventes,
    CAST(round(percentile(prix_m2, 0.5)) AS INT)       AS prix_m2_median
FROM ventes
GROUP BY GROUPING SETS (
    (code_departement, annee, type_bien),
    (code_departement, annee),
    (annee, type_bien),
    (annee)
)
ORDER BY code_departement, annee, type_bien
"""


def session_locale(nom: str = "prix-immo-idf"):
    """Session Spark locale, sans interface web, adaptée à un portable."""
    from pyspark.sql import SparkSession

    # Sous Windows, Spark doit savoir quel Python lancer pour ses workers.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if os.name == "nt" and "JAVA_TOOL_OPTIONS" not in os.environ:
        # Java 16+ crée ses sockets locales dans %TEMP% ; un chemin long ou avec espaces
        # fait échouer Netty (« Unable to establish loopback connection »).
        court = Path(os.environ.get("SystemDrive", "C:") + "\\") / "Temp" / "spark-sockets"
        court.mkdir(parents=True, exist_ok=True)
        os.environ["JAVA_TOOL_OPTIONS"] = f"-Djdk.net.unixdomain.tmpdir={court}"
    return (
        SparkSession.builder.appName(nom)
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def lire_brut(spark, fichiers: list[Path]):
    """Lit les CSV DVF en texte, puis convertit chaque colonne explicitement."""
    from pyspark.sql import functions as F

    lus = [spark.read.csv(str(f), header=True, inferSchema=False) for f in fichiers]
    brut = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), lus)
    return brut.select(
        *[
            # Hors mode ANSI (défaut), une valeur illisible devient NULL au lieu de faire échouer le job.
            (F.to_date(F.col(c)) if t == "date" else F.col(c).cast(t)).alias(
                "surface" if c == "surface_reelle_bati" else c
            )
            for c, t in COLONNES.items()
        ]
    )


def ventes_candidates(brut):
    """Une ligne par mutation portant sur un seul logement (mêmes règles que transform.SQL_VENTES)."""
    from pyspark.sql import functions as F

    mutations = brut.groupBy("id_mutation").agg(
        F.min("date_mutation").alias("date_mutation"),
        F.max("valeur_fonciere").alias("valeur_fonciere"),
        F.countDistinct("numero_disposition").alias("nb_dispositions"),
        F.sum(F.when(F.col("code_type_local") == "4", 1).otherwise(0)).alias("nb_locaux_pro"),
        F.bool_and(F.col("nature_mutation") == "Vente").alias("que_des_ventes"),
    )
    logements = (
        brut.where(F.col("type_local").isin("Appartement", "Maison"))
        .select(
            "id_mutation", "id_parcelle", "lot1_numero", "type_local", "surface",
            "code_commune", "nom_commune", "code_departement", "longitude", "latitude",
        )
        .distinct()
    )
    logement_unique = (
        logements.groupBy("id_mutation")
        .agg(
            F.count("*").alias("n"),
            F.first("type_local").alias("type_bien"),
            F.first("surface").alias("surface"),
            F.first("code_commune").alias("code_commune"),
            F.first("nom_commune").alias("nom_commune"),
            F.first("code_departement").alias("code_departement"),
            F.first("longitude").alias("longitude"),
            F.first("latitude").alias("latitude"),
        )
        .where("n = 1")
        .drop("n")
    )
    return (
        logement_unique.join(mutations, "id_mutation")
        .where(
            F.col("que_des_ventes")
            & (F.col("nb_dispositions") == 1)
            & (F.col("nb_locaux_pro") == 0)
            & F.col("date_mutation").isNotNull()
            & (F.col("valeur_fonciere") > 0)
            & (F.col("surface") > 0)
        )
        .select(
            "id_mutation", "type_bien", "surface", "code_commune", "nom_commune", "code_departement",
            "longitude", "latitude", "date_mutation",
            F.year("date_mutation").alias("annee"),
            "valeur_fonciere",
            (F.col("valeur_fonciere") / F.col("surface")).alias("prix_m2"),
        )
    )


def construire_agregats_spark(
    fichiers: list[Path],
    dossier_sortie: Path,
    *,
    spark=None,
    min_ventes: int = config.MIN_VENTES,
    surface_min: float = config.SURFACE_MIN,
    surface_max: float = config.SURFACE_MAX,
    prix_min: float = config.PRIX_M2_MIN,
    prix_max: float = config.PRIX_M2_MAX,
) -> dict:
    """Équivalent Spark de `transform.construire_agregats` : mêmes fichiers de sortie, même bilan."""
    from pyspark.sql import functions as F

    if not fichiers:
        raise ValueError("Aucun fichier DVF à traiter.")
    dossier_sortie.mkdir(parents=True, exist_ok=True)
    spark = spark or session_locale()

    brut = lire_brut(spark, fichiers).cache()
    candidates = ventes_candidates(brut).cache()
    ventes = candidates.where(
        F.col("surface").between(surface_min, surface_max) & F.col("prix_m2").between(prix_min, prix_max)
    ).cache()
    ventes.createOrReplaceTempView("ventes")

    # Les agrégats tiennent en mémoire : on les écrit avec pandas, ce qui évite
    # d'installer les binaires Hadoop (winutils) que Spark exige sous Windows.
    communes = spark.sql(SQL_COMMUNES.format(min_ventes=min_ventes)).toPandas()
    departements = spark.sql(SQL_DEPARTEMENTS).toPandas()
    communes.to_csv(dossier_sortie / "prix_communes.csv", index=False)
    departements.to_csv(dossier_sortie / "prix_departements.csv", index=False)

    annees = [r.annee for r in ventes.select("annee").distinct().orderBy("annee").collect()]
    derniere = ventes.agg(F.max("date_mutation")).first()[0]
    bilan = {
        "genere_le": date.today().isoformat(),
        "moteur": "spark",
        "source": "Demandes de valeurs foncières géolocalisées (Etalab, data.gouv.fr)",
        "annees": annees,
        "derniere_vente": str(derniere),
        "lignes_brutes": brut.count(),
        "mutations": brut.select("id_mutation").distinct().count(),
        "ventes_un_logement": candidates.count(),
        "ventes_retenues": ventes.count(),
        "communes_publiees": int(communes["code_commune"].nunique()) if len(communes) else 0,
        "regles": {
            "surface_m2": [surface_min, surface_max],
            "prix_m2": [prix_min, prix_max],
            "min_ventes": min_ventes,
        },
    }
    for df in (ventes, candidates, brut):
        df.unpersist()

    (dossier_sortie / "metadata.json").write_text(
        json.dumps(bilan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return bilan

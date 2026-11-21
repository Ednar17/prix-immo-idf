"""Nettoyage des fichiers DVF et calcul des prix médians au m² (DuckDB)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb

from . import config

# DVF contient une ligne par local ET par nature de culture de la parcelle :
# une maison avec jardin apparaît donc plusieurs fois. On lit tout en texte
# puis on convertit explicitement, pour ne dépendre d'aucune détection de type.
SQL_BRUT = """
CREATE OR REPLACE TABLE brut AS
SELECT
    id_mutation,
    TRY_CAST(date_mutation AS DATE)                AS date_mutation,
    numero_disposition,
    nature_mutation,
    TRY_CAST(valeur_fonciere AS DOUBLE)            AS valeur_fonciere,
    code_commune,
    nom_commune,
    code_departement,
    id_parcelle,
    lot1_numero,
    code_type_local,
    type_local,
    TRY_CAST(surface_reelle_bati AS DOUBLE)        AS surface,
    TRY_CAST(longitude AS DOUBLE)                  AS longitude,
    TRY_CAST(latitude AS DOUBLE)                   AS latitude
FROM read_csv({fichiers}, header = true, all_varchar = true, union_by_name = true)
"""

# Une vente est retenue si la mutation :
#   - ne contient que des lignes « Vente » (pas de VEFA, d'échange, d'adjudication) ;
#   - n'a qu'une disposition et aucun local professionnel ;
#   - porte sur UN seul logement (sinon le prix ne peut pas être ramené à une surface).
SQL_VENTES = """
CREATE OR REPLACE TABLE candidates AS
WITH mutations AS (
    SELECT
        id_mutation,
        min(date_mutation)                                   AS date_mutation,
        max(valeur_fonciere)                                 AS valeur_fonciere,
        count(DISTINCT numero_disposition)                   AS nb_dispositions,
        count(*) FILTER (WHERE code_type_local = '4')        AS nb_locaux_pro,
        bool_and(nature_mutation = 'Vente')                  AS que_des_ventes
    FROM brut
    GROUP BY id_mutation
),
logements AS (
    SELECT DISTINCT
        id_mutation, id_parcelle, lot1_numero, type_local, surface,
        code_commune, nom_commune, code_departement, longitude, latitude
    FROM brut
    WHERE type_local IN ('Appartement', 'Maison')
),
logement_unique AS (
    SELECT
        id_mutation,
        any_value(type_local)       AS type_bien,
        any_value(surface)          AS surface,
        any_value(code_commune)     AS code_commune,
        any_value(nom_commune)      AS nom_commune,
        any_value(code_departement) AS code_departement,
        any_value(longitude)        AS longitude,
        any_value(latitude)         AS latitude
    FROM logements
    GROUP BY id_mutation
    HAVING count(*) = 1
)
SELECT
    l.*,
    m.date_mutation,
    year(m.date_mutation)             AS annee,
    m.valeur_fonciere,
    m.valeur_fonciere / l.surface     AS prix_m2
FROM logement_unique l
JOIN mutations m USING (id_mutation)
WHERE m.que_des_ventes
  AND m.nb_dispositions = 1
  AND m.nb_locaux_pro = 0
  AND m.date_mutation IS NOT NULL
  AND m.valeur_fonciere > 0
  AND l.surface > 0;

CREATE OR REPLACE TABLE ventes AS
SELECT * FROM candidates
WHERE surface BETWEEN {surface_min} AND {surface_max}
  AND prix_m2 BETWEEN {prix_min} AND {prix_max};
"""

SQL_COMMUNES = """
WITH centres AS (
    SELECT code_commune, median(longitude) AS longitude, median(latitude) AS latitude
    FROM ventes
    WHERE longitude IS NOT NULL AND latitude IS NOT NULL
    GROUP BY code_commune
),
agregats AS (
    SELECT
        code_commune,
        max(nom_commune)                        AS nom_commune,
        code_departement,
        annee,
        coalesce(type_bien, 'Tous')             AS type_bien,
        count(*)                                AS nb_ventes,
        round(median(prix_m2))::INTEGER         AS prix_m2_median,
        round(quantile_cont(prix_m2, 0.25))::INTEGER AS prix_m2_q1,
        round(quantile_cont(prix_m2, 0.75))::INTEGER AS prix_m2_q3,
        round(median(surface))::INTEGER         AS surface_mediane
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
    coalesce(code_departement, 'Ensemble')  AS code_departement,
    annee,
    coalesce(type_bien, 'Tous')             AS type_bien,
    count(*)                                AS nb_ventes,
    round(median(prix_m2))::INTEGER         AS prix_m2_median
FROM ventes
GROUP BY GROUPING SETS (
    (code_departement, annee, type_bien),
    (code_departement, annee),
    (annee, type_bien),
    (annee)
)
ORDER BY code_departement, annee, type_bien
"""


def _liste_sql(fichiers: list[Path]) -> str:
    chemins = ", ".join("'" + str(f).replace("\\", "/").replace("'", "''") + "'" for f in fichiers)
    return f"[{chemins}]"


def construire_agregats(
    fichiers: list[Path],
    dossier_sortie: Path,
    *,
    min_ventes: int = config.MIN_VENTES,
    surface_min: float = config.SURFACE_MIN,
    surface_max: float = config.SURFACE_MAX,
    prix_min: float = config.PRIX_M2_MIN,
    prix_max: float = config.PRIX_M2_MAX,
) -> dict:
    """Lit les fichiers DVF, écrit les deux CSV agrégés et renvoie le bilan qualité."""
    if not fichiers:
        raise ValueError("Aucun fichier DVF à traiter.")
    dossier_sortie.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(SQL_BRUT.format(fichiers=_liste_sql(fichiers)))
    con.execute(
        SQL_VENTES.format(
            surface_min=surface_min,
            surface_max=surface_max,
            prix_min=prix_min,
            prix_max=prix_max,
        )
    )

    communes = dossier_sortie / "prix_communes.csv"
    departements = dossier_sortie / "prix_departements.csv"
    con.execute(
        f"COPY ({SQL_COMMUNES.format(min_ventes=min_ventes)}) "
        f"TO '{communes.as_posix()}' (HEADER, DELIMITER ',')"
    )
    con.execute(f"COPY ({SQL_DEPARTEMENTS}) TO '{departements.as_posix()}' (HEADER, DELIMITER ',')")

    def un(sql: str):
        return con.execute(sql).fetchone()[0]

    bilan = {
        "genere_le": date.today().isoformat(),
        "source": "Demandes de valeurs foncières géolocalisées (Etalab, data.gouv.fr)",
        "annees": [a for (a,) in con.execute("SELECT DISTINCT annee FROM ventes ORDER BY 1").fetchall()],
        "derniere_vente": str(un("SELECT max(date_mutation) FROM ventes")),
        "lignes_brutes": un("SELECT count(*) FROM brut"),
        "mutations": un("SELECT count(DISTINCT id_mutation) FROM brut"),
        "ventes_un_logement": un("SELECT count(*) FROM candidates"),
        "ventes_retenues": un("SELECT count(*) FROM ventes"),
        "communes_publiees": un(f"SELECT count(DISTINCT code_commune) FROM read_csv('{communes.as_posix()}')"),
        "regles": {
            "surface_m2": [surface_min, surface_max],
            "prix_m2": [prix_min, prix_max],
            "min_ventes": min_ventes,
        },
    }
    con.close()

    (dossier_sortie / "metadata.json").write_text(
        json.dumps(bilan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return bilan

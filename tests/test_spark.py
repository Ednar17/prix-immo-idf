"""La version PySpark doit produire exactement les mêmes chiffres que la version DuckDB."""

import csv

import pytest

pytest.importorskip("pyspark")

from pipeline.spark import construire_agregats_spark, session_locale  # noqa: E402
from pipeline.transform import construire_agregats  # noqa: E402

from test_pipeline import fichier_dvf  # noqa: E402,F401  (fixture partagée)

CLES_BILAN = ["mutations", "ventes_un_logement", "ventes_retenues", "communes_publiees", "annees"]


@pytest.fixture(scope="module")
def spark():
    session = session_locale("tests")
    yield session
    session.stop()


def lire(chemin):
    with open(chemin, encoding="utf-8", newline="") as f:
        return sorted(csv.DictReader(f), key=lambda l: tuple(l.values()))


def normaliser(lignes):
    """pandas écrit les flottants « 48.89 » et les entiers « 5000 » comme DuckDB, sauf les vides."""
    return [{k: (v if v not in ("", "nan") else "") for k, v in l.items()} for l in lignes]


@pytest.mark.parametrize("min_ventes", [1, 5])
def test_spark_et_duckdb_donnent_les_memes_resultats(fichier_dvf, tmp_path, spark, min_ventes):  # noqa: F811
    bilan_duckdb = construire_agregats([fichier_dvf], tmp_path / "duckdb", min_ventes=min_ventes)
    bilan_spark = construire_agregats_spark([fichier_dvf], tmp_path / "spark", spark=spark, min_ventes=min_ventes)

    assert {k: bilan_spark[k] for k in CLES_BILAN} == {k: bilan_duckdb[k] for k in CLES_BILAN}
    for fichier in ("prix_communes.csv", "prix_departements.csv"):
        assert normaliser(lire(tmp_path / "spark" / fichier)) == normaliser(lire(tmp_path / "duckdb" / fichier))

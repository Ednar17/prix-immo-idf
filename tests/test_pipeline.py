"""Tests des règles de nettoyage sur un petit fichier DVF fabriqué à la main."""

import csv
import gzip

import pytest

from pipeline.download import extraire_annees
from pipeline.transform import construire_agregats

COLONNES = [
    "id_mutation", "date_mutation", "numero_disposition", "nature_mutation",
    "valeur_fonciere", "code_commune", "nom_commune", "code_departement",
    "id_parcelle", "lot1_numero", "code_type_local", "type_local",
    "surface_reelle_bati", "nature_culture", "longitude", "latitude",
]


def ligne(mutation, valeur, type_local, surface, *, nature="Vente", parcelle="P1", lot="", culture=""):
    codes = {"Maison": "1", "Appartement": "2", "Dépendance": "3", "Local industriel. commercial ou assimilé": "4"}
    return {
        "id_mutation": mutation,
        "date_mutation": "2024-03-15",
        "numero_disposition": "000001",
        "nature_mutation": nature,
        "valeur_fonciere": valeur,
        "code_commune": "92044",
        "nom_commune": "Levallois-Perret",
        "code_departement": "92",
        "id_parcelle": parcelle,
        "lot1_numero": lot,
        "code_type_local": codes.get(type_local, ""),
        "type_local": type_local,
        "surface_reelle_bati": surface,
        "nature_culture": culture,
        "longitude": "2.28",
        "latitude": "48.89",
    }


LIGNES = [
    # M1 : une maison répétée sur deux natures de culture -> une seule vente à 5 000 €/m².
    ligne("M1", 500000, "Maison", 100, culture="sols"),
    ligne("M1", 500000, "Maison", 100, culture="jardins"),
    # M2 : un appartement et sa cave -> 6 000 €/m², la dépendance ne compte pas.
    ligne("M2", 300000, "Appartement", 50, lot="12"),
    ligne("M2", 300000, "Dépendance", "", lot="40"),
    # M3 : deux appartements vendus ensemble -> prix au m² indéterminable, exclue.
    ligne("M3", 700000, "Appartement", 60, lot="1"),
    ligne("M3", 700000, "Appartement", 45, lot="2"),
    # M4 : vente sur plan (VEFA) -> exclue.
    ligne("M4", 400000, "Appartement", 50, nature="Vente en l'état futur d'achèvement"),
    # M5 : 50 000 €/m² -> valeur aberrante, exclue.
    ligne("M5", 2000000, "Appartement", 40, lot="7"),
    # M6 : appartement vendu avec un local commercial -> exclue.
    ligne("M6", 600000, "Appartement", 60, lot="3"),
    ligne("M6", 600000, "Local industriel. commercial ou assimilé", 80, lot="4"),
    # M7 : un studio -> 4 000 €/m².
    ligne("M7", 80000, "Appartement", 20, lot="9"),
]


@pytest.fixture
def fichier_dvf(tmp_path):
    chemin = tmp_path / "2024_92.csv.gz"
    with gzip.open(chemin, "wt", encoding="utf-8", newline="") as f:
        ecrivain = csv.DictWriter(f, fieldnames=COLONNES)
        ecrivain.writeheader()
        ecrivain.writerows(LIGNES)
    return chemin


def lire(chemin):
    with open(chemin, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_seules_les_ventes_d_un_logement_sont_gardees(fichier_dvf, tmp_path):
    bilan = construire_agregats([fichier_dvf], tmp_path / "out", min_ventes=1)

    assert bilan["mutations"] == 7
    assert bilan["ventes_un_logement"] == 4  # M1, M2, M5, M7
    assert bilan["ventes_retenues"] == 3  # M5 écartée comme aberrante


def test_medianes_par_commune_et_type(fichier_dvf, tmp_path):
    construire_agregats([fichier_dvf], tmp_path / "out", min_ventes=1)
    lignes = {l["type_bien"]: l for l in lire(tmp_path / "out" / "prix_communes.csv")}

    assert set(lignes) == {"Tous", "Appartement", "Maison"}
    assert (lignes["Tous"]["nb_ventes"], lignes["Tous"]["prix_m2_median"]) == ("3", "5000")
    assert (lignes["Appartement"]["nb_ventes"], lignes["Appartement"]["prix_m2_median"]) == ("2", "5000")
    assert (lignes["Maison"]["nb_ventes"], lignes["Maison"]["prix_m2_median"]) == ("1", "5000")
    assert lignes["Tous"]["annee"] == "2024"
    assert lignes["Tous"]["code_commune"] == "92044"


def test_communes_avec_trop_peu_de_ventes_non_publiees(fichier_dvf, tmp_path):
    bilan = construire_agregats([fichier_dvf], tmp_path / "out", min_ventes=5)

    assert lire(tmp_path / "out" / "prix_communes.csv") == []
    assert bilan["communes_publiees"] == 0
    # Le niveau département, lui, reste publié.
    ensemble = [l for l in lire(tmp_path / "out" / "prix_departements.csv") if l["code_departement"] == "Ensemble"]
    assert {l["type_bien"] for l in ensemble} == {"Tous", "Appartement", "Maison"}


def test_extraire_annees_de_l_index():
    html = '<a href="../">../</a><a href="/geo-dvf/latest/csv/2024/">2024/</a><a href="/geo-dvf/latest/csv/2021/">2021/</a>'
    assert extraire_annees(html) == [2021, 2024]

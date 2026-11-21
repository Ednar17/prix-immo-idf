"""Paramètres du pipeline : périmètre, source et règles de nettoyage."""

from pathlib import Path

# Source : « Demandes de valeurs foncières géolocalisées » (Etalab, data.gouv.fr).
# Un fichier CSV compressé par département et par année.
BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"

# Île-de-France.
DEPARTEMENTS = ["75", "77", "78", "91", "92", "93", "94", "95"]

# On garde les N dernières années publiées par la source.
NB_ANNEES = 5

# Règles de nettoyage (voir README, section « Méthode »).
SURFACE_MIN = 9  # m², en dessous ce n'est pas un logement décent au sens légal
SURFACE_MAX = 400  # m², au-delà ce sont surtout des erreurs de saisie ou des biens atypiques
PRIX_M2_MIN = 500  # €/m², en dessous : ventes symboliques, donations déguisées, erreurs
PRIX_M2_MAX = 30_000  # €/m², au-delà : erreurs de surface ou biens d'exception

# Une médiane sur moins de 5 ventes ne veut rien dire : la ligne n'est pas publiée.
MIN_VENTES = 5

RACINE = Path(__file__).resolve().parent.parent
DOSSIER_BRUT = RACINE / "data" / "raw"
DOSSIER_SORTIE = RACINE / "data"

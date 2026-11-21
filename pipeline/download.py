"""Téléchargement des fichiers DVF depuis data.gouv.fr."""

from __future__ import annotations

import re
from pathlib import Path

import requests

from . import config

TIMEOUT = 60
EN_TETES = {"User-Agent": "prix-immo-idf"}


def annees_disponibles(session: requests.Session, base_url: str = config.BASE_URL) -> list[int]:
    """Lit l'index de la source et renvoie les années publiées, triées."""
    reponse = session.get(f"{base_url}/", headers=EN_TETES, timeout=TIMEOUT)
    reponse.raise_for_status()
    return extraire_annees(reponse.text)


def extraire_annees(html: str) -> list[int]:
    """Extrait les années d'une page d'index (liens du type .../csv/2024/)."""
    return sorted({int(a) for a in re.findall(r'href="[^"]*?/?(\d{4})/"', html)})


def telecharger(
    session: requests.Session,
    annee: int,
    departement: str,
    dossier: Path,
    base_url: str = config.BASE_URL,
) -> tuple[Path, bool]:
    """Télécharge un fichier département/année.

    Renvoie (chemin, téléchargé). Le fichier n'est pas retéléchargé si sa taille
    locale est déjà celle annoncée par le serveur.
    """
    url = f"{base_url}/{annee}/departements/{departement}.csv.gz"
    cible = dossier / f"{annee}_{departement}.csv.gz"
    dossier.mkdir(parents=True, exist_ok=True)

    with session.get(url, headers=EN_TETES, timeout=TIMEOUT, stream=True) as reponse:
        reponse.raise_for_status()
        taille = reponse.headers.get("Content-Length")
        if cible.exists() and taille and cible.stat().st_size == int(taille):
            return cible, False

        temporaire = cible.with_suffix(".part")
        with open(temporaire, "wb") as fichier:
            for bloc in reponse.iter_content(chunk_size=1 << 16):
                fichier.write(bloc)
        temporaire.replace(cible)
    return cible, True


def telecharger_tout(
    departements: list[str],
    nb_annees: int,
    dossier: Path,
) -> list[Path]:
    """Télécharge les `nb_annees` dernières années pour chaque département."""
    fichiers = []
    with requests.Session() as session:
        annees = annees_disponibles(session)[-nb_annees:]
        if not annees:
            raise RuntimeError("Aucune année trouvée dans l'index de la source DVF.")
        for annee in annees:
            for departement in departements:
                chemin, nouveau = telecharger(session, annee, departement, dossier)
                etat = "téléchargé" if nouveau else "déjà à jour"
                print(f"  {annee} / {departement} : {etat}")
                fichiers.append(chemin)
    return fichiers

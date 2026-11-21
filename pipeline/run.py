"""Point d'entrée : `python -m pipeline.run`."""

from __future__ import annotations

import argparse
from pathlib import Path

from . import config
from .download import telecharger_tout
from .transform import construire_agregats


def main() -> None:
    parser = argparse.ArgumentParser(description="Prix de l'immobilier par commune (DVF).")
    parser.add_argument("--departements", nargs="+", default=config.DEPARTEMENTS)
    parser.add_argument("--annees", type=int, default=config.NB_ANNEES, help="nombre d'années à garder")
    parser.add_argument("--brut", type=Path, default=config.DOSSIER_BRUT)
    parser.add_argument("--sortie", type=Path, default=config.DOSSIER_SORTIE)
    parser.add_argument(
        "--moteur",
        choices=["duckdb", "spark"],
        default="duckdb",
        help="duckdb (par défaut, rapide sur un portable) ou spark (même résultat, passe à l'échelle)",
    )
    parser.add_argument(
        "--sans-telechargement",
        action="store_true",
        help="recalcule à partir des fichiers déjà présents",
    )
    args = parser.parse_args()

    if args.sans_telechargement:
        fichiers = sorted(args.brut.glob("*.csv.gz"))
    else:
        print("Téléchargement des fichiers DVF")
        fichiers = telecharger_tout(args.departements, args.annees, args.brut)

    print(f"Calcul des prix médians sur {len(fichiers)} fichiers ({args.moteur})")
    if args.moteur == "spark":
        from .spark import construire_agregats_spark

        bilan = construire_agregats_spark(fichiers, args.sortie)
    else:
        bilan = construire_agregats(fichiers, args.sortie)

    print(f"  mutations lues          : {bilan['mutations']:>9,}".replace(",", " "))
    print(f"  ventes d'un logement    : {bilan['ventes_un_logement']:>9,}".replace(",", " "))
    print(f"  ventes retenues         : {bilan['ventes_retenues']:>9,}".replace(",", " "))
    print(f"  communes publiées       : {bilan['communes_publiees']:>9,}".replace(",", " "))
    print(f"Résultats écrits dans {args.sortie}")


if __name__ == "__main__":
    main()

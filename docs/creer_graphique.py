"""Trace le prix médian au m² par département à partir de data/prix_departements.csv."""

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RACINE = Path(__file__).resolve().parent.parent
NOMS = {
    "75": "Paris", "92": "Hauts-de-Seine", "94": "Val-de-Marne", "93": "Seine-Saint-Denis",
    "78": "Yvelines", "95": "Val-d'Oise", "91": "Essonne", "77": "Seine-et-Marne",
    "Ensemble": "Île-de-France",
}

with open(RACINE / "data" / "prix_departements.csv", encoding="utf-8") as f:
    lignes = [l for l in csv.DictReader(f) if l["type_bien"] == "Tous"]

fig, ax = plt.subplots(figsize=(9, 5), dpi=120)
for code, nom in NOMS.items():
    points = sorted((int(l["annee"]), int(l["prix_m2_median"])) for l in lignes if l["code_departement"] == code)
    if not points:
        continue
    annees, prix = zip(*points)
    gras = code in ("75", "Ensemble")
    ax.plot(annees, prix, marker="o", linewidth=2.6 if gras else 1.4, linestyle="--" if code == "Ensemble" else "-", label=nom)
    if code in ("75", "92", "Ensemble", "77"):
        ax.annotate(f"{prix[-1]:,} €".replace(",", " "), (annees[-1], prix[-1]), xytext=(6, 0),
                    textcoords="offset points", va="center", fontsize=8)

ax.set_title("Prix médian au m² des logements anciens, Île-de-France (DVF)")
ax.set_ylabel("€ / m²")
ax.set_xticks(sorted({int(l["annee"]) for l in lignes}))
ax.grid(axis="y", alpha=0.3)
ax.spines[["top", "right"]].set_visible(False)
ax.legend(fontsize=8, frameon=False, loc="upper left", bbox_to_anchor=(1.08, 1))
fig.tight_layout()
fig.savefig(RACINE / "docs" / "prix_departements.png")
print("docs/prix_departements.png")

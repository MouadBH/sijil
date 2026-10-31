# SAMPLE DATA — not real documents

Everything in this folder is **fictitious sample data** for development and tests. The companies don't exist, the ICE numbers are made up, and every PDF is watermarked `SAMPLE / ÉCHANTILLON / عينة`. Never put real customer documents here.

| File | What |
| --- | --- |
| `suppliers.json` | 8 fictitious Moroccan suppliers: French and Arabic names, 15-digit ICE, city, VAT rate (20/14/10/7 %) |
| `invoices/manifest.json` | Ground truth for 12 invoices (6 French, 6 Arabic): number, date, HT, TVA, TTC in MAD |
| `invoices/*.pdf` | The rendered invoices, produced by `generate.py` |
| `fonts/` | Noto Naskh Arabic (SIL Open Font License 1.1, see `OFL.txt`) |

Cases the set deliberately includes, for extraction and review tests:

- `sample-05-fr` and `sample-08-ar` print a **wrong total TTC** (`printed_ttc` in the manifest). HT + TVA ≠ the printed TTC, so they must land in Review.
- `sample-04-ar` uses **Arabic-Indic digits** (`١٢٣`, `٫` decimal separator, date with the month name).
- Arabic invoices with Western digits group thousands with a period (`3.275,50`). French ones use a space (`1 250,00`).

Regenerate the PDFs after editing the manifest:

```bash
uv run python fixtures/generate.py && uv run pytest fixtures
```

#!/usr/bin/env python
"""
CUB Data Pipeline - Extract Latest CUB Data from All States

This script uses extract_latest() on each scraper to automatically
discover and extract the most recent CUB data available.

Output: CSV file in format CUB_COMPILADO_YYYY_MM.csv

Usage:
    python run_all_scrapers.py          # Run all scrapers
    python run_all_scrapers.py SC SP    # Run specific scrapers only
"""

import sys
import csv
from datetime import datetime
from pathlib import Path

# Mapping of state codes to scraper modules
SCRAPERS = {
    "DF": "src.scrapers.df.ScraperDF",
    "ES": "src.scrapers.es.ScraperES",
    "GO": "src.scrapers.go.ScraperGO",
    "MA": "src.scrapers.ma.ScraperMA",
    "MG": "src.scrapers.mg.ScraperMG",
    "MT": "src.scrapers.mt.ScraperMT",
    "PA": "src.scrapers.pa.ScraperPA",
    "PE": "src.scrapers.pe.ScraperPE",
    "PR": "src.scrapers.pr.ScraperPR",
    "RJ": "src.scrapers.rj.ScraperRJ",
    "RS": "src.scrapers.rs.ScraperRS",
    "SC": "src.scrapers.sc.ScraperSC",
    "SP": "src.scrapers.sp.ScraperSP",
}


def get_scraper_class(class_path: str):
    """Dynamically import and return a scraper class."""
    module_path, class_name = class_path.rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def run_scraper(estado: str, headless: bool = True) -> dict:
    """Run a single scraper and return results."""
    if estado not in SCRAPERS:
        return {"estado": estado, "status": "error", "message": f"Unknown state: {estado}"}
    
    try:
        scraper_class = get_scraper_class(SCRAPERS[estado])
        scraper = scraper_class(headless=headless)
        
        print(f"\n{'='*60}")
        print(f"🔍 [{estado}] Extracting latest CUB data...")
        print(f"{'='*60}")
        
        data = scraper.extract_latest()
        
        if data:
            result = {
                "estado": data.estado,
                "status": "success",
                "mes_referencia": data.mes_referencia,
                "ano_referencia": data.ano_referencia,
                "projeto": data.valores[0].projeto if data.valores else None,
                "valor": data.valores[0].valor if data.valores else None,
                "unidade": data.valores[0].unidade if data.valores else "R$/m²",
                "data_extracao": data.data_extracao.isoformat(),
            }
            print(f"✅ [{estado}] Success: R$ {result['valor']:,.2f} ({result['mes_referencia']}/{result['ano_referencia']})")
            return result
        else:
            return {"estado": estado, "status": "failed", "message": "extract_latest returned None"}
            
    except Exception as e:
        print(f"❌ [{estado}] Error: {e}")
        return {"estado": estado, "status": "error", "message": str(e)}


def save_to_csv(results: list, output_dir: Path) -> Path:
    """
    Save results to CSV in the format CUB_COMPILADO_YYYY_MM.csv
    
    Columns: Estado,Mes_Referencia,Ano_Referencia,Projeto,Valor,Unidade,Data_Extracao
    """
    # Filter only successful results
    successful = [r for r in results if r["status"] == "success"]
    
    if not successful:
        print("⚠️ No successful extractions to save.")
        return None
    
    # Determine reference period from the most common month/year
    # (in case different states have different reference months)
    from collections import Counter
    periods = [(r["ano_referencia"], r["mes_referencia"]) for r in successful]
    most_common_period = Counter(periods).most_common(1)[0][0]
    year, month = most_common_period
    
    # Create filename
    filename = f"CUB_COMPILADO_{year}_{month:02d}.csv"
    output_path = output_dir / filename
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write CSV
    fieldnames = ["Estado", "Mes_Referencia", "Ano_Referencia", "Projeto", "Valor", "Unidade", "Data_Extracao"]
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by state name for consistent output
        for r in sorted(successful, key=lambda x: x["estado"]):
            writer.writerow({
                "Estado": r["estado"],
                "Mes_Referencia": r["mes_referencia"],
                "Ano_Referencia": r["ano_referencia"],
                "Projeto": r["projeto"],
                "Valor": r["valor"],
                "Unidade": r["unidade"],
                "Data_Extracao": r["data_extracao"],
            })
    
    return output_path


def main():
    print("=" * 70)
    print("📊 CUB Data Pipeline - Extract Latest Data")
    print(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        states = [s.upper() for s in sys.argv[1:]]
    else:
        states = list(SCRAPERS.keys())
    
    print(f"\n📋 States to process: {states}")
    
    results = []
    for estado in states:
        result = run_scraper(estado, headless=True)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    
    success_count = sum(1 for r in results if r["status"] == "success")
    total = len(results)
    
    print(f"\n{'Estado':<8} {'Status':<10} {'Valor':<15} {'Referência'}")
    print("-" * 60)
    
    for r in sorted(results, key=lambda x: x["estado"]):
        if r["status"] == "success":
            valor_str = f"R$ {r['valor']:,.2f}"
            ref_str = f"{r['mes_referencia']}/{r['ano_referencia']}"
        else:
            valor_str = "-"
            ref_str = r.get("message", "Unknown error")[:30]
        
        status_icon = "✅" if r["status"] == "success" else "❌"
        print(f"{r['estado']:<8} {status_icon} {r['status']:<8} {valor_str:<15} {ref_str}")
    
    print("-" * 60)
    print(f"\n📈 Success rate: {success_count}/{total} ({100*success_count/total:.0f}%)")
    
    # Save results to CSV
    output_dir = Path("data/output")
    csv_path = save_to_csv(results, output_dir)
    
    if csv_path:
        print(f"\n💾 Results saved to: {csv_path}")
        
        # Show CSV preview
        print(f"\n📄 CSV Preview:")
        print("-" * 60)
        with open(csv_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < 5:  # Show first 5 lines
                    print(line.strip())
        if success_count > 4:
            print(f"   ... and {success_count - 4} more rows")


if __name__ == "__main__":
    main()

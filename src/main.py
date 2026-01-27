"""
CUB Data Pipeline - Main Orchestrator

Orchestrates the execution of all state scrapers and compiles
the extracted data into a unified CSV file.

Features:
- Multi-state scraping (SC, SP, PR, MG, RS, GO, RJ, ES, PE, DF, MT, MA, PA) + INCC-M (BR)
- Incremental data loading (Upsert strategy)
- Deduplication by State + Month + Year + Project
- Automatic month calculation if not specified
- State filtering via --states argument
- CI/CD ready with --auto flag for GitHub Actions

Usage:
    python -m src.main [month] [year] [--states STATE1 STATE2 ...]
    python -m src.main --auto              # CI/CD mode: auto-calculate previous month
    
Examples:
    python -m src.main                     # Uses previous month, all states
    python -m src.main 12 2025             # December 2025, all states
    python -m src.main 12 2025 --states GO # December 2025, only GO
    python -m src.main 12 2025 -s GO RS    # December 2025, GO and RS only
    python -m src.main --auto              # Auto mode for CI/CD pipelines

Exit Codes:
    0 - Success (at least one state extracted)
    1 - Partial failure (some states failed, but others succeeded)
    2 - Complete failure (no data extracted from any source)
"""

import sys
import os
import logging
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Ensure src is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scrapers.sc import ScraperSC
from src.scrapers.sp import ScraperSP
from src.scrapers.pr import ScraperPR
from src.scrapers.mg import ScraperMG
from src.scrapers.rs import ScraperRS
from src.scrapers.go import ScraperGO
from src.scrapers.rj import ScraperRJ
from src.scrapers.es import ScraperES
from src.scrapers.pe import ScraperPE
from src.scrapers.df import ScraperDF
from src.scrapers.mt import ScraperMT
from src.scrapers.ma import ScraperMA
from src.scrapers.pa import ScraperPA
from src.scrapers.incc import ScraperINCC
from src.utils.helpers import get_logger, get_data_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = get_logger("main")


def get_reference_month() -> tuple[int, int]:
    """
    Calculate the reference month (previous month by default).
    
    CUB data is typically published for the previous month,
    so if today is January 14, we look for December data.
    
    Returns:
        Tuple of (month, year)
    """
    today = datetime.now()
    # Go back to the first of current month, then subtract 1 day
    first_of_month = today.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    
    return last_month.month, last_month.year


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments using argparse.
    
    Supports two modes:
    1. Manual mode: Specify month and year directly
    2. Auto mode (--auto): Automatically calculate previous month (for CI/CD)
    
    Returns:
        Namespace with month, year, states, and auto flag
    """
    parser = argparse.ArgumentParser(
        description="CUB Data Pipeline - Extract CUB data from state Sinduscon websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.main                      # Previous month, all states
  python -m src.main 12 2025              # December 2025, all states
  python -m src.main 12 2025 --states GO  # December 2025, only GO
  python -m src.main 12 2025 -s GO RS MG  # December 2025, GO, RS, and MG only
  python -m src.main --auto               # CI/CD mode: auto-calculate previous month

Exit Codes:
  0 - Success (at least one state extracted successfully)
  1 - Partial failure (some states failed, but others succeeded)
  2 - Complete failure (no data extracted from any source)
        """
    )
    
    parser.add_argument(
        "month",
        type=int,
        nargs="?",
        default=None,
        help="Reference month (1-12). Defaults to previous month."
    )
    
    parser.add_argument(
        "year",
        type=int,
        nargs="?",
        default=None,
        help="Reference year (e.g., 2025). Defaults to current/previous year."
    )
    
    parser.add_argument(
        "-s", "--states",
        type=str,
        nargs="+",
        default=None,
        metavar="STATE",
        help="Filter to run only specific states (e.g., GO RS MG). Case-insensitive."
    )
    
    parser.add_argument(
        "-a", "--auto",
        action="store_true",
        help="CI/CD mode: automatically calculate the previous month as target. "
             "Ignores manual month/year arguments when set."
    )
    
    args = parser.parse_args()
    
    # Auto mode: always use calculated previous month
    if args.auto:
        args.month, args.year = get_reference_month()
        logger.info("🤖 AUTO MODE: Target month calculated automatically")
    else:
        # Handle month/year defaults for manual mode
        if args.month is None or args.year is None:
            default_month, default_year = get_reference_month()
            if args.month is None:
                args.month = default_month
            if args.year is None:
                args.year = default_year
    
    # Validate month
    if not 1 <= args.month <= 12:
        parser.error(f"Month must be between 1 and 12, got {args.month}")
    
    # Validate year
    if not 2000 <= args.year <= 2100:
        parser.error(f"Year must be between 2000 and 2100, got {args.year}")
    
    # Normalize states to uppercase
    if args.states:
        args.states = [s.upper() for s in args.states]
    
    return args


def flatten_cub_data(data) -> list[dict]:
    """
    Flatten CUBData object into a list of row dictionaries.
    
    Args:
        data: CUBData object
    
    Returns:
        List of dictionaries, one per valor
    """
    rows = []
    for item in data.valores:
        row = {
            "Estado": data.estado,
            "Mes_Referencia": data.mes_referencia,
            "Ano_Referencia": data.ano_referencia,
            "Projeto": item.projeto,
            "Valor": item.valor,
            "Unidade": item.unidade,
            "Data_Extracao": data.data_extracao.isoformat()
        }
        rows.append(row)
    return rows


def main() -> int:
    """
    Main orchestration function.
    
    Returns:
        int: Exit code
            - 0: Success (at least one state extracted)
            - 1: Partial failure (some failed, but data was saved)
            - 2: Complete failure (no data extracted)
    """
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("🚀 CUB Data Pipeline - Starting Extraction")
    logger.info(f"⏰ Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # 1. Parse arguments
    args = parse_arguments()
    month = args.month
    year = args.year
    is_auto_mode = getattr(args, 'auto', False)
    
    logger.info(f"📅 Target period: {month:02d}/{year}")
    if is_auto_mode:
        logger.info("🤖 Running in AUTO mode (CI/CD)")
    
    # 2. Setup output path
    output_dir = get_data_path("output")
    csv_filename = f"CUB_COMPILADO_{year}_{month:02d}.csv"
    output_path = output_dir / csv_filename
    
    logger.info(f"📁 Output file: {output_path}")
    
    # 3. Initialize ALL scrapers
    all_scrapers = [
        ScraperSC(headless=True),
        ScraperSP(headless=True),
        ScraperPR(headless=True),
        ScraperMG(headless=True),
        ScraperRS(headless=True),
        ScraperGO(headless=True),
        ScraperRJ(headless=True),
        ScraperES(headless=True),
        ScraperPE(headless=True),
        ScraperDF(headless=True),
        ScraperMT(headless=True),
        ScraperMA(headless=True),
        ScraperPA(headless=True),
        ScraperINCC(headless=True),
    ]
    
    # 4. Filter scrapers based on --states argument
    if args.states:
        scrapers = [s for s in all_scrapers if s.estado in args.states]
        
        # Validate that requested states exist
        available_states = {s.estado for s in all_scrapers}
        requested_states = set(args.states)
        unknown_states = requested_states - available_states
        
        if unknown_states:
            logger.warning(f"⚠️ Unknown states ignored: {unknown_states}")
            logger.info(f"Available states: {sorted(available_states)}")
        
        if not scrapers:
            logger.error(f"❌ No valid scrapers selected. Available: {sorted(available_states)}")
            return 2
        
        logger.info(f"🎯 Filtered to selected states: {[s.estado for s in scrapers]}")
    else:
        scrapers = all_scrapers
        logger.info(f"🌐 Running ALL scrapers: {[s.estado for s in scrapers]}")
    
    # 5. Run scrapers and collect data
    new_rows = []
    successful = []
    failed = []
    delayed = []  # Renamed from 'skipped' for clarity
    
    total_scrapers = len(scrapers)
    for idx, scraper in enumerate(scrapers, 1):
        estado = scraper.estado
        logger.info(f"\n[{idx}/{total_scrapers}] [{estado}] Checking availability...")
        
        try:
            if scraper.check_availability(month, year):
                logger.info(f"[{estado}] ✅ Data available. Extracting...")
                data = scraper.extract(month, year)
                
                # Flatten and collect
                rows = flatten_cub_data(data)
                new_rows.extend(rows)
                
                logger.info(f"[{estado}] ✅ Extracted {len(rows)} values")
                successful.append(estado)
            else:
                logger.warning(f"[{estado}] ⏳ Data NOT available for {month:02d}/{year} (DELAYED)")
                delayed.append(estado)
                
        except Exception as e:
            logger.error(f"[{estado}] ❌ Extraction failed: {e}")
            failed.append(estado)
            continue
    
    # 6. Summary of scraping phase (CI/CD friendly format)
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 SCRAPING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Processed:      {successful if successful else 'None'}")
    logger.info(f"⏳ Delayed/Not Found: {delayed if delayed else 'None'}")
    logger.info(f"❌ Failed:         {failed if failed else 'None'}")
    logger.info(f"📈 New rows:       {len(new_rows)}")
    logger.info(f"⏱️  Duration:       {duration:.1f}s")
    logger.info("=" * 60)
    
    # Generate JSON summary for GitHub Actions (can be parsed by subsequent steps)
    summary = {
        "target_month": month,
        "target_year": year,
        "auto_mode": is_auto_mode,
        "successful": successful,
        "delayed": delayed,
        "failed": failed,
        "new_rows": len(new_rows),
        "duration_seconds": round(duration, 1),
        "timestamp": end_time.isoformat()
    }
    logger.info(f"📋 JSON Summary: {json.dumps(summary)}")
    
    # 7. Handle case where no new data was extracted
    if not new_rows:
        logger.warning("⚠️ No new data extracted from any source.")
        if output_path.exists():
            logger.info(f"📁 Existing file preserved: {output_path}")
        
        # Return appropriate exit code
        if failed and not delayed:
            return 2  # Complete failure
        return 0  # All delayed is not a failure (data just not published yet)
    
    # 8. Load existing data (Upsert strategy)
    if output_path.exists():
        logger.info(f"📂 Found existing file. Loading for merge...")
        try:
            existing_df = pd.read_csv(output_path)
            logger.info(f"📊 Existing data: {len(existing_df)} rows")
        except Exception as e:
            logger.warning(f"⚠️ Could not read existing file: {e}. Creating new.")
            existing_df = pd.DataFrame()
    else:
        logger.info("📝 No existing file. Creating new...")
        existing_df = pd.DataFrame()
    
    # 9. Create new DataFrame and merge
    new_df = pd.DataFrame(new_rows)
    
    if not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        logger.info(f"🔗 Combined: {len(combined_df)} rows before deduplication")
    else:
        combined_df = new_df
    
    # 10. Deduplicate
    # Unique key: State + Month + Year + Project Code
    # keep='last' ensures re-runs overwrite old data with fresh extractions
    before_count = len(combined_df)
    combined_df.drop_duplicates(
        subset=['Estado', 'Mes_Referencia', 'Ano_Referencia', 'Projeto'],
        keep='last',
        inplace=True
    )
    after_count = len(combined_df)
    
    if before_count > after_count:
        logger.info(f"🔄 Removed {before_count - after_count} duplicate rows (kept newest)")
    
    # 11. Sort for consistent output
    combined_df.sort_values(
        by=['Estado', 'Projeto'],
        inplace=True
    )
    
    # 12. Save
    if combined_df.empty:
        logger.warning("⚠️ Combined dataframe is empty. Nothing to save.")
        return 2
    
    combined_df.to_csv(output_path, index=False)
    logger.info(f"\n✅ Successfully saved {len(combined_df)} rows to {output_path}")
    
    # 13. Final summary with exit code determination
    logger.info("\n" + "=" * 60)
    logger.info("🏁 PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"📁 Output: {output_path}")
    logger.info(f"🗺️  States: {combined_df['Estado'].unique().tolist()}")
    logger.info(f"📊 Total rows: {len(combined_df)}")
    
    # Determine exit code
    if failed:
        logger.warning(f"⚠️ Exiting with code 1 (partial failure: {len(failed)} states failed)")
        return 1
    
    logger.info("✅ Exiting with code 0 (success)")
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


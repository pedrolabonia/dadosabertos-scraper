import asyncio
import httpx
import math
import os
import orjson
import argparse
from typing import Optional

# --- Configuration ---
BASE_URL = "https://dados.gov.br/api/publico/conjuntos-dados/buscar"
LICENSES = ["cc-by", "cc-zero", "odc-odbl", "odc-pddl"]

def get_total_records(timeout: int, license_filter: str) -> Optional[int]:
    """Fetches the total number of records for a specific license filter."""
    filter_name = f"'{license_filter}'" if license_filter else "'None'"
    print(f"🔍 Fetching total for license: {filter_name}...")
    params = {"offset": 0, "tamanhoPagina": 1, "dadosAbertos": "true"}
    if license_filter:
        params["licenca"] = license_filter
    
    try:
        with httpx.Client() as client:
            response = client.get(BASE_URL, params=params, timeout=timeout)
            response.raise_for_status()
            data = orjson.loads(response.content)
            total_records = data["totalRegistros"]
            print(f"✅ Found {total_records} records for this license.")
            return total_records
    except (httpx.RequestError, orjson.JSONDecodeError, KeyError) as e:
        print(f"❌ Error fetching total for license {filter_name}: [{type(e).__name__}] {e}")
        return None

async def fetch_and_save(
    client: httpx.AsyncClient,
    api_offset: int,
    filename: str,
    args: argparse.Namespace,
    license_filter: str,
    semaphore: asyncio.Semaphore
):
    """
    Fetches a single page of data with a retry mechanism.
    """
    async with semaphore:
        params = {"offset": api_offset, "tamanhoPagina": args.page_size, "dadosAbertos": "true"}
        if license_filter:
            params["licenca"] = license_filter
        
        filepath = os.path.join(args.output_dir, filename)
        filter_name = license_filter if license_filter else "None"
        print(f"Requesting (Lic: {filter_name}, API Offset: {api_offset}) -> Saving as {filename}...")
        
        for attempt in range(args.retries):
            try:
                response = await client.get(BASE_URL, params=params, timeout=args.timeout)
                response.raise_for_status()
                
                with open(filepath, "wb") as f:
                    f.write(response.content)
                print(f"✅ Success: Saved {filename}")
                return  # Exit the function on success
            except Exception as e:
                print(f"❌ Error for {filename} (Attempt {attempt + 1}/{args.retries}): [{type(e).__name__}] {e}")
                if attempt < args.retries - 1:
                    print(f"Retrying in {args.retry_delay} seconds...")
                    await asyncio.sleep(args.retry_delay)
                else:
                    print(f"❌ Failed to download {filename} after {args.retries} attempts.")

async def main():
    """Core asynchronous logic for the scraper."""
    parser = argparse.ArgumentParser(
        description="Scrape datasets from dados.gov.br by filtering through licenses.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--page_size', type=int, default=500, help="Records per request.")
    parser.add_argument('--concurrency', type=int, default=10, help="Max parallel requests.")
    parser.add_argument('--timeout', type=int, default=90, help="Request timeout in seconds.")
    parser.add_argument('--output_dir', type=str, default="scraped_data", help="Directory for all output JSON files.")
    parser.add_argument('--retries', type=int, default=3, help="Number of times to retry a failed download.")
    parser.add_argument('--retry_delay', type=int, default=5, help="Seconds to wait between retries.")
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Global counter for file naming, starts at 1 for 1-based indexing
    file_naming_start_index = 1

    for license_filter in LICENSES:
        filter_name = license_filter if license_filter else "None"
        print(f"\n--- Starting scrape for license: '{filter_name}' ---")
        
        total_records_for_license = get_total_records(timeout=args.timeout, license_filter=license_filter)
        
        if not total_records_for_license:
            print("No records to fetch, skipping.")
            continue
            
        if total_records_for_license > 9999:
             print(f"⚠️ Warning: Total records ({total_records_for_license}) for license '{filter_name}' exceeds API limit.")

        semaphore = asyncio.Semaphore(args.concurrency)
        num_pages = math.ceil(total_records_for_license / args.page_size)
        
        async with httpx.AsyncClient() as client:
            tasks = []
            for i in range(num_pages):
                # API offset is always 0-based and relative to the current license filter
                api_offset = i * args.page_size
                
                # Calculate start and end for the filename using the global counter
                start_index = file_naming_start_index + api_offset
                records_on_this_page = min(args.page_size, total_records_for_license - api_offset)
                end_index = start_index + records_on_this_page - 1
                
                filename = f"{start_index}-{end_index}.json"
                
                task = fetch_and_save(
                    client, api_offset, filename, args, license_filter, semaphore
                )
                tasks.append(task)
            
            if tasks:
                print(f"🚀 Starting to download {len(tasks)} files for license '{filter_name}'...")
                await asyncio.gather(*tasks)
        
        # Increment the global file counter by the total records found in this category
        file_naming_start_index += total_records_for_license
        print(f"--- Finished license: '{filter_name}'. Next file will start at index {file_naming_start_index}. ---")

    print("\n🎉 All scraping tasks are complete!")

def cli_entrypoint():
    """Synchronous entry point that safely runs the main async function."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")

if __name__ == "__main__":
    cli_entrypoint()

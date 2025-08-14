# Dados.gov.br Scraper

This script efficiently downloads all dataset **metadata** from Brazil's open data portal, [dados.gov.br](https://dados.gov.br/).

The metadata has many goodies such as direct links to the dataset downloads, file formats, tags, full description, etc.

It works around the API's 9999-item pagination limit by sequentially scraping smaller categories based on license type (`cc-by`, `cc-zero`, etc.). This ensures a successful download of (almost) all available metadata - scrapes 11600 out of 14666 total datasets at the time of writing this readme.

## How to Run

1.  **Prerequisites**:
    * Python 3.11+
    * [uv](https://github.com/astral-sh/uv)

2.  **Installation**:
    Clone this repository, and use `uv sync` to create the venv and install the necessary packages
    ```bash
      git clone https://github.com/pedrolabonia/dadosabertos-scraper.git
      cd dadosabertos-scraper
      uv sync
    ```

3.  **Execution**:
    Run the scraper using the `scrape` command. All files will be saved to a single output directory.

    * **Run with defaults(recommended):**
        ```bash
        uv run scrape
        ```
    * **Run with custom arguments:**
        ```bash
        uv run scrape --page_size 500 --concurrency 20 --output_dir ./my_data
        ```
    * **See all options:**
        ```bash
        uv run scrape --help
        ```

## Command-Line Arguments
Recommended a 90s timeout since the API can take a while.

| Argument      | Default        | Description                                     |
| :------------ | :------------- | :---------------------------------------------- |
| `--page_size`   | `500`          | Records to fetch per API request.               |
| `--concurrency` | `10`           | Max number of parallel download requests.       |
| `--timeout`     | `90`           | Timeout in seconds for each HTTP request.       |
| `--output_dir`  | `scraped_data` | Directory to save the output `.json` files.     |

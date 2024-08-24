from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict

import arxiv
import requests  # type: ignore
from langchain_community.document_loaders import PyPDFLoader
from loguru import logger
from ratelimit import limits, sleep_and_retry

# Configuration for rate limiting
THREE_SECONDS = 3
ONE_REQUEST = 1


def fetch_papers(search_query: str, max_results: int) -> List[str]:
    """
    Fetches a list of paper URLs from the arXiv database based on a search query.

    Args:
        search_query (str): The query term used for searching papers in arXiv.
                            It can include various search criteria like title, author, abstract, etc.
        max_results (int): The maximum number of paper URLs to fetch.

    Returns:
        list: A list of URLs, each pointing to a PDF of a research paper.
              The number of URLs in the list is up to the specified 'max_results'.
    """
    client = arxiv.Client()

    search = arxiv.Search(
        query=search_query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate,
    )

    search_results = list(client.results(search))
    paper_urls = [result.pdf_url for result in search_results]
    return paper_urls


@sleep_and_retry  # type: ignore
@limits(calls=ONE_REQUEST, period=THREE_SECONDS)  # type: ignore
def download_paper(url: str, filepath: Path) -> None:
    """
    Downloads a paper from a specified URL and saves it to a given file path.

    Args:
        url (str): The URL from where the paper will be downloaded.
        filepath (Path or str): The file path (including filename) where the paper will be saved.

    This function attempts to download a paper from the given URL. If successful, the paper
    is saved to the specified filepath. If the download fails due to network issues or
    HTTP errors, it logs an appropriate error message.

    The function uses the 'requests' library for HTTP requests and 'loguru' for logging.
    """
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

        with open(filepath, "wb") as f:
            f.write(response.content)
        logger.info(f"Downloaded Paper {url}")

    except requests.HTTPError as http_err:
        logger.error(f"HTTP error occurred while downloading Paper {url}: {http_err}")
    except requests.ConnectionError as conn_err:
        logger.error(
            f"Connection error occurred while downloading Paper {url}: {conn_err}"
        )
    except requests.Timeout as timeout_err:
        logger.error(
            f"Timeout error occurred while downloading Paper {url}: {timeout_err}"
        )
    except requests.RequestException as req_err:
        # For any other requests-related exceptions
        logger.error(f"Error occurred while downloading Paper {url}: {req_err}")
    except IOError as io_err:
        # Handle file I/O errors
        logger.error(f"File I/O error while saving Paper {url}: {io_err}")


def download_papers_from_arxiv(
        output_directory: Path, search_query: str, max_results: int
) -> None:
    """
    Downloads a specified number of papers from arXiv based on a given search query.

    Args:
        output_directory (Path): The directory where downloaded papers will be stored.
        search_query (str): The query term to search for papers.
        max_results (int): The maximum number of papers to download.

    This function fetches paper URLs using the search query and downloads the papers
    concurrently using multiple threads.
    """
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    logger.info("Starting paper download...")
    paper_urls = fetch_papers(search_query, max_results)

    # Using ThreadPoolExecutor for concurrent downloads
    with ThreadPoolExecutor(max_workers=3) as executor:
        for url in paper_urls:
            filename = url.split("/")[-1] + ".pdf"
            executor.submit(download_paper, url, output_directory / filename)
    logger.info("Download complete.")


def ingest_documents(input_directory: Path) -> List[Dict[str, str]]:
    """
    Ingests a specified number of PDF documents from a given directory, extracting their content.

    This function searches for PDF files in the specified directory, up to a maximum number specified by `max_docs`.
    It processes each PDF file, extracting its content and metadata using the PyPDFLoader from the langchain package.
    The extracted content is returned as a list of dictionaries, each containing the source and text of a page from the PDFs.

    Args:
        input_directory (Path): The directory path where the PDF files are located.
        max_docs (int, optional): The maximum number of PDF documents to process. Defaults to 1.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each containing 'source' and 'text' keys corresponding to the
                              metadata and content of each page in the processed PDF documents.

    Note:
        This function assumes that the `langchain` package and its `PyPDFLoader` class are available in your environment.
        If not, you'll need to install or import the necessary libraries.
    """
    nb_files_processed = 0
    processed_files_path = Path("_processed_files.txt")
    if not processed_files_path.exists():
        processed_files_path.touch()

    # Read the processed files before the loop
    processed_files = processed_files_path.read_text().split("\n")

    pdf_files = list(input_directory.rglob("*.pdf"))
    langchain_documents = []

    # Process each PDF file found
    for document in pdf_files:
        if str(document) in processed_files:
            continue

        loader = PyPDFLoader(str(document))
        data = loader.load()
        for item in data:
            langchain_documents.append(
                {"source": item.metadata["source"], "text": item.page_content}
            )
        nb_files_processed += 1

        # Add the processed file to the record
        with open(processed_files_path, "a") as f:
            f.write(f"{str(document)}\n")

    # Log the number of documents
    logger.info(
        f"{nb_files_processed} files have been processed. {len(langchain_documents)} documents have been created.")

    return langchain_documents


if __name__ == "__main__":
    output_directory = Path.cwd() / "data" / "raw"
    output_directory.mkdir(parents=True, exist_ok=True)

    search_query = "large language model"
    max_results = 2

    download_papers_from_arxiv(output_directory, search_query, max_results)

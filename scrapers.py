"""
Web scrapers for various job boards.
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright
import re
# from simple_crawler import batch_crawl_company_domains


# Keywords to filter jobs in description
FILTER_KEYWORDS = ["engineer", "developer", "software", "full stack", "frontend", "backend", "programmer", "coding",
                    "data scientist", "data analyst", "analytics", "business intelligence", "machine learning", "ai",
                    "product manager", "ux", "ui", "designer", "product owner", "product design",
                    "devops", "sre", "site reliability", "infrastructure", "cloud", "kubernetes", "aws"]


def filter_description(description: str) -> bool:
    """Filter description for specific keywords."""
    if not description:
        return False
    desc_lower = description.lower()
    return any(keyword in desc_lower for keyword in FILTER_KEYWORDS)


def extract_domain_from_url(url: str) -> str:
    """Extract clean domain from URL."""
    if not url:
        return ""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.netloc:
        domain = parsed.netloc
        # Add scheme
        scheme = parsed.scheme or "https"
        return f"{scheme}://{domain}"
    return ""


def extract_company_from_text(full_text: str, title: str) -> str:
    """Extract company name from full text using | separator."""
    # Split by | and take the last part as company
    if '|' in full_text:
        parts = full_text.split('|')
        if len(parts) > 1:
            # Company is usually after the pipe
            company = parts[-1].strip()
            if len(company) > 0 and len(company) < 100:
                return company

    # Fallback: extract from title
    at_match = re.search(r'\bat\s+([A-Z][A-Za-z0-9\s&\-\.]+?)(?:\s*\(|\.|$)', title)
    if at_match:
        return at_match.group(1).strip()

    dash_match = re.search(r'\-\s*([A-Z][A-Za-z0-9\s&\-\.]+)$', title)
    if dash_match:
        return dash_match.group(1).strip()

    return "Unknown"


SERPAPI_KEY = "31baa192d94faa0fef6a1b05b0d4788e197e9b5d050a8b306596218f09270aa1"


async def scrape_arbeitnow(session, progress_callback=None) -> List[Dict]:
    """Scrape jobs from Arbeitnow API with pagination."""
    jobs = []
    page = 1

    try:
        while True:
            url = f"https://www.arbeitnow.com/api/job-board-api?page={page}"

            if progress_callback:
                progress_callback({
                    "type": "progress",
                    "message": f"Fetching Arbeitnow page {page}...",
                    "page": page,
                    "total": len(jobs)
                })

            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 429:
                    # Rate limit hit - wait and retry
                    if progress_callback:
                        progress_callback({
                            "type": "progress",
                            "message": f"Rate limited! Waiting 5 seconds before retrying page {page}...",
                            "page": page,
                            "total": len(jobs)
                        })
                    await asyncio.sleep(5)
                    # Retry the same page
                    continue

                if response.status != 200:
                    if progress_callback:
                        progress_callback({
                            "type": "progress",
                            "message": f"Error fetching page {page}: status {response.status}",
                            "page": page,
                            "total": len(jobs)
                        })
                    break

                data = await response.json()
                page_jobs = data.get("data", [])

                if not page_jobs:
                    if progress_callback:
                        progress_callback({
                            "type": "progress",
                            "message": f"No more jobs found. Finished at page {page-1}",
                            "page": page-1,
                            "total": len(jobs)
                        })
                    break

                for job in page_jobs:
                    title = job.get("title", "N/A")
                    company = job.get("company_name") or job.get("company") or "Unknown"
                    job_url = job.get("url", "") or ""

                    # Parse description HTML to extract description
                    description_html = job.get("description", "") or ""
                    description = ""
                    if description_html:
                        soup = BeautifulSoup(description_html, 'html.parser')
                        description = soup.get_text(strip=True, separator=' ')[:500]

                    # Location
                    location = job.get("location", "") or ""

                    # Employment type
                    job_type = ""
                    if job.get("job_types"):
                        job_type = ", ".join(job["job_types"]) if job["job_types"] else ""

                    # Remote
                    remote = "On-site"
                    if job.get("remote"):
                        remote = "Remote"
                    if "hybrid" in description.lower():
                        remote = "Hybrid"

                    # Date posted
                    date_posted = "N/A"
                    if job.get("created_at"):
                        created_at = job["created_at"]
                        if isinstance(created_at, int):
                            date_posted = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d")

                    jobs.append({
                        "job_title": title,
                        "company": company,
                        "company_domain": "",
                        "location": location,
                        "description": description,
                        "job_url": job_url,
                        "date_posted": date_posted,
                        "employment_type": job_type,
                        "salary": "",
                        "remote": remote,
                        "status": "Active"
                    })

                if progress_callback:
                    progress_callback({
                        "type": "progress",
                        "message": f"Page {page}: {len(page_jobs)} jobs fetched",
                        "page": page,
                        "page_jobs": len(page_jobs),
                        "total": len(jobs)
                    })

                # Check if there's a next page
                links = data.get("links", {})
                if not links.get("next"):
                    break

                # Add delay between requests to avoid rate limiting
                await asyncio.sleep(2)

                page += 1

    except Exception as e:
        if progress_callback:
            progress_callback({
                "type": "error",
                "message": f"Error scraping Arbeitnow: {e}"
            })

    return jobs


async def scrape_berlin_startup_jobs(session) -> List[Dict]:
    """Scrape jobs from Berlin Startup Jobs using Playwright."""
    jobs = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto("https://berlinstartupjobs.com/engineering/", timeout=30000)
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(3)

            soup = BeautifulSoup(await page.content(), 'lxml')

            for div in soup.find_all('div', class_='bjs-jlid__meta'):
                h4 = div.find('h4')
                if h4:
                    title = h4.get_text(strip=True)
                    full_text = div.get_text(separator='|', strip=True)
                    company = extract_company_from_text(full_text, title)
                    job_url = ""

                    jobs.append({
                        "job_title": title,
                        "company": company,
                        "company_domain": "",
                        "location": "Berlin",
                        "description": title,
                        "job_url": job_url,
                        "date_posted": datetime.now().strftime("%Y-%m-%d"),
                        "employment_type": "",
                        "salary": "",
                        "status": "Active"
                    })

                    if len(jobs) >= 50:
                        break

            await browser.close()

    except Exception as e:
        print(f"Error scraping Berlin Startup Jobs: {e}")

    return jobs


async def scrape_job4good(session) -> List[Dict]:
    """Scrape jobs from Job4Good using Playwright."""
    jobs = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto("https://www.job4good.it/annunci-di-lavoro/", timeout=30000)
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(3)

            soup = BeautifulSoup(await page.content(), 'lxml')

            for item in soup.find_all(['div', 'article', 'li']):
                title_elem = item.find(['h2', 'h3', 'h4'])
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if len(title) > 15:
                        skip = ['chi siamo', 'privacy', 'menu', 'candidati', 'aziende', 'accedi', 'home', 'info', 'servizi', 'risorse', 'formazione', 'contatti', 'job4good', 'annunci']
                        if not any(s in title.lower() for s in skip):
                            full_text = item.get_text(separator='|', strip=True)
                            company = extract_company_from_text(full_text, title)
                            job_url = ""

                            jobs.append({
                                "job_title": title,
                                "company": company,
                                "company_domain": "",
                                "location": "Italy",
                                "description": title,
                                "job_url": job_url,
                                "date_posted": datetime.now().strftime("%Y-%m-%d"),
                                "employment_type": "",
                                "salary": "",
                                "status": "Active"
                            })

                            if len(jobs) >= 30:
                                break

            await browser.close()

    except Exception as e:
        print(f"Error scraping Job4Good: {e}")

    return jobs


async def scrape_turijobs(session) -> List[Dict]:
    """Scrape jobs from TuriJobs using Playwright."""
    jobs = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto("https://www.turijobs.com/ofertas-trabajo", timeout=30000)
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(3)

            soup = BeautifulSoup(await page.content(), 'lxml')

            for item in soup.find_all(['div', 'article', 'li']):
                title_elem = item.find(['h2', 'h3', 'h4'])
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if len(title) > 15:
                        skip = ['inicia', 'registra', 'blog', 'empleos', 'turijobs', 'ofertas', 'empresa']
                        if not any(s in title.lower() for s in skip):
                            full_text = item.get_text(separator='|', strip=True)
                            company = extract_company_from_text(full_text, title)
                            job_url = ""

                            jobs.append({
                                "job_title": title,
                                "company": company,
                                "company_domain": "",
                                "location": "Spain",
                                "description": title,
                                "job_url": job_url,
                                "date_posted": datetime.now().strftime("%Y-%m-%d"),
                                "employment_type": "",
                                "salary": "",
                                "status": "Active"
                            })

                            if len(jobs) >= 30:
                                break

            await browser.close()

    except Exception as e:
        print(f"Error scraping TuriJobs: {e}")

    return jobs


async def scrape_all_jobs(websites: Optional[List[str]] = None, crawl_company_domains: bool = False, max_crawl: int = 20, progress_callback=None) -> List[Dict]:
    """
    Scrape jobs from all specified websites.

    Args:
        websites: List of website IDs to scrape
        crawl_company_domains: Whether to crawl job pages for company domains
        max_crawl: Maximum number of job pages to crawl
        progress_callback: Optional callback function for progress updates
    """
    all_jobs = []

    scrapers_map = {
        "arbeitnow": scrape_arbeitnow,
    }

    if websites is None or len(websites) == 0:
        websites = list(scrapers_map.keys())

    if progress_callback:
        progress_callback({
            "type": "start",
            "message": f"Starting to scrape {len(websites)} website(s)",
            "websites": websites
        })

    connector = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for site_id in websites:
            if site_id in scrapers_map:
                scraper_func = scrapers_map[site_id]
                # Pass progress callback to each scraper
                tasks.append(scraper_func(session, progress_callback))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, list):
                    all_jobs.extend(result)
                elif isinstance(result, Exception):
                    print(f"Scraper error: {result}")

    # Remove duplicates
    seen = set()
    unique_jobs = []
    for job in all_jobs:
        key = (job["job_title"].lower().strip(), job["company"].lower().strip())
        if key not in seen:
            seen.add(key)
            unique_jobs.append(job)

    # Crawl company domains if requested
    if crawl_company_domains:
        # Collect job URLs to crawl
        job_urls = [job["job_url"] for job in unique_jobs if job.get("job_url")]

        if not job_urls:
            print("No job URLs available for company domain crawling.")
        else:
            print(f"Company domain crawling not available - simple_crawler module missing")
            # print(f"Crawling {len(job_urls)} job pages for company domains...")
            # domain_results = await batch_crawl_company_domains(job_urls, use_playwright=False)
            #
            # # Merge company domains back into jobs
            # for job in unique_jobs:
            #     job_url = job.get("job_url", "")
            #     if job_url and job_url in domain_results:
            #         job["company_domain"] = domain_results[job_url]

    return unique_jobs

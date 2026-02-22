"""
Web scrapers for various job boards.
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from playwright.async_api import async_playwright
import re


def categorize_job(title: str) -> str:
    """Determine job category based on title keywords."""
    title_lower = title.lower()

    categories = {
        "Engineering/Software": ["engineer", "developer", "software", "full stack", "frontend", "backend", "programmer", "coding"],
        "Data/Analytics": ["data scientist", "data analyst", "analytics", "business intelligence", "machine learning", "ai"],
        "Product/Design": ["product manager", "ux", "ui", "designer", "product owner", "product design"],
        "DevOps/Infrastructure": ["devops", "sre", "site reliability", "infrastructure", "cloud", "kubernetes", "aws"],
        "Management/Leadership": ["manager", "director", "head of", "vp", "chief", "cto", "ceo", "lead"],
        "Marketing/Sales": ["marketing", "sales", "growth", "seo", "content", "brand", "account manager"],
        "HR/Recruiting": ["hr", "recruiter", "talent", "people", "hiring", "recruitment"],
        "Finance/Accounting": ["finance", "accounting", "financial", "controller", "cfo", "analyst"],
        "Customer Support": ["support", "customer success", "customer service", "help desk"],
        "Operations": ["operations", "operational", "logistics", "supply chain"],
        "Hospitality/Tourism": ["hotel", "restaurant", "chef", "receptionist", "waiter", "tourism", "travel"],
        "Social/NGO": ["social", "ngo", "non-profit", "volunteer", "community", "charity"],
    }

    for category, keywords in categories.items():
        if any(keyword in title_lower for keyword in keywords):
            return category

    return "Other"


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


async def scrape_arbeitnow(session) -> List[Dict]:
    """Scrape jobs from Arbeitnow API."""
    jobs = []
    try:
        url = "https://www.arbeitnow.com/api/job-board-api"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status == 200:
                data = await response.json()
                for job in data.get("data", []):
                    title = job.get("title", "N/A")
                    company = job.get("company_name") or job.get("company") or "Unknown"

                    date_posted = "N/A"
                    if job.get("created_at"):
                        date_posted = job["created_at"][:10]

                    jobs.append({
                        "job_title": title,
                        "company": company,
                        "category": categorize_job(title),
                        "date_posted": date_posted,
                        "status": "Active",
                        "website": "arbeitnow.com"
                    })
    except Exception as e:
        print(f"Error scraping Arbeitnow: {e}")

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

            # Find job cards - they have class bjs-jlid__meta
            for div in soup.find_all('div', class_='bjs-jlid__meta'):
                h4 = div.find('h4')
                if h4:
                    title = h4.get_text(strip=True)
                    full_text = div.get_text(separator='|', strip=True)
                    company = extract_company_from_text(full_text, title)

                    jobs.append({
                        "job_title": title,
                        "company": company,
                        "category": categorize_job(title),
                        "date_posted": datetime.now().strftime("%Y-%m-%d"),
                        "status": "Active",
                        "website": "berlinstartupjobs.com"
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

            # Look for job listings - check various containers
            for item in soup.find_all(['div', 'article', 'li']):
                # Find the title element
                title_elem = item.find(['h2', 'h3', 'h4'])
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if len(title) > 15:
                        skip = ['chi siamo', 'privacy', 'menu', 'candidati', 'aziende', 'accedi', 'home', 'info', 'servizi', 'risorse', 'formazione', 'contatti', 'job4good', 'annunci']
                        if not any(s in title.lower() for s in skip):
                            # Get full text from container
                            full_text = item.get_text(separator='|', strip=True)
                            company = extract_company_from_text(full_text, title)

                            jobs.append({
                                "job_title": title,
                                "company": company,
                                "category": categorize_job(title),
                                "date_posted": datetime.now().strftime("%Y-%m-%d"),
                                "status": "Active",
                                "website": "job4good.it"
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

            # Look for job listings
            for item in soup.find_all(['div', 'article', 'li']):
                title_elem = item.find(['h2', 'h3', 'h4'])
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if len(title) > 15:
                        skip = ['inicia', 'registra', 'blog', 'empleos', 'turijobs', 'ofertas', 'empresa']
                        if not any(s in title.lower() for s in skip):
                            # Get full text from container
                            full_text = item.get_text(separator='|', strip=True)
                            company = extract_company_from_text(full_text, title)

                            jobs.append({
                                "job_title": title,
                                "company": company,
                                "category": categorize_job(title),
                                "date_posted": datetime.now().strftime("%Y-%m-%d"),
                                "status": "Active",
                                "website": "turijobs.com"
                            })

                            if len(jobs) >= 30:
                                break

            await browser.close()

    except Exception as e:
        print(f"Error scraping TuriJobs: {e}")

    return jobs


async def scrape_all_jobs(websites: Optional[List[str]] = None) -> List[Dict]:
    """Scrape jobs from all specified websites."""
    all_jobs = []

    scrapers_map = {
        "arbeitnow": scrape_arbeitnow,
        "berlinstartupjobs": scrape_berlin_startup_jobs,
        "job4good": scrape_job4good,
        "turijobs": scrape_turijobs
    }

    if websites is None or len(websites) == 0:
        websites = list(scrapers_map.keys())

    connector = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for site_id in websites:
            if site_id in scrapers_map:
                scraper_func = scrapers_map[site_id]
                tasks.append(scraper_func(session))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, list):
                    all_jobs.extend(result)
                elif isinstance(result, Exception):
                    print(f"Scraper error: {result}")

    # Remove duplicates based on job_title + company + website
    seen = set()
    unique_jobs = []
    for job in all_jobs:
        # Create a unique key
        key = (job["job_title"].lower().strip(), job["company"].lower().strip(), job["website"])
        if key not in seen:
            seen.add(key)
            unique_jobs.append(job)

    return unique_jobs

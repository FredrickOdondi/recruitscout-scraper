"""
Crawler for detailed job information extraction.
Crawls individual job post pages to get full details like description, requirements, benefits, etc.
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
from playwright.async_api import async_playwright
from datetime import datetime
import re
import json


class JobCrawler:
    """Crawler for extracting detailed job information from individual job pages."""

    def __init__(self):
        self.session = None
        self.browser = None
        self.page = None

    async def init_session(self):
        """Initialize aiohttp session for HTTP requests."""
        if self.session is None:
            connector = aiohttp.TCPConnector(ssl=False, limit=10)
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def init_browser(self):
        """Initialize Playwright browser for JavaScript-heavy pages."""
        if self.browser is None:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(headless=True)
            self.page = await self.browser.new_page()

    async def close(self):
        """Close all connections."""
        if self.session:
            await self.session.close()
        if self.browser:
            await self.browser.close()

    async def crawl_job_page(self, url: str, use_playwright: bool = False) -> Dict[str, str]:
        """
        Crawl a single job page and extract detailed information.

        Args:
            url: The job post URL
            use_playwright: Whether to use Playwright (for JS-heavy pages)

        Returns:
            Dictionary with job details: description, requirements, benefits, etc.
        """
        if use_playwright:
            return await self._crawl_with_playwright(url)
        else:
            return await self._crawl_with_aiohttp(url)

    async def _crawl_with_aiohttp(self, url: str) -> Dict[str, str]:
        """Crawl page using aiohttp (faster for static pages)."""
        await self.init_session()

        result = {
            "full_description": "",
            "requirements": "",
            "benefits": "",
            "skills": [],
            "salary_range": "",
            "remote": "",
            "experience_level": ""
        }

        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return result
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')

                # Extract full description
                result.update(self._extract_from_soup(soup))

        except Exception as e:
            print(f"Error crawling {url} with aiohttp: {e}")

        return result

    async def _crawl_with_playwright(self, url: str) -> Dict[str, str]:
        """Crawl page using Playwright (for dynamic/JS-heavy pages)."""
        await self.init_browser()

        result = {
            "full_description": "",
            "requirements": "",
            "benefits": "",
            "skills": [],
            "salary_range": "",
            "remote": "",
            "experience_level": ""
        }

        try:
            await self.page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await asyncio.sleep(2)  # Wait for any lazy-loaded content
            html = await self.page.content()
            soup = BeautifulSoup(html, 'lxml')

            # Extract full description
            result.update(self._extract_from_soup(soup))

        except Exception as e:
            print(f"Error crawling {url} with Playwright: {e}")

        return result

    def _extract_from_soup(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract job details from BeautifulSoup object."""
        result = {
            "full_description": "",
            "requirements": "",
            "benefits": "",
            "skills": [],
            "salary_range": "",
            "remote": "",
            "experience_level": ""
        }

        # First, try to extract from JSON-LD structured data
        json_ld_result = self._extract_from_json_ld(soup)
        if json_ld_result:
            result.update(json_ld_result)

        # If JSON-LD didn't have description, use HTML selectors
        if not result["full_description"]:
            # Common patterns for job descriptions
            description_selectors = [
                ('article', {}),
                ('div', {'class': re.compile(r'description|job-detail|job-content|posting-description', re.I)}),
                ('div', {'class': re.compile(r'description-text|job-description-text', re.I)}),
                ('section', {'class': re.compile(r'description|details', re.I)}),
                ('div', {'id': re.compile(r'description|job-description', re.I)}),
            ]

            for tag, attrs in description_selectors:
                elem = soup.find(tag, attrs)
                if elem:
                    result["full_description"] = elem.get_text(strip=True, separator=' ')
                    break

            # Fallback: find the largest text block
            if not result["full_description"]:
                all_divs = soup.find_all('div')
                largest_div = max(all_divs, key=lambda d: len(d.get_text(strip=True)), default=None)
                if largest_div and len(largest_div.get_text(strip=True)) > 200:
                    result["full_description"] = largest_div.get_text(strip=True, separator=' ')

        # Extract requirements (if not already from JSON-LD)
        if not result["requirements"]:
            requirements_keywords = ['requirement', 'qualification', 'what you need', 'you have', 'must have']
            result["requirements"] = self._extract_section_by_keywords(soup, requirements_keywords)

        # Extract benefits (if not already from JSON-LD)
        if not result["benefits"]:
            benefits_keywords = ['benefit', 'perk', 'what we offer', 'we offer', 'compensation']
            result["benefits"] = self._extract_section_by_keywords(soup, benefits_keywords)

        # Extract skills
        result["skills"] = self._extract_skills(soup)

        # Extract salary range
        result["salary_range"] = self._extract_salary(soup)

        # Extract remote/hybrid info
        result["remote"] = self._extract_remote(soup)

        # Extract experience level
        result["experience_level"] = self._extract_experience(soup)

        return result

    def _extract_section_by_keywords(self, soup: BeautifulSoup, keywords: List[str]) -> str:
        """Extract a section of the job post by matching keywords in headers."""
        for keyword in keywords:
            # Look for headers containing the keyword
            for header in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                header_text = header.get_text(strip=True).lower()
                if keyword in header_text:
                    # Get content after this header until next header
                    content = []
                    sibling = header.find_next_sibling()
                    while sibling and sibling.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        text = sibling.get_text(strip=True)
                        if text:
                            content.append(text)
                        sibling = sibling.find_next_sibling()
                        if sibling and len(content) > 5:  # Limit section length
                            break
                    if content:
                        return ' '.join(content[:10])  # Return first 10 items
        return ""

    def _extract_skills(self, soup: BeautifulSoup) -> List[str]:
        """Extract skills from the job posting."""
        skills = []

        # Look for skill tags/labels
        skill_selectors = [
            ('div', {'class': re.compile(r'skill|tag|badge|pill', re.I)}),
            ('span', {'class': re.compile(r'skill|tag|badge|pill', re.I)}),
            ('li', {'class': re.compile(r'skill', re.I)}),
        ]

        for tag, attrs in skill_selectors:
            for elem in soup.find_all(tag, attrs):
                skill_text = elem.get_text(strip=True)
                if skill_text and len(skill_text) < 50 and skill_text not in skills:
                    skills.append(skill_text)

        # Also try to extract from common skill sections
        skill_keywords = ['skill', 'tech stack', 'technologies', 'tech', 'stack']
        for keyword in skill_keywords:
            for header in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                header_text = header.get_text(strip=True).lower()
                if keyword in header_text:
                    sibling = header.find_next_sibling()
                    while sibling and sibling.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        text = sibling.get_text(strip=True)
                        if text:
                            # Split by common delimiters
                            items = re.split(r'[,;•\n]+', text)
                            for item in items:
                                item = item.strip()
                                if item and len(item) < 50 and item not in skills:
                                    skills.append(item)
                        sibling = sibling.find_next_sibling()
                        if sibling and len(skills) > 20:
                            break
                    break

        return skills[:15]  # Limit to 15 skills

    def _extract_salary(self, soup: BeautifulSoup) -> str:
        """Extract salary information."""
        # Look for salary in text using patterns
        salary_patterns = [
            r'\$[\d,]+(?:\s*[-–to]+\s*[\d,]+)?\s*(?:per\s*(?:year|month|hour|hr)|\/(?:year|month|hour|hr)|annual|monthly|hourly)',
            r'[\d,]+(?:\s*[-–to]+\s*[\d,]+)?\s*(?:USD|EUR|GBP)\s*(?:per\s*(?:year|month|hour)|\/(?:year|month|hour))',
            r'[\d,]+k?\s*[-–to]+\s*[\d,]+k?\s*(?:per\s*year|annual)',
            r'€[\d,]+(?:\s*[-–to]+\s*[\d,]+)?',
            r'£[\d,]+(?:\s*[-–to]+\s*[\d,]+)?',
        ]

        full_text = soup.get_text()

        for pattern in salary_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                return match.group(0).strip()

        # Look for salary in specific elements
        salary_selectors = [
            ('span', {'class': re.compile(r'salary|compensation|pay|wage', re.I)}),
            ('div', {'class': re.compile(r'salary|compensation|pay|wage', re.I)}),
        ]

        for tag, attrs in salary_selectors:
            elem = soup.find(tag, attrs)
            if elem:
                salary_text = elem.get_text(strip=True)
                if any(x in salary_text.lower() for x in ['$', '€', '£', 'salary', 'pay']):
                    return salary_text

        return ""

    def _extract_remote(self, soup: BeautifulSoup) -> str:
        """Extract remote/hybrid work information."""
        remote_keywords = ['remote', 'hybrid', 'onsite', 'on-site', 'office', 'work from home', 'wfh']
        full_text = soup.get_text().lower()

        for keyword in remote_keywords:
            if keyword in full_text:
                if keyword in ['remote', 'work from home', 'wfh']:
                    return "Remote"
                elif keyword == 'hybrid':
                    return "Hybrid"
                elif keyword in ['onsite', 'on-site', 'office']:
                    return "On-site"

        return ""

    def _extract_experience(self, soup: BeautifulSoup) -> str:
        """Extract experience level required."""
        experience_patterns = [
            r'(\d+)\s*\+\s*years?\s*(?:of\s*)?(?:experience|exp)',
            r'(?:junior|entry[- ]level|intern)',
            r'(?:mid[- ]level|mid[- ]senior)',
            r'(?:senior|lead|principal|staff)',
            r'(?:executive|director|vp|c[- ]level)',
        ]

        full_text = soup.get_text().lower()

        for pattern in experience_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                matched = match.group(0)
                if 'junior' in matched or 'entry' in matched or 'intern' in matched:
                    return "Entry Level"
                elif 'mid' in matched:
                    return "Mid Level"
                elif 'senior' in matched:
                    return "Senior Level"
                elif 'lead' in matched or 'principal' in matched or 'staff' in matched:
                    return "Lead/Principal"
                elif 'executive' in matched or 'director' in matched or 'vp' in matched or 'c-level' in matched:
                    return "Executive"
                elif re.search(r'\d+', matched):
                    years = re.search(r'(\d+)', matched).group(1)
                    years_int = int(years)
                    if years_int < 2:
                        return "Entry Level"
                    elif years_int < 5:
                        return "Mid Level"
                    elif years_int < 8:
                        return "Senior Level"
                    else:
                        return "Lead/Principal"

        return ""

    def _extract_from_json_ld(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract job details from JSON-LD structured data."""
        result = {}

        try:
            # Find all script tags with JSON-LD
            for script in soup.find_all('script', {'type': 'application/ld+json'}):
                try:
                    data = json.loads(script.string)

                    # Handle single object or array
                    items = [data] if isinstance(data, dict) else data

                    for item in items:
                        # Look for JobPosting type
                        if item.get('@type') == 'JobPosting':
                            # Extract description
                            if item.get('description'):
                                result["full_description"] = BeautifulSoup(item['description'], 'lxml').get_text(strip=True, separator=' ')

                            # Extract requirements (often in description but can be separate)
                            if item.get('requirements'):
                                result["requirements"] = BeautifulSoup(item['requirements'], 'lxml').get_text(strip=True, separator=' ')

                            # Extract skills (from skills property)
                            if item.get('skills'):
                                skills = item['skills']
                                if isinstance(skills, str):
                                    result["skills"] = [s.strip() for s in skills.split(',')]
                                elif isinstance(skills, list):
                                    result["skills"] = [str(s).strip() for s in skills]

                            # Extract salary
                            if item.get('baseSalary'):
                                salary = item['baseSalary']
                                if isinstance(salary, dict):
                                    result["salary_range"] = f"{salary.get('value', {}).get('value', '')} {salary.get('value', {}).get('currency', '')}"
                                else:
                                    result["salary_range"] = str(salary)

                            # Extract employment type
                            if item.get('employmentType'):
                                result["employment_type"] = item['employmentType']

                            # Extract remote info
                            if item.get('jobLocationType'):
                                location_type = item['jobLocationType']
                                if 'REMOTE' in location_type.upper():
                                    result["remote"] = "Remote"
                                elif 'TELECOMMUTE' in location_type.upper():
                                    result["remote"] = "Remote"

                            # Extract experience level
                            if item.get('experienceRequirements'):
                                exp = item['experienceRequirements']
                                if isinstance(exp, dict) and exp.get('name'):
                                    result["experience_level"] = exp['name']
                                elif isinstance(exp, str):
                                    result["experience_level"] = exp

                            # Extract company domain from hiringOrganization
                            if item.get('hiringOrganization'):
                                org = item['hiringOrganization']
                                if isinstance(org, dict):
                                    # Try to get domain multiple ways
                                    if org.get('url'):
                                        result["company_page_domain"] = extract_domain_from_url(org['url'])
                                    # Also try name field (some sources use name instead of URL)
                                    elif org.get('name'):
                                        result["company_page_domain"] = extract_domain_from_url(org['name'])

                            break  # Found JobPosting, no need to check others

                except (json.JSONDecodeError, KeyError, TypeError):
                    continue

        except Exception as e:
            print(f"Error extracting JSON-LD: {e}")

        return result


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


class PaginationCrawler:
    """Crawler for handling pagination on job boards."""

    async def crawl_with_pagination(
        self,
        start_url: str,
        job_selector: str,
        next_page_selector: str,
        max_pages: int = 5,
        extract_func=None
    ) -> List[Dict]:
        """
        Crawl multiple pages using pagination.

        Args:
            start_url: The URL to start from
            job_selector: CSS selector for job listings
            next_page_selector: CSS selector for next page link
            max_pages: Maximum number of pages to crawl
            extract_func: Function to extract job data from page

        Returns:
            List of job dictionaries
        """
        jobs = []
        current_url = start_url
        page_count = 0

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            while current_url and page_count < max_pages:
                try:
                    await page.goto(current_url, timeout=30000)
                    await page.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(2)

                    soup = BeautifulSoup(await page.content(), 'lxml')

                    # Extract jobs from current page
                    if extract_func:
                        page_jobs = extract_func(soup)
                    else:
                        page_jobs = self._default_extract(soup, job_selector)
                    jobs.extend(page_jobs)

                    page_count += 1
                    print(f"Crawled page {page_count}: {len(page_jobs)} jobs found")

                    # Find next page link
                    next_button = page.query_selector(next_page_selector)
                    if next_button:
                        current_url = await next_button.get_attribute('href')
                        if current_url and not current_url.startswith('http'):
                            current_url = self._resolve_url(current_url, start_url)
                    else:
                        current_url = None

                except Exception as e:
                    print(f"Error crawling page {page_count + 1}: {e}")
                    break

            await browser.close()

        return jobs

    def _default_extract(self, soup: BeautifulSoup, selector: str) -> List[Dict]:
        """Default extraction function for pagination."""
        jobs = []
        for elem in soup.select(selector):
            jobs.append({
                "title": elem.get_text(strip=True),
                "url": elem.get('href', '')
            })
        return jobs

    def _resolve_url(self, url: str, base_url: str) -> str:
        """Resolve relative URL to absolute."""
        if url.startswith('http'):
            return url
        from urllib.parse import urljoin
        return urljoin(base_url, url)


async def batch_crawl_jobs(job_urls: List[str], crawler: JobCrawler, use_playwright: bool = False) -> List[Dict]:
    """
    Crawl multiple job URLs concurrently.

    Args:
        job_urls: List of job URLs to crawl
        crawler: JobCrawler instance
        use_playwright: Whether to use Playwright

    Returns:
        List of job details
    """
    tasks = [crawler.crawl_job_page(url, use_playwright) for url in job_urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    job_details = []
    for url, result in zip(job_urls, results):
        if isinstance(result, Exception):
            print(f"Error crawling {url}: {result}")
            job_details.append({})
        else:
            result["source_url"] = url
            job_details.append(result)

    return job_details

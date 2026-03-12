"""
FastAPI application for job scraping from multiple job boards.
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from datetime import datetime
import uvicorn
import io
import asyncio
import json

from scrapers import scrape_all_jobs

app = FastAPI(title="RecruitScout Job Scraper")

templates = Jinja2Templates(directory="templates")


class JobData(BaseModel):
    job_title: str
    company: str
    company_domain: str = ""
    location: str = ""
    description: str = ""
    job_url: str = ""
    date_posted: str
    employment_type: str = ""
    salary: str = ""
    status: str


class ScrapingRequest(BaseModel):
    websites: Optional[List[str]] = None
    crawl_company_domains: bool = False
    max_crawl: int = 20


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the home page with the scraping interface."""
    available_websites = [
        {
            "name": "Arbeitnow",
            "url": "https://www.arbeitnow.com/api/job-board-api",
            "id": "arbeitnow"
        }
    ]

    return templates.TemplateResponse("index.html", {"request": request, "websites": available_websites})


@app.post("/api/scrape")
async def scrape_jobs(request: ScrapingRequest):
    """
    Scrape jobs from the selected websites.
    If no websites specified, scrape all.

    Args:
        websites: List of website IDs to scrape
        crawl_company_domains: Whether to crawl job pages for company domains
        max_crawl: Maximum number of job pages to crawl
    """
    websites_to_scrape = request.websites
    crawl_company_domains = request.crawl_company_domains
    max_crawl = request.max_crawl

    jobs = await scrape_all_jobs(websites_to_scrape, crawl_company_domains=crawl_company_domains, max_crawl=max_crawl)

    return {
        "success": True,
        "count": len(jobs),
        "data": jobs,
        "scraped_at": datetime.now().isoformat()
    }


@app.get("/api/export/csv")
async def export_csv():
    """
    Scrape all jobs and return as CSV file.
    """
    jobs = await scrape_all_jobs()

    df = pd.DataFrame(jobs)
    output = io.StringIO()
    df.to_csv(output, index=False)

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode('utf-8')),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        }
    )


@app.post("/api/scrape/stream")
async def scrape_jobs_stream(request: ScrapingRequest):
    """
    Scrape jobs with real-time progress updates via SSE.
    """
    websites_to_scrape = request.websites
    crawl_company_domains = request.crawl_company_domains
    max_crawl = request.max_crawl

    async def generate():
        progress_queue = asyncio.Queue()

        # Start scraping in background task with progress callback
        async def scrape_with_progress():
            progress_callback = lambda msg: asyncio.create_task(progress_queue.put(msg))
            jobs = await scrape_all_jobs(
                websites_to_scrape,
                crawl_company_domains=crawl_company_domains,
                max_crawl=max_crawl,
                progress_callback=progress_callback
            )
            # Send jobs as complete signal
            await progress_queue.put({"type": "complete", "data": jobs})

        scraping_task = asyncio.create_task(scrape_with_progress())

        try:
            while True:
                message = await asyncio.wait_for(progress_queue.get(), timeout=300.0)

                if message.get("type") == "complete":
                    jobs = message.get("data", [])
                    yield f"data: {json.dumps({'type': 'complete', 'count': len(jobs), 'data': jobs})}\n\n"
                    break
                else:
                    yield f"data: {json.dumps(message)}\n\n"

        except asyncio.TimeoutError:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Scraping timeout'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            if not scraping_task.done():
                scraping_task.cancel()

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

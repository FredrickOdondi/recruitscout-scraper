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

from scrapers import scrape_all_jobs

app = FastAPI(title="RecruitScout Job Scraper")

templates = Jinja2Templates(directory="templates")


class JobData(BaseModel):
    job_title: str
    company: str
    category: str
    date_posted: str
    status: str
    website: str


class ScrapingRequest(BaseModel):
    websites: Optional[List[str]] = None


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the home page with the scraping interface."""
    available_websites = [
        {
            "name": "Arbeitnow",
            "url": "https://www.arbeitnow.com/api/job-board-api",
            "id": "arbeitnow"
        },
        {
            "name": "Berlin Startup Jobs",
            "url": "https://berlinstartupjobs.com/",
            "id": "berlinstartupjobs"
        },
        {
            "name": "Job4Good",
            "url": "https://www.job4good.it/annunci-lavoro-sociale/",
            "id": "job4good"
        },
        {
            "name": "TuriJobs",
            "url": "https://www.turijobs.com/ofertas-trabajo",
            "id": "turijobs"
        }
    ]

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "websites": available_websites}
    )


@app.post("/api/scrape")
async def scrape_jobs(request: ScrapingRequest):
    """
    Scrape jobs from the selected websites.
    If no websites specified, scrape all.
    """
    websites_to_scrape = request.websites

    jobs = await scrape_all_jobs(websites_to_scrape)

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


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

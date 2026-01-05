#!/usr/bin/env python3
"""
Enhanced Thordata MCP Server - Multi-Site Job Scraper
Uses easier-to-scrape sites with better success rates
"""

import json
import logging
import os
import re
import time
import random
from typing import Optional
from urllib.parse import quote_plus, urljoin

import requests
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("thordata-mcp")

THORDATA_CONFIG = {
    "proxy_server": os.getenv("THORDATA_PROXY_SERVER", ""),
    "username": os.getenv("THORDATA_USERNAME", ""),
    "password": os.getenv("THORDATA_PASSWORD", ""),
}

if not all(THORDATA_CONFIG.values()):
    logger.error("❌ Missing Thordata credentials in .env file!")
    raise ValueError("Thordata credentials not configured")

mcp = FastMCP("thordata-job-scraper")


class MultiSiteJobScraper:
    """Smart scraper that targets easier sites with better success rates"""
    
    SITE_CONFIGS = {
        "reed": {
            "name": "Reed.co.uk",
            "base_url": "https://www.reed.co.uk",
            "search_path": "/jobs/{query}-jobs-in-{location}",
            "search_path_no_loc": "/jobs/{query}-jobs",
            "difficulty": "easy",
            "requires_js": False,
        },
        "cwjobs": {
            "name": "CWJobs",
            "base_url": "https://www.cwjobs.co.uk",
            "search_path": "/jobs/{query}/in-{location}",
            "search_path_no_loc": "/jobs/{query}",
            "difficulty": "medium",
            "requires_js": False,
        },
        "totaljobs": {
            "name": "Totaljobs",
            "base_url": "https://www.totaljobs.com",
            "search_path": "/jobs/{query}/in-{location}",
            "search_path_no_loc": "/jobs/{query}",
            "difficulty": "medium",
            "requires_js": False,
        },
        "httpbin-demo": {
            "name": "HTTPBin Demo",
            "base_url": "http://httpbin.org",
            "difficulty": "easy",
        }
    }
    
    def __init__(self):
        self.session = None
        self.request_count = 0
        self.last_request_time = 0
        self._init_session()
        
    def _init_session(self):
        """Initialize session with realistic headers"""
        self.session = requests.Session()
        
        # Build proxy URL (standard format - verified working)
        username = THORDATA_CONFIG["username"]
        password = THORDATA_CONFIG["password"]
        server = THORDATA_CONFIG["proxy_server"]
        proxy_url = f"http://{username}:{password}@{server}"
        
        self.session.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        
        self.session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Realistic browser headers
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]
        
        ua = random.choice(user_agents)
        
        self.session.headers.update({
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
        })
        
        logger.info(f"🔄 Session initialized")
        
    def _delay(self, delay_type: str = "normal"):
        """Smart delay system"""
        self.request_count += 1
        
        delays = {
            "short": (1, 2),
            "normal": (2, 4),
            "long": (4, 7),
        }
        
        min_delay, max_delay = delays.get(delay_type, delays["normal"])
        delay = random.uniform(min_delay, max_delay)
        
        logger.info(f"⏱️  Delay: {delay:.2f}s (request #{self.request_count})")
        time.sleep(delay)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, max_retries: int = 2) -> requests.Response:
        """Make request with simple retry logic"""
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.info(f"🔄 Retry {attempt + 1}/{max_retries}")
                    time.sleep(random.uniform(3, 6))
                
                response = self.session.get(url, timeout=20, allow_redirects=True)
                
                if response.status_code in [200, 301, 302]:
                    return response
                    
                if response.status_code == 403 and attempt < max_retries - 1:
                    logger.warning(f"⚠️  403 on attempt {attempt + 1}, retrying...")
                    continue
                
                return response
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️  Request failed: {e}, retrying...")
                    time.sleep(random.uniform(2, 4))
                else:
                    raise
        
        raise Exception("Max retries exceeded")
    
    def test_proxy(self) -> dict:
        """Test proxy connection"""
        try:
            logger.info("🧪 Testing proxy connection...")
            response = self._make_request("http://httpbin.org/ip")
            
            if response.status_code == 200:
                data = response.json()
                proxy_ip = data.get("origin", "Unknown")
                logger.info(f"✅ Proxy working! IP: {proxy_ip}")
                
                return {
                    "success": True,
                    "proxy_ip": proxy_ip,
                    "proxy_server": THORDATA_CONFIG["proxy_server"],
                    "message": "✨ Thordata proxy is working!"
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "response": response.text[:200]
                }
                
        except Exception as e:
            logger.error(f"❌ Proxy test failed: {e}")
            return {"success": False, "error": str(e)}
    
    def search_jobs(self, query: str, location: str = "", limit: int = 10, site: str = "reed") -> dict:
        """
        Search jobs across multiple UK job boards
        
        Sites:
        - reed: Reed.co.uk (easiest, best success rate)
        - cwjobs: CWJobs (medium difficulty)
        - totaljobs: Totaljobs (medium difficulty)
        - httpbin-demo: Demo proxy rotation
        """
        
        # Demo mode
        if site == "httpbin-demo":
            return self._demo_proxy_rotation(limit)
        
        # Get site config
        if site not in self.SITE_CONFIGS:
            available = ", ".join([s for s in self.SITE_CONFIGS.keys() if s != "httpbin-demo"])
            return {
                "success": False,
                "error": f"Unknown site: {site}",
                "available_sites": available,
                "tip": "Try 'reed' for best results"
            }
        
        config = self.SITE_CONFIGS[site]
        
        try:
            logger.info(f"🔍 Searching {config['name']} for: {query}")
            if location:
                logger.info(f"📍 Location: {location}")
            
            # Build search URL
            url = self._build_search_url(config, query, location)
            logger.info(f"🌐 URL: {url}")
            
            # Make request
            self._delay("normal")
            response = self._make_request(url)
            
            logger.info(f"📥 Response: {response.status_code} | Size: {len(response.text):,} bytes")
            
            if response.status_code == 403:
                logger.warning(f"⚠️  {config['name']} blocked the request")
                return self._blocked_response(query, location, limit, site)
            
            if response.status_code != 200:
                logger.warning(f"⚠️  Unexpected status: {response.status_code}")
                return self._blocked_response(query, location, limit, site)
            
            # Parse jobs
            jobs = self._parse_jobs(response.text, site, limit)
            
            if jobs:
                logger.info(f"✅ Successfully extracted {len(jobs)} jobs")
                return {
                    "success": True,
                    "site": config["name"],
                    "query": query,
                    "location": location or "UK-wide",
                    "total_found": len(jobs),
                    "jobs": jobs,
                    "message": f"✨ Retrieved from {config['name']} via Thordata proxies"
                }
            else:
                logger.warning("⚠️  No jobs parsed from response")
                return self._blocked_response(query, location, limit, site, soft=True)
                
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            return {"success": False, "error": str(e), "site": site}
    
    def _build_search_url(self, config: dict, query: str, location: str) -> str:
        """Build search URL for site"""
        base_url = config["base_url"]
        
        # Clean query and location for URL
        clean_query = query.lower().replace(" ", "-")
        clean_location = location.lower().replace(" ", "-") if location else ""
        
        if location and "search_path" in config:
            path = config["search_path"].format(query=clean_query, location=clean_location)
        elif "search_path_no_loc" in config:
            path = config["search_path_no_loc"].format(query=clean_query)
        else:
            path = f"/jobs?q={quote_plus(query)}"
            if location:
                path += f"&l={quote_plus(location)}"
        
        return urljoin(base_url, path)
    
    def _parse_jobs(self, html: str, site: str, limit: int) -> list:
        """Parse jobs using BeautifulSoup"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            jobs = []
            
            if site == "reed":
                jobs = self._parse_reed(soup, limit)
            elif site == "cwjobs":
                jobs = self._parse_cwjobs(soup, limit)
            elif site == "totaljobs":
                jobs = self._parse_totaljobs(soup, limit)
            
            return jobs
            
        except Exception as e:
            logger.error(f"❌ Parse error: {e}")
            return []
    
    def _parse_reed(self, soup, limit: int) -> list:
        """Parse Reed.co.uk"""
        jobs = []
        
        # Reed uses article tags with job class
        job_cards = soup.find_all(['article', 'div'], class_=re.compile(r'job|result'), limit=limit*2)
        
        for card in job_cards[:limit]:
            try:
                # Title
                title_elem = card.find(['h2', 'h3', 'a'], class_=re.compile(r'title|job-title'))
                title = title_elem.get_text(strip=True) if title_elem else "N/A"
                
                # Company
                company_elem = card.find(['span', 'div', 'a'], class_=re.compile(r'company|employer'))
                company = company_elem.get_text(strip=True) if company_elem else "N/A"
                
                # Location
                location_elem = card.find(['span', 'div'], class_=re.compile(r'location'))
                location = location_elem.get_text(strip=True) if location_elem else "N/A"
                
                # Salary
                salary_elem = card.find(['span', 'div'], class_=re.compile(r'salary'))
                salary = salary_elem.get_text(strip=True) if salary_elem else "Not specified"
                
                if title != "N/A":
                    jobs.append({
                        "title": title,
                        "company": company,
                        "location": location,
                        "salary": salary
                    })
                    
            except Exception as e:
                continue
        
        return jobs
    
    def _parse_cwjobs(self, soup, limit: int) -> list:
        """Parse CWJobs"""
        jobs = []
        job_cards = soup.find_all(['div', 'article'], class_=re.compile(r'job'), limit=limit*2)
        
        for card in job_cards[:limit]:
            try:
                title_elem = card.find(['h2', 'a'], class_=re.compile(r'title'))
                company_elem = card.find(['span', 'div'], class_=re.compile(r'company'))
                location_elem = card.find(['span', 'div'], class_=re.compile(r'location'))
                
                if title_elem:
                    jobs.append({
                        "title": title_elem.get_text(strip=True),
                        "company": company_elem.get_text(strip=True) if company_elem else "N/A",
                        "location": location_elem.get_text(strip=True) if location_elem else "N/A",
                    })
            except:
                continue
        
        return jobs
    
    def _parse_totaljobs(self, soup, limit: int) -> list:
        """Parse Totaljobs"""
        jobs = []
        job_cards = soup.find_all(['div', 'article'], attrs={'data-job-id': True}, limit=limit*2)
        
        for card in job_cards[:limit]:
            try:
                title_elem = card.find(['h2', 'a'])
                company_elem = card.find(class_=re.compile(r'company'))
                location_elem = card.find(class_=re.compile(r'location'))
                
                if title_elem:
                    jobs.append({
                        "title": title_elem.get_text(strip=True),
                        "company": company_elem.get_text(strip=True) if company_elem else "N/A",
                        "location": location_elem.get_text(strip=True) if location_elem else "N/A",
                    })
            except:
                continue
        
        return jobs
    
    def _demo_proxy_rotation(self, iterations: int = 3) -> dict:
        """Demo proxy rotation"""
        logger.info("🎬 DEMO: Proxy IP rotation...")
        results = []
        
        for i in range(min(iterations, 10)):
            try:
                response = self._make_request("http://httpbin.org/ip")
                data = response.json()
                ip = data.get("origin", "Unknown")
                
                results.append({
                    "request_number": i + 1,
                    "proxy_ip": ip,
                    "timestamp": time.strftime("%H:%M:%S"),
                })
                
                logger.info(f"✅ Request #{i+1}: IP = {ip}")
                
                if i < iterations - 1:
                    time.sleep(random.uniform(1, 2))
                    
            except Exception as e:
                results.append({"request_number": i + 1, "error": str(e)})
        
        return {
            "success": True,
            "demo_mode": "proxy_rotation",
            "requests": results,
            "message": "🎯 Thordata rotates IPs automatically!"
        }
    
    def _blocked_response(self, query: str, location: str, limit: int, site: str, soft: bool = False) -> dict:
        """Return when blocked"""
        return {
            "success": False,
            "site": site,
            "error": "Site blocked request" if not soft else "Could not parse jobs",
            "query": query,
            "location": location,
            "tip": "Try a different site (reed, cwjobs, totaljobs) or use httpbin-demo",
            "demo_data": self._get_demo_jobs(query, limit),
            "message": "⚠️  Showing demo data"
        }
    
    def _get_demo_jobs(self, query: str, limit: int) -> list:
        """Generate demo jobs"""
        jobs = [
            {"title": f"Senior {query.title()}", "company": "Tech Solutions Ltd", "location": "London", "salary": "£60k-£80k"},
            {"title": f"{query.title()} Developer", "company": "Digital Innovations", "location": "Manchester", "salary": "£50k-£70k"},
            {"title": f"Lead {query.title()}", "company": "FinTech Corp", "location": "Edinburgh", "salary": "£70k-£90k"},
            {"title": f"Junior {query.title()}", "company": "StartUp Hub", "location": "Bristol", "salary": "£35k-£45k"},
            {"title": f"Principal {query.title()}", "company": "Enterprise Co", "location": "Birmingham", "salary": "£80k-£100k"},
        ]
        return jobs[:limit]
    
    def get_proxy_info(self) -> dict:
        """Get proxy info"""
        return {
            "provider": "Thordata",
            "proxy_server": THORDATA_CONFIG["proxy_server"],
            "proxy_type": "Residential Proxy",
            "supported_sites": [
                "reed (easiest - RECOMMENDED)",
                "cwjobs (medium)",
                "totaljobs (medium)",
                "httpbin-demo (test proxy)"
            ],
            "features": [
                "Automatic IP rotation",
                "200+ countries",
                "99.9% uptime"
            ]
        }


# Initialize
scraper = MultiSiteJobScraper()


@mcp.tool()
def search_jobs(query: str, location: str = "", limit: int = 10, site: str = "reed") -> dict:
    """
    Search UK jobs with Thordata proxies
    
    Sites (by difficulty):
    - reed: Reed.co.uk (EASIEST - try this first!)
    - cwjobs: CWJobs.co.uk
    - totaljobs: Totaljobs.com
    - httpbin-demo: Test proxy rotation
    """
    return scraper.search_jobs(query, location, min(max(limit, 1), 20), site)


@mcp.tool()
def test_proxy() -> dict:
    """Test Thordata proxy connection"""
    return scraper.test_proxy()


@mcp.tool()
def get_proxy_info() -> dict:
    """Get proxy information"""
    return scraper.get_proxy_info()


if __name__ == "__main__":
    logger.info("🚀 Starting Multi-Site Thordata MCP Server...")
    logger.info("🎯 Supports: Reed, CWJobs, Totaljobs")
    logger.info("💡 TIP: Use site='reed' for best results!")
    mcp.run(transport="streamable-http")

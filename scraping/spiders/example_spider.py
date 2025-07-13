"""
Example Scrapy spider for web scraping.
Template spider for extracting data from websites without APIs.
"""

import scrapy
from scrapy import Request
from typing import Dict, Any, Iterator, Optional
from datetime import datetime
import json
import re
from urllib.parse import urljoin, urlparse
from scrapy.http import Response

from ..items import ScrapedItem
from ..utils import clean_text, extract_date, validate_url


class ExampleSpider(scrapy.Spider):
    """
    Example spider for web scraping.
    
    TODO: Customize this spider for specific websites:
    - News websites for political articles
    - Government websites for official data
    - Social media platforms for public opinion
    - Academic websites for research data
    """
    
    name = 'example_spider'
    allowed_domains = ['example.com']
    start_urls = ['https://example.com']
    
    # Custom settings for this spider
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 4,
        'ROBOTSTXT_OBEY': True,
        'FEEDS': {
            'data/raw/example_spider_data.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
            },
        },
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = datetime.now()
        self.scraped_count = 0
        self.error_count = 0
    
    def start_requests(self) -> Iterator[Request]:
        """Generate initial requests."""
        for url in self.start_urls:
            yield Request(
                url=url,
                callback=self.parse,
                meta={'start_time': datetime.now()}
            )
    
    def parse(self, response: Response) -> Iterator[Request]:
        """
        Parse the main page and extract links.
        
        TODO: Customize parsing logic for specific websites.
        """
        try:
            # Extract article links (example)
            article_links = response.css('a.article-link::attr(href)').getall()
            
            for link in article_links:
                absolute_url = urljoin(response.url, link)
                
                if self.is_valid_url(absolute_url):
                    yield Request(
                        url=absolute_url,
                        callback=self.parse_article,
                        meta={
                            'source_url': response.url,
                            'found_time': datetime.now()
                        }
                    )
            
            # Follow pagination (example)
            next_page = response.css('a.next-page::attr(href)').get()
            if next_page:
                yield Request(
                    url=urljoin(response.url, next_page),
                    callback=self.parse,
                    meta={'page': response.meta.get('page', 1) + 1}
                )
                
        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}")
            self.error_count += 1
    
    def parse_article(self, response: Response) -> Iterator[ScrapedItem]:
        """
        Parse individual article pages.
        
        TODO: Customize extraction logic for specific content types.
        """
        try:
            # Extract article data (example)
            title = response.css('h1::text').get()
            content = response.css('div.content p::text').getall()
            author = response.css('span.author::text').get()
            date_str = response.css('time::attr(datetime)').get()
            tags = response.css('span.tag::text').getall()
            
            # Clean and validate data
            if title:
                title = clean_text(title)
            
            if content:
                content = ' '.join([clean_text(p) for p in content])
            
            if author:
                author = clean_text(author)
            
            published_date = extract_date(date_str) if date_str else None
            
            # Create item
            item = ScrapedItem(
                url=response.url,
                title=title,
                content=content,
                author=author,
                published_date=published_date,
                tags=tags,
                scraped_at=datetime.now(),
                source_domain=urlparse(response.url).netloc,
                metadata={
                    'source_url': response.meta.get('source_url'),
                    'found_time': response.meta.get('found_time'),
                    'response_status': response.status,
                    'content_length': len(content) if content else 0,
                    'word_count': len(content.split()) if content else 0,
                }
            )
            
            self.scraped_count += 1
            yield item
            
        except Exception as e:
            self.logger.error(f"Error parsing article {response.url}: {e}")
            self.error_count += 1
    
    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and should be scraped."""
        try:
            parsed = urlparse(url)
            
            # Check domain
            if parsed.netloc not in self.allowed_domains:
                return False
            
            # Check for unwanted paths
            unwanted_paths = ['/login', '/register', '/admin', '/api/']
            if any(path in parsed.path for path in unwanted_paths):
                return False
            
            # Check for valid scheme
            if parsed.scheme not in ['http', 'https']:
                return False
            
            return True
            
        except Exception:
            return False
    
    def closed(self, reason: str):
        """Called when spider is closed."""
        duration = datetime.now() - self.start_time
        
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Duration: {duration}")
        self.logger.info(f"Items scraped: {self.scraped_count}")
        self.logger.info(f"Errors: {self.error_count}")
        
        # Log statistics
        stats = self.crawler.stats.get_stats()
        self.logger.info(f"Final statistics: {stats}")


class NewsSpider(ExampleSpider):
    """
    Spider for news websites.
    
    TODO: Implement specific logic for news extraction:
    - Political news articles
    - Opinion pieces
    - Editorial content
    - Press releases
    """
    
    name = 'news_spider'
    allowed_domains = ['news-website.com']
    start_urls = ['https://news-website.com/politics']
    
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
        'FEEDS': {
            'data/raw/news_data.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
            },
        },
    }
    
    def parse_article(self, response: Response) -> Iterator[ScrapedItem]:
        """Parse news article with specific extraction logic."""
        try:
            # TODO: Implement news-specific extraction
            # - Extract headline, byline, lead paragraph
            # - Identify political topics and entities
            # - Extract quotes and sources
            # - Classify article type (news, opinion, analysis)
            
            # Placeholder implementation
            yield from super().parse_article(response)
            
        except Exception as e:
            self.logger.error(f"Error parsing news article {response.url}: {e}")


class SocialMediaSpider(ExampleSpider):
    """
    Spider for social media platforms.
    
    TODO: Implement specific logic for social media:
    - Public posts and comments
    - Hashtag trends
    - User profiles and statistics
    - Political sentiment analysis
    
    Note: Respect robots.txt and terms of service.
    """
    
    name = 'social_media_spider'
    allowed_domains = ['social-platform.com']
    start_urls = ['https://social-platform.com/trending']
    
    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'ROBOTSTXT_OBEY': True,
        'FEEDS': {
            'data/raw/social_media_data.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
            },
        },
    }
    
    def parse(self, response: Response) -> Iterator[Request]:
        """Parse social media pages."""
        try:
            # TODO: Implement social media parsing
            # - Extract trending topics
            # - Follow hashtag links
            # - Respect rate limits
            # - Handle dynamic content (if using splash/selenium)
            
            # Placeholder implementation
            yield from super().parse(response)
            
        except Exception as e:
            self.logger.error(f"Error parsing social media {response.url}: {e}")


class GovernmentSpider(ExampleSpider):
    """
    Spider for government websites.
    
    TODO: Implement specific logic for government data:
    - Press releases and announcements
    - Policy documents
    - Statistical reports
    - Legislative updates
    """
    
    name = 'government_spider'
    allowed_domains = ['government.gov']
    start_urls = ['https://government.gov/press-releases']
    
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 4,
        'ROBOTSTXT_OBEY': True,
        'FEEDS': {
            'data/raw/government_data.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
            },
        },
    }
    
    def parse_article(self, response: Response) -> Iterator[ScrapedItem]:
        """Parse government documents."""
        try:
            # TODO: Implement government-specific extraction
            # - Extract official announcements
            # - Parse structured data (dates, departments, etc.)
            # - Handle PDF documents
            # - Extract contact information
            
            # Placeholder implementation
            yield from super().parse_article(response)
            
        except Exception as e:
            self.logger.error(f"Error parsing government page {response.url}: {e}")

import re
import html
from bs4 import BeautifulSoup
from typing import Optional
from utils.logger import logger


def html_to_text(html_content: str, max_length: Optional[int] = None) -> str:
    """
    Convert HTML to plain text with optional truncation
    
    Args:
        html_content: HTML string to convert
        max_length: Maximum length of output text (None for no limit)
        
    Returns:
        Plain text string
    """
    if not html_content:
        return ""
    
    try:
        # Decode HTML entities
        text = html.unescape(html_content)
        
        # Use BeautifulSoup for HTML parsing
        soup = BeautifulSoup(text, 'html.parser')
        
        # Remove unwanted tags
        for element in soup(['script', 'style', 'head', 'title', 'meta', '[document]']):
            element.decompose()
        
        # Get text
        text = soup.get_text(separator=' ', strip=True)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Truncate if needed
        if max_length and len(text) > max_length:
            text = text[:max_length].rsplit(' ', 1)[0] + '...'
        
        return text.strip()
        
    except Exception as e:
        logger.warning(f"HTML to text conversion failed: {e}")
        # Fallback: remove HTML tags with regex
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


def slugify(text: str) -> str:
    """
    Convert text to URL-friendly slug
    
    Args:
        text: Input text
        
    Returns:
        URL-friendly slug
    """
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower().strip()
    
    # Replace spaces and underscores with hyphens
    text = re.sub(r'[\s_]+', '-', text)
    
    # Remove special characters, keep only alphanumeric and hyphens
    text = re.sub(r'[^a-z0-9\-]', '', text)
    
    # Remove consecutive hyphens
    text = re.sub(r'\-+', '-', text)
    
    # Remove leading/trailing hyphens
    text = text.strip('-')
    
    return text


def truncate_text(text: str, max_length: int, suffix: str = '...') -> str:
    """
    Truncate text to maximum length
    
    Args:
        text: Input text
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated text
    """
    if not text or len(text) <= max_length:
        return text
    
    # Truncate to nearest word boundary
    truncated = text[:max_length - len(suffix)]
    if ' ' in truncated:
        truncated = truncated.rsplit(' ', 1)[0]
    
    return truncated + suffix


def extract_meta_description(html_content: str, default: str = "") -> str:
    """
    Extract meta description from HTML
    
    Args:
        html_content: HTML string
        default: Default description if not found
        
    Returns:
        Meta description text
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for meta description tag
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return html_to_text(meta_desc['content'], 160)
        
        # Fallback: use first paragraph
        first_p = soup.find('p')
        if first_p:
            return html_to_text(str(first_p), 160)
        
    except Exception as e:
        logger.debug(f"Meta description extraction failed: {e}")
    
    return default


def clean_product_description(description: str) -> str:
    """
    Clean product description for e-commerce display
    
    Args:
        description: Raw product description
        
    Returns:
        Cleaned description
    """
    if not description:
        return ""
    
    # Convert HTML to text
    text = html_to_text(description)
    
    # Remove excessive newlines
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    
    # Remove common unwanted phrases
    unwanted_phrases = [
        r'buy now\b',
        r'shop now\b',
        r'add to cart\b',
        r'limited time offer\b',
        r'free shipping\b',
        r'\*\s*terms and conditions apply\b',
    ]
    
    for phrase in unwanted_phrases:
        text = re.sub(phrase, '', text, flags=re.IGNORECASE)
    
    # Trim and return
    return text.strip()
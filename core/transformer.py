import re
from typing import Any
import html
from bs4 import BeautifulSoup
from utils.logger import logger


class Transformer:
    """Data transformation utilities"""
    
    @staticmethod
    def strip(value: Any) -> str:
        """Strip whitespace from string"""
        if value is None:
            return ''
        return str(value).strip()
    
    @staticmethod
    def slugify(text: str) -> str:
        """Convert text to URL-friendly slug"""
        if not text:
            return ''
            
        # Convert to lowercase
        text = text.lower().strip()
        
        # Replace spaces and underscores with hyphens
        text = re.sub(r'[\s_]+', '-', text)
        
        # Remove special characters, keep only alphanumeric and hyphens
        text = re.sub(r'[^a-z0-9\-]', '', text)
        
        # Remove consecutive hyphens
        text = re.sub(r'\-+', '-', text)
        
        return text
    
    @staticmethod
    def html_to_text(html_content: str) -> str:
        """Convert HTML to plain text"""
        if not html_content:
            return ''
            
        try:
            # Decode HTML entities
            text = html.unescape(html_content)
            
            # Use BeautifulSoup to extract text
            soup = BeautifulSoup(text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
                
            # Get text
            text = soup.get_text()
            
            # Break into lines and remove leading/trailing space on each
            lines = (line.strip() for line in text.splitlines())
            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            # Drop blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            return text
        except Exception as e:
            logger.error(f"HTML to text conversion failed: {e}")
            return html_content
    def clean_html(self, html_content: str) -> str:
        """Clean HTML but keep basic formatting"""
        if not html_content:
            return ""
        
        try:
            allowed_tags = ['p', 'br', 'b', 'strong', 'i', 'em', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'h4']
            soup = BeautifulSoup(html_content, 'html.parser')
            
            for tag in soup.find_all(True):
                if tag.name not in allowed_tags:
                    tag.unwrap() 
            
            return str(soup)
        except Exception:
            return html_content
        
    @staticmethod
    def to_boolean(value: Any) -> bool:
        """Convert various formats to boolean"""
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            value_lower = value.lower().strip()
            return value_lower in ('true', 'yes', '1', 'y', 'on')
        return False
    
    @staticmethod
    def to_integer(value: Any, default: int = 0) -> int:
        """Convert to integer with default fallback"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def to_float(value: Any, default: float = 0.0) -> float:
        """Convert to float with default fallback"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def normalize_price(price: Any, from_currency: str = None, 
                       to_currency: str = 'usd') -> int:
        """
        Normalize price (convert to cents/integer)
        
        Args:
            price: Original price (string, float, int)
            from_currency: Original currency code
            to_currency: Target currency code
            
        Returns:
            int: Price in smallest unit (cents for USD)
        """
        try:
            # Convert to float
            price_float = float(price)
            
            # Convert to cents (assuming USD)
            if to_currency.lower() == 'usd':
                return int(price_float * 100)
            else:
                return int(price_float)
        except Exception as e:
            logger.error(f"Price normalization failed: {e}")
            return 0
    
    @staticmethod
    def truncate(text: str, max_length: int, suffix: str = '...') -> str:
        """Truncate text to maximum length"""
        if not text or len(text) <= max_length:
            return text
            
        return text[:max_length - len(suffix)] + suffix
    
    @staticmethod
    def clean_sku(sku: str) -> str:
        """Clean SKU - remove special characters, normalize"""
        if not sku:
            return ''
            
        # Remove leading/trailing whitespace
        sku = sku.strip()
        
        # Replace multiple spaces with single space
        sku = re.sub(r'\s+', ' ', sku)
        
        # Remove special characters except alphanumeric, dash, underscore
        sku = re.sub(r'[^a-zA-Z0-9\-_]', '', sku)
        
        return sku
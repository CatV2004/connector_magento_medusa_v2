import asyncio
from typing import List, Dict
from datetime import datetime
from utils.logger import logger
from connectors.medusa.medusa_connector import MedusaConnector
import json
from pathlib import Path

class PasswordResetService:
    
    def __init__(self, medusa_connector: MedusaConnector):
        self.medusa = medusa_connector
        
    async def send_reset_emails(self, customer_emails: List[str]) -> Dict:
        results = {
            'total': len(customer_emails),
            'success': 0,
            'failed': 0,
            'failed_emails': []
        }
        
        logger.info(f"Sending password reset emails to {len(customer_emails)} customers...")
        
        for email in customer_emails:
            try:
                # Method 1: Use Medusa's invite endpoint (preferred)
                success = await self._send_customer_invite(email)
                
                if success:
                    results['success'] += 1
                    logger.info(f"✓ Password reset email sent to: {email}")
                else:
                    results['failed'] += 1
                    results['failed_emails'].append(email)
                    logger.error(f"✗ Failed to send reset email to: {email}")
                    
            except Exception as e:
                results['failed'] += 1
                results['failed_emails'].append(email)
                logger.error(f"Error sending reset email to {email}: {e}")
            
            # Rate limiting
            await asyncio.sleep(0.5)
        
        return results
    
    async def _send_customer_invite(self, email: str) -> bool:
        try:
            # Check if customer exists
            customers = await self.medusa.get_customer_by_email(email=email)
            
            if not customers or len(customers) == 0:
                logger.warning(f"Customer not found: {email}")
                return False
            
            customer_id = customers[0]['id']
            
            # Send invitation email
            response = await self.medusa.send_invite(customer_id)
            
            if response and response.get('customer'):
                logger.debug(f"Invite sent for customer {customer_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to send invite to {email}: {e}")
            return False
    
    def generate_reset_report(self, results: Dict) -> str:
        """Generate a report of password reset operations"""
        report_path = Path(f"logs/password_reset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(report_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        return str(report_path)
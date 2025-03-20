import aiohttp
import asyncio
import logging
import os
import re
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('combined_date_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

API_URL = "https://www.hamichlol.org.il/w/api.php"
USERNAME = os.getenv('BOT_USERNAME')
PASSWORD = os.getenv('BOT_PASSWORD')
TEMPLATE_TO_FIND = "תאריך משולב"

class CombinedDateBot:
    def __init__(self):
        self.session = None
        self.edit_token = None
        self.num_requests = 0
        self.log_path = "combined_date_log.txt"
        self.wiki_log_page = "משתמש:נריה_בוט/log-combined-date" 
        self.processed_count = 0
        self.edited_pages = []
        self.error_pages = []
        
        self.template = {
            "name": "תאריך משולב",
            "regex": r"\{\{תאריך משולב\}\}",
            "parameters": [
                {"claim": "P569", "param": "תאריך לידה", "text": ""},
                {"claim": "P570", "param": "תאריך פטירה", "text": ""}
            ]
        }

    async def open_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def __aenter__(self):
        await self.open_session()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close_session()

    async def wiki_request(self, method: str, data: dict, token=None):
        self.num_requests += 1
        await self.open_session()
        data['format'] = 'json'
        if token is not None:
            data['token'] = token

        try:
            async with self.session.request(method, API_URL, 
                                          params=data if method == 'get' else None, 
                                          data=data if method == 'post' else None) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logging.error(f"Error in wiki_request: {str(e)}")
            return None

    async def get_token(self, token_type="csrf"):
        params = {
            "action": "query",
            "meta": "tokens",
            "type": token_type
        }
        response = await self.wiki_request('get', params)
        try:
            return response['query']['tokens'][f'{token_type}token']
        except Exception as e:
            logging.error(f'Error getting {token_type} token: {str(e)}')
            return None

    async def login(self):
        login_token = await self.get_token('login')
        if not login_token:
            logging.error("Failed to get login token")
            return False

        data = {
            "action": "login",
            "lgname": USERNAME,
            "lgpassword": PASSWORD,
            "lgtoken": login_token
        }
        
        response = await self.wiki_request('post', data)
        if response and response.get('login', {}).get('result') == 'Success':
            self.edit_token = await self.get_token()
            logging.info("Successfully logged in and got edit token")
            return True
        logging.error("Login failed")
        return False

    async def get_pages_with_template(self, template_name, continuetoken=None):
        """Return pages containing a specific template"""
        params = {
            'action': 'query',
            'list': 'embeddedin',
            'eititle': f'תבנית:{template_name}',
            'einamespace': '0',
            'eilimit': '500',
            'format': 'json'
        }
        
        if continuetoken:
            params.update(continuetoken)
            
        response = await self.wiki_request('get', params)
        
        if response and 'query' in response:
            pages = response['query']['embeddedin']
            continue_token = response.get('continue', None)
            return pages, continue_token
        
        return [], None

    async def get_all_pages_with_template(self, template_name):
        """Return all pages containing a specific template"""
        continue_token = None
        count = 0
        
        while True:
            pages, continue_token = await self.get_pages_with_template(template_name, continue_token)
            
            for page in pages:
                count += 1
                yield page
                
            if not continue_token:
                break
                
            await asyncio.sleep(0)
            
            if count % 500 == 0:
                logging.info(f"Found {count} pages with the template {template_name}")

    async def get_wikidata_claims(self, title):
        params = {
            'action': 'wbgetentities',
            'sites': 'hewiki',
            'titles': title,
            'props': 'claims|labels|descriptions',
            'languages': 'he',
            'format': 'json'
        }
        
        for attempt in range(3):
            try:
                async with self.session.get('https://www.wikidata.org/w/api.php', 
                                          params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    
            except Exception as e:
                logging.error(f"Wikidata attempt {attempt + 1} failed: {str(e)}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                continue
                
        logging.error(f"Failed to fetch Wikidata claims for {title} after 3 attempts")
        return {}

    async def get_page_content(self, title):
        params = {
            'action': 'query',
            'prop': 'revisions',
            'titles': title,
            'rvprop': 'content',
            'format': 'json'
        }
        
        try:
            response = await self.wiki_request('get', params)
            if not response or 'query' not in response:
                return ""
                
            page = next(iter(response['query']['pages'].values()))
            if 'revisions' not in page:
                return ""
                
            return page['revisions'][0]['*']
            
        except Exception as e:
            logging.error(f"Error getting page content: {str(e)}")
            return ""

    async def save_page(self, title, text, summary):
        if not self.edit_token:
            self.edit_token = await self.get_token()

        data = {
            'action': 'edit',
            'title': title,
            'text': text,
            'summary': summary,
            'token': self.edit_token,
            'bot': '1'
        }
        
        try:
            response = await self.wiki_request('post', data)
            return response and 'error' not in response
        except Exception as e:
            logging.error(f"Error saving page {title}: {str(e)}")
            return False

    def format_date(self, date_value):
        """Format date from Wikidata format to Hebrew format"""
        try:
            if not date_value or 'time' not in date_value:
                return None
                
            time_str = date_value['time'][1:11] 
            date_obj = datetime.strptime(time_str, '%Y-%m-%d')
            
            hebrew_months = {
                1: "בינואר",
                2: "בפברואר",
                3: "במרץ",
                4: "באפריל", 
                5: "במאי",
                6: "ביוני",
                7: "ביולי",
                8: "באוגוסט",
                9: "בספטמבר",
                10: "באוקטובר",
                11: "בנובמבר",
                12: "בדצמבר"
            }
            
            formatted_date = f"{date_obj.day} {hebrew_months[date_obj.month]} {date_obj.year}"
            return formatted_date
            
        except Exception as e:
            logging.error(f"Error formatting date: {str(e)}")
            return None

    def process_template(self, text, wikidata_data):
        """Process only the Combined Date template"""
        if not wikidata_data or 'entities' not in wikidata_data:
            return text
            
        entity_id = next(iter(wikidata_data['entities'].keys()))
        entity_data = wikidata_data['entities'][entity_id]
        
        if 'claims' not in entity_data:
            return text
            
        claims = entity_data['claims']
        
        birth_date = None
        death_date = None
        
        if 'P569' in claims and claims['P569'][0]['mainsnak']['datatype'] == 'time':
            if 'datavalue' in claims['P569'][0]['mainsnak']:
                birth_date = self.format_date(claims['P569'][0]['mainsnak']['datavalue']['value'])
        
        if 'P570' in claims and claims['P570'][0]['mainsnak']['datatype'] == 'time':
            if 'datavalue' in claims['P570'][0]['mainsnak']:
                death_date = self.format_date(claims['P570'][0]['mainsnak']['datavalue']['value'])
        
        if birth_date or death_date:
            template_parts = ["{{תאריך משולב"]
            
            if birth_date:
                template_parts.append(f"|תאריך לידה={birth_date}")
            if death_date:
                template_parts.append(f"|תאריך פטירה={death_date}")
                
            new_template = ''.join(template_parts) + "}}"
            return re.sub(self.template['regex'], new_template, text)
            
        return text

    async def process_page(self, title, content):
        try:
            title_for_wikidata = title.replace('הרב ', '').replace('רבי ', '')
            
            wikidata_data = await self.get_wikidata_claims(title_for_wikidata)
            
            if re.search(self.template['regex'], content):
                new_text = self.process_template(content, wikidata_data)
                return new_text
                
            return content
            
        except Exception as e:
            logging.error(f"Error processing page {title}: {str(e)}")
            return content

    def log_progress(self, message, is_error=False):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"{timestamp}: {message}\n"
        
        if is_error:
            logging.error(message)
            self.error_pages.append(message)
        else:
            logging.info(message)
            if "נערך הדף" in message:
                self.edited_pages.append(message)
        
        try:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception as e:
            logging.error(f"Error writing to local log: {str(e)}")

    async def update_wiki_log(self):
        try:
            log_content = "== עריכות אחרונות ==\n"
            log_content += "\n".join(self.edited_pages[-200:]) if self.edited_pages else "אין עריכות עדיין"
            log_content += "\n\n== שגיאות אחרונות ==\n"
            log_content += "\n".join(self.error_pages[-200:]) if self.error_pages else "אין שגיאות"
            
            success = await self.save_page(
                self.wiki_log_page, 
                log_content,
                f"עדכון לוג אוטומטי - {len(self.edited_pages)} עריכות, {len(self.error_pages)} שגיאות"
            )
            if success:
                logging.info(f"Updated wiki log at {self.processed_count} pages")
        except Exception as e:
            logging.error(f"Error updating wiki log: {str(e)}")

    async def run(self):
        logging.info("התחלת ריצת בוט תאריך משולב")
        self.log_progress("התחלת ריצת בוט תאריך משולב")
        
        try:
            if not await self.login():
                logging.error("Failed to login, stopping bot")
                return

            async for page in self.get_all_pages_with_template(TEMPLATE_TO_FIND):
                try:
                    title = page['title']
                    logging.info(f"מעבד את הדף: {title}")
                    content = await self.get_page_content(title)
                    
                    if not content:
                        continue
                    
                    new_text = await self.process_page(title, content)
                    
                    if new_text != content:
                        if await self.save_page(title, new_text, 'בוט: עדכון ויקינתונים (תאריך משולב)'):
                            self.log_progress(f"נערך הדף: {title}")
                            await asyncio.sleep(1)
                    
                    self.processed_count += 1
                    
                    if self.processed_count % 50 == 0:
                        await self.update_wiki_log()
                    
                except Exception as e:
                    error_msg = f"שגיאה בדף {title}: {str(e)}"
                    self.log_progress(error_msg, is_error=True)
                    continue
                    
        except Exception as e:
            error_msg = f"שגיאה כללית: {str(e)}"
            self.log_progress(error_msg, is_error=True)
            raise
        finally:
            await self.close_session()

if __name__ == "__main__":
    bot = CombinedDateBot()
    asyncio.run(bot.run())

import requests
import json
import os
from datetime import datetime
from time import sleep
import re
import logging
import urllib3
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

API_URL = "https://www.hamichlol.org.il/w/api.php"
USERNAME = "נריה_בוט@test1"
PASSWORD = " " ## סיסמת הבוט

class WikidataBot:
    def __init__(self):
        self.session = requests.Session()
        
        retry_strategy = Retry(
            total=3, 
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.verify = False
        
        self.login()
        
        self.log_path = "log.txt"
        self.wiki_log_page = "משתמש:נריה_בוט/log-wikidata"
        self.processed_count = 0
        self.edited_pages = []
        self.error_pages = []
        
        self.templates = [ ## Customer from https://www.hamichlol.org.il/משתמש:מקוה/ויקינתונים.js
            {
                "name": "ויקישיתוף בשורה",
                "regex": r"\{\{ויקישיתוף\sבשורה\s?\|?\s?\}\}",
                "parameters": [{"claim": "P373", "param": "", "text": "Category:"}]
            },
                        {
                "name": "מיזמים",
                "regex": r"\{\{מיזמים\|?\}\}",
                "parameters": [{"claim": "P373", "param": "ויקישיתוף", "text": "Category:"}]
            },
            {
                "name": "מידע טקסונומי",
                "regex": r"\{\{מידע\sטקסונומי\s?\|?\s?\}\}",
                "parameters": [
                    {"claim": "P815", "param": "ITIS", "text": ""},
                    {"claim": "P685", "param": "NCBI", "text": ""},
                    {"claim": "P4024", "param": "Animal Diversity Web", "text": ""},
                    {"claim": "P2833", "param": "ARKive", "text": ""},
                    {"claim": "P830", "param": "האנציקלופדיה של החיים", "text": ""},
                    {"claim": "P938", "param": "FishBase", "text": ""},
                    {"claim": "P3099", "param": "Internet Bird Collection", "text": ""},
                    {"claim": "P3746", "param": "צמח השדה", "text": ""},
                    {"claim": "P3795", "param": "צמחיית ישראל ברשת", "text": ""},
                    {"claim": "P960", "param": "Tropicos", "text": ""},
                    {"claim": "P846", "param": "GBIF", "text": ""},
                    {"claim": "P1070", "param": "TPList", "text": ""},
                    {"claim": "P961", "param": "IPNI", "text": ""}
                ]
            },
            {
                "name": "בריטניקה",
                "regex": r"\{\{בריטניקה\s?\|?\s?\}\}",
                "parameters": [{"claim": "P1417", "param": "", "text": ""}]
            },
            {
                "name": "Find a Grave",
                "regex": r"\{\{(Find\sa\sGrave|Findagrave|מצא\sקבר)\s?\|?\}\}",
                "parameters": [{"claim": "P535", "param": "", "text": ""}]
            },
            {
                "name": "פירוש נוסף",
                "regex": r"\{\{פירוש\sנוסף\s?\|?\s?\}\}",
                "parameters": [{"claim": "", "param": "נוכחי", "text": ""}]
            },
            {
                "name": "אתר רשמי",
                "regex": r"\{\{אתר\sרשמי\s?\|?\s?\}\}",
                "parameters": [{"claim": "P856", "param": "", "text": ""}]
            },
            {
                "name": "רשתות חברתיות",
                "regex": r"\{\{רשתות\sחברתיות\s?\|?\s?\}\}",
                "parameters": [
                    {"claim": "P2013", "param": "פייסבוק", "text": ""},
                    {"claim": "P2002", "param": "טוויטר", "text": ""},
                    {"claim": "P2003", "param": "אינסטגרם", "text": ""},
                    {"claim": "P7085", "param": "טיקטוק", "text": ""},
                    {"claim": "P3185", "param": "VK", "text": ""},
                    {"claim": "P3789", "param": "טלגרם", "text": ""},
                    {"claim": "P6634", "param": "לינקדין1", "text": ""},
                    {"claim": "P4264", "param": "לינקדין2", "text": ""},
                    {"claim": "P2397", "param": "יוטיוב", "text": ""},
                    {"claim": "P5797", "param": "טוויצ'", "text": ""},
                    {"claim": "P4015", "param": "וימאו", "text": ""},
                    {"claim": "P1581", "param": "בלוג", "text": ""}
                ]
            },
            {
                "name": "פרופילי מדענים",
                "regex": r"\{\{פרופילי\sמדענים\s?\|?\s?\}\}",
                "parameters": [
                    {"claim": "P8024", "param": "פרס נובל", "text": ""},
                    {"claim": "P549", "param": "פרויקט הגנאלוגיה במתמטיקה", "text": ""},
                    {"claim": "P1563", "param": "MacTutor", "text": ""},
                    {"claim": "P2030", "param": "נאס\"א", "text": ""},
                    {"claim": "P2456", "param": "dblp", "text": ""},
                    {"claim": "P2038", "param": "ResearchGate", "text": ""},
                    {"claim": "P1960", "param": "גוגל סקולר", "text": ""},
                    {"claim": "P5715", "param": "Academia", "text": ""},
                    {"claim": "P6479", "param": "IEEE", "text": ""},
                    {"claim": "P3747", "param": "SSRN", "text": ""},
                    {"claim": "P3874", "param": "Justia", "text": ""}
                ]
            },
            {
                "name": "פרופילי מוזיקאים",
                "regex": r"\{\{פרופילי\sמוזיקאים\s?\|?\s?\}\}",
                "parameters": [
                    {"claim": "P4071", "param": "זמרשת", "text": ""},
                    {"claim": "P4034", "param": "שירונט", "text": ""},
                    {"claim": "P2850", "param": "iTunes", "text": ""},
                    {"claim": "P1902", "param": "Spotify", "text": ""},
                    {"claim": "P3040", "param": "SoundCloud", "text": ""},
                    {"claim": "P3192", "param": "Last", "text": ""},
                    {"claim": "P1287", "param": "Komponisten", "text": ""},
                    {"claim": "P1728", "param": "AllMusic", "text": ""},
                    {"claim": "P434", "param": "MusicBrainz", "text": ""},
                    {"claim": "P1989", "param": "MetallumA", "text": ""},
                    {"claim": "P1952", "param": "MetallumB", "text": ""},
                    {"claim": "P2164", "param": "SIGIC", "text": ""},
                    {"claim": "P2514", "param": "Jamendo", "text": ""},
                    {"claim": "P2722", "param": "דיזר", "text": ""},
                    {"claim": "P1553", "param": "יאנדקס", "text": ""},
                    {"claim": "P3674", "param": "מוטופיה", "text": ""},
                    {"claim": "P1953", "param": "Discogs", "text": ""},
                    {"claim": "P3478", "param": "Songkick", "text": ""},
                    {"claim": "P3839", "param": "Tab4u", "text": ""},
                    {"claim": "P2373", "param": "Genius", "text": ""},
                    {"claim": "P3952", "param": "סטריאו ומונו", "text": ""},
                    {"claim": "P3997", "param": "בית לזמר", "text": ""},
                    {"claim": "P2909", "param": "SecondHandSongs", "text": ""},
                    {"claim": "P2510", "param": "DNCI", "text": ""},
                    {"claim": "P3283", "param": "בנדקמפ", "text": ""},
                    {"claim": "P4208", "param": "בילבורד", "text": ""}
                ]
            },
           {
                "name": "פרופילי אנציקלופדיות",
               "regex": r"\{\{פרופילי\sאנציקלופדיות\s?\|?\}\}",
                "parameters": [
                    {"claim": "P8590", "param": "האנציקלופדיה היהודית", "text": ""},
                    {"claim": "P12134", "param": "מסע אל העבר", "text": ""},
                    {"claim": "P3710", "param": "דעת", "text": ""},
                    {"claim": "P10717", "param": "אנציקלופדיה של הרעיונות", "text": ""},
                    {"claim": "P1296", "param": "האנציקלופדיה הקטלאנית", "text": ""},
                    {"claim": "P6337", "param": "Half-Life 2", "text": ""},
                    {"claim": "P2812", "param": "MathWorld", "text": ""},
                    {"claim": "P3012", "param": "אנציקלופדיה איראניקה", "text": ""},
                    {"claim": "P5088", "param": "אנציקלופדיה אינטרנטית לפסיכולוגיה", "text": ""},
                    {"claim": "P1417", "param": "בריטניקה", "text": ""}
                ]
            },
            {
                "name": "פרופילי חברות",
                "regex": r"\{\{פרופילי\sחברות\s?\|?\s?\}\}",
                "parameters": [
                    {"claim": "P2088", "param": "Crunchbase", "text": ""},
                    {"claim": "P5531", "param": "EDGAR", "text": ""}
                ]
            },
            {
                "name": "מידע בורסאי",
                "regex": r"\{\{מידע\sבורסאי\s?\|?\s?\}\}",
                "parameters": [
                    {"claim": "P2088", "param": "Crunchbase", "text": ""},
                    {"claim": "P3377", "param": "בלומברג", "text": ""}
                ]
            },
            {
                "name": "ביו-קונגרס",
                "regex": r"\{\{ביו\-קונגרס\s?\|?\s?\}\}",
                "parameters": [{"claim": "P1157", "param": "", "text": ""}]
            },
            {
                "name": "ביו-נובל",
                "regex": r"\{\{ביו\-נובל\s?\|?\s?\}\}",
                "parameters": [{"claim": "P8024", "param": "", "text": ""}]
            },
            {
                "name": "ביו-נאס\"א",
                "regex": r"\{\{ביו\-נאס\"א\s?\|?\s?\}\}",
                "parameters": [{"claim": "P2030", "param": "", "text": ""}]
            },
            {
                "name": "MathWorld",
                "regex": r"\{\{MathWorld\s?\|?\s?\}\}",
                "parameters": [{"claim": "P2812", "param": "", "text": ""}]
            },
            {
                "name": "גיידסטאר",
                "regex": r"\{\{גיידסטאר\s?\|?\s?\}\}",
                "parameters": [{"claim": "P3914", "param": "", "text": ""}]
            },
            {
                "name": "SIMBAD",
                "regex": r"\{\{SIMBAD\s?\|?\s?\}\}",
                "parameters": [{"claim": "P3083", "param": "", "text": ""}]
            },
            {
                "name": "אתר החכם היומי",
                "regex": r"\{\{אתר\sהחכם\sהיומי\s?\|?\s?\}\}",
                "parameters": [{"claim": "P10776", "param": "", "text": ""}]
            },
            {
                "name": "CIA factbook",
                "regex": r"\{\{CIA factbook\s?\|?\s?\}\}",
                "parameters": [{"claim": "P9948", "param": "", "text": ""}]
            },
            {
                "name": "אנציקלופדיית ההיסטוריה העולמית",
                "regex": r"\{\{אנציקלופדיית ההיסטוריה העולמית\}\}",
                "parameters": [{"claim": "P9000", "param": "", "text": ""}]
            },
            {
                "name": "שם בשפת המקור",
                "regex": r"\{\{שם\sבשפת\sהמקור\s?\|?\s?\}\}",
                "parameters": [
                    {"claim": "P1559", "param": "שם", "text": ""},
                    {"claim": "P1559", "param": "שפה", "text": ""},
                    {"claim": "P443", "param": "קובץ", "text": ""}
                ]
            },
            {
                "name": "דף שער בספרייה הלאומית",
                "regex": r"\{\{דף שער בספרייה הלאומית\}\}",
                "parameters": [{"claim": "P3997", "param": "", "text": ""}]
            },
            {
                "name": "אנצ יהודית",
                "regex": r"\{\{אנצ יהודית\}\}",
                "parameters": [{"claim": "P8590", "param": "", "text": ""}]
            },
        ]
        
    def login(self):
        try:
            r1 = self.session.get(API_URL, params={
                'action': 'query',
                'meta': 'tokens',
                'type': 'login',
                'format': 'json'
            })
            r1.raise_for_status()
            
            if 'query' not in r1.json() or 'tokens' not in r1.json()['query']:
                raise Exception('Invalid login response structure')
            
            r2 = self.session.post(API_URL, data={
                'action': 'login',
                'lgname': USERNAME,
                'lgpassword': PASSWORD,
                'lgtoken': r1.json()['query']['tokens']['logintoken'],
                'format': 'json'
            })
            r2.raise_for_status()
            
            if r2.json()['login']['result'] != 'Success':
                raise Exception('Login failed')
            
            r3 = self.session.get(API_URL, params={
                'action': 'query',
                'meta': 'tokens',
                'format': 'json'
            })
            r3.raise_for_status()
            
            if 'query' not in r3.json() or 'tokens' not in r3.json()['query']:
                raise Exception('Failed to get edit token')
                
            self.edit_token = r3.json()['query']['tokens']['csrftoken']
            logging.info("Successfully logged in and got edit token")
            
        except requests.exceptions.RequestException as e:
            raise Exception(f'Network error during login: {str(e)}')
        except Exception as e:
            raise Exception(f'Login error: {str(e)}')

    def get_all_pages(self):
        continue_param = {} ## משיכת כל הערכים במרחב הראשי
        while True:
            try:
                params = {
                    'action': 'query',
                    'list': 'allpages',
                    'apnamespace': '0',
                    'aplimit': '500',
                    'format': 'json'
                }
                params.update(continue_param)
                
                response = self.session.get(API_URL, params=params)
                response.raise_for_status()
                data = response.json()
                
                if 'query' not in data or 'allpages' not in data['query']:
                    logging.warning("Unexpected API response structure")
                    break
                    
                for page in data['query']['allpages']:
                    yield page
                
                if 'continue' not in data:
                    break
                continue_param = data['continue']
                
                sleep(1)
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error while fetching pages: {str(e)}")
                sleep(5) 
                continue
            except Exception as e:
                logging.error(f"Error fetching pages: {str(e)}")
                break

    def process_another_meaning(self, text, wikidata_data):
        try:
            if not re.search(r"\{\{פירוש\sנוסף\s?\|?\s?\}\}", text):
                return text
                
            if 'descriptions' not in wikidata_data or 'he' not in wikidata_data.get('descriptions', {}):
                return text
                
            description = wikidata_data['descriptions']['he'].get('value', '')
            if description:
                return re.sub(
                    r"\{\{פירוש\sנוסף\s?\|?\s?\}\}",
                    f"{{פירוש נוסף|נוכחי={description}}}",
                    text
                )
            return text
        except Exception as e:
            logging.error(f"Error processing another meaning: {str(e)}")
            return text

    def get_claim_value(self, claim, parameter):
        try:
            if parameter['claim'] == 'P1559':
                if parameter['param'] == 'שפה':
                    return claim['mainsnak']['datavalue']['value']['language']
                return claim['mainsnak']['datavalue']['value']['text'].replace('=', '{{=}}')
            
            if 'datavalue' not in claim['mainsnak']:
                return None
                
            value = claim['mainsnak']['datavalue'].get('value', '')
            if isinstance(value, str):
                return value.replace('=', '{{=}}')
            elif isinstance(value, dict) and 'text' in value:
                return value['text'].replace('=', '{{=}}')
            return str(value)
        except Exception as e:
            logging.error(f"Error extracting claim value: {str(e)}")
            return None

    def process_template(self, text, wikidata_data, template):
        if not wikidata_data or 'claims' not in wikidata_data:
            return text
            
        claims = wikidata_data['claims']
        parameters = []
        
        for param in template['parameters']:
            if not param['claim']:
                continue
                
            if param['claim'] in claims:
                claim_value = self.get_claim_value(claims[param['claim']][0], param)
                if claim_value:
                    param_text = (f"|{param['param']}={claim_value}" if param['param'] 
                                else f"|{param['text']}{claim_value}" if param['text']
                                else f"|{claim_value}")
                    parameters.append(param_text)
        
        if parameters:
            new_template = f"{{{{{template['name']}{''.join(parameters)}}}}}"
            return re.sub(template['regex'], new_template, text, flags=re.IGNORECASE)
        
        return text

    def process_page(self, title, content):
        try:
            title_for_wikidata = title.replace('הרב ', '').replace('רבי ', '')
            
            wikidata_data = self.get_wikidata_claims(title_for_wikidata)
            if not wikidata_data or 'entities' not in wikidata_data:
                return content
                
            entity_data = next(iter(wikidata_data['entities'].values()))
            
            new_text = content
            
            new_text = self.process_another_meaning(new_text, entity_data)
            
            for template in self.templates:
                if re.search(template['regex'], new_text, re.IGNORECASE):
                    new_text = self.process_template(new_text, entity_data, template)
            
            return new_text
            
        except Exception as e:
            logging.error(f"Error processing page {title}: {str(e)}")
            return content

    def get_wikidata_claims(self, title):
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
                response = requests.get('https://www.wikidata.org/w/api.php', 
                                     params=params, 
                                     verify=False)
                response.raise_for_status()
                
                if response.status_code == 200:
                    return response.json()
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < 2:
                    sleep(2 ** attempt) 
                continue
                
            except Exception as e:
                logging.error(f"Error processing Wikidata response: {str(e)}")
                break
                
        logging.error(f"Failed to fetch Wikidata claims for {title} after 3 attempts")
        return {}

    def get_page_content(self, title):
        params = {
            'action': 'query',
            'prop': 'revisions',
            'titles': title,
            'rvprop': 'content',
            'format': 'json'
        }
        
        try:
            response = self.session.get(API_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'query' not in data or 'pages' not in data['query']:
                logging.error(f"Error: Unexpected API response structure for {title}")
                return ""
                
            page = next(iter(data['query']['pages'].values()))
            if 'revisions' not in page:
                logging.error(f"Error: No revisions found for {title}")
                return ""
                
            return page['revisions'][0]['*']
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error getting page content: {str(e)}")
            return ""
        except Exception as e:
            logging.error(f"Error getting page content: {str(e)}")
            return ""

    def save_page(self, title, text, summary):
        try:
            data = {
                'action': 'edit',
                'title': title,
                'text': text,
                'summary': summary,
                'token': self.edit_token,
                'bot': '1',
                'format': 'json'
            }
            
            response = self.session.post(API_URL, data=data)
            response.raise_for_status()
            
            result = response.json()
            return 'error' not in result
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error saving page {title}: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Error saving page {title}: {str(e)}")
            return False

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
        
        if self.processed_count > 0 and self.processed_count % 200 == 0:
            try:
                log_content = "== עריכות אחרונות ==\n"
                log_content += "\n".join(self.edited_pages[-200:])
                log_content += "\n\n== שגיאות אחרונות ==\n"
                log_content += "\n".join(self.error_pages[-200:])
                
                self.save_page(
                    self.wiki_log_page, 
                    log_content,
                    f"עדכון לוג אוטומטי - {len(self.edited_pages)} עריכות, {len(self.error_pages)} שגיאות"
                )
                logging.info(f"Updated wiki log at {self.processed_count} pages")
            except Exception as e:
                logging.error(f"Error updating wiki log: {str(e)}")

    def run(self):
        logging.info("התחלת ריצת הבוט")
        self.log_progress("התחלת ריצת הבוט")
        
        try:
            for page in self.get_all_pages():
                try:
                    title = page['title']
                    logging.info(f"מעבד את הדף: {title}")
                    content = self.get_page_content(title)
                    
                    if not content:
                        continue
                    
                    if not any(re.search(template['regex'], content, re.IGNORECASE) 
                             for template in self.templates):
                        continue
                    
                    new_text = self.process_page(title, content)
                    
                    if new_text != content:
                        if self.save_page(title, new_text, 'שאיבת פרמטרי תבנית מוויקינתונים'):
                            self.log_progress(f"נערך הדף: {title}")
                            sleep(1)
                    
                    self.processed_count += 1
                    
                except Exception as e:
                    error_msg = f"שגיאה בדף {title}: {str(e)}"
                    self.log_progress(error_msg, is_error=True)
                    continue
                    
        except Exception as e:
            error_msg = f"שגיאה כללית: {str(e)}"
            self.log_progress(error_msg, is_error=True)
            raise

if __name__ == "__main__":
    bot = WikidataBot()
    bot.run()

import asyncio
import logging
import time
import requests
from datetime import datetime
import pytz
from typing import Dict, List, Set
from telegram import Bot
from telegram.error import TelegramError
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('live_monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('live_monitoring_bot')

class LiveMonitoringBot:
    def __init__(self):
        # API Configuration
        self.api_token = os.getenv('API_TOKEN')
        self.base_url = "https://api.sportmonks.com/v3/football"
        
        # Telegram Configuration
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.channel_id = os.getenv('TELEGRAM_CHANNEL_ID', '7834082132')
        self.bot = None
        
        # Monitoring Configuration
        self.polling_interval = int(os.getenv('POLLING_INTERVAL_SECONDS', '2'))
        
        # Event IDs for different types
        self.GOAL_EVENT_IDS = [14]  # Goal events
        self.CARD_EVENT_IDS = [19, 20]  # Yellow (19), Red (20) card events
        
        # Cache for processed events and last sent data
        self.processed_events: Set[int] = set()
        self.last_sent: Dict[int, Dict] = {}
        
        # Timezone
        self.timezone = pytz.timezone('Europe/Budapest')
        
        # Whitelisted leagues
        self.whitelisted_leagues = {
            2, 5, 8, 9, 12, 14, 72, 74, 82, 85, 88, 181, 184, 208, 211, 244, 262, 265, 271, 274, 
            292, 295, 301, 304, 313, 325, 360, 363, 372, 375, 384, 387, 444, 447, 453, 456, 462, 
            465, 474, 564, 567, 573, 579, 591, 594, 600, 603, 636, 645, 648, 651, 663, 720, 959, 
            968, 983, 989, 992, 1022, 1025, 1034, 1037, 1203, 1204, 1205, 2286
        }

    async def initialize_telegram(self):
        """Initialize Telegram bot"""
        try:
            if self.telegram_token:
                self.bot = Bot(token=self.telegram_token)
                logger.info("Telegram bot inicializálva")
            else:
                logger.error("Telegram bot token hiányzik")
        except Exception as e:
            logger.error(f"Telegram bot inicializálás hiba: {e}")

    def format_datetime(self, dt_string: str) -> str:
        """Format datetime string to Budapest timezone"""
        try:
            if not dt_string:
                return datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M')
            
            dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)
            
            budapest_dt = dt.astimezone(self.timezone)
            return budapest_dt.strftime('%Y-%m-%d %H:%M')
        except Exception as e:
            logger.error(f"Datetime formázás hiba: {e}")
            return datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M')

    def get_live_matches(self) -> List[Dict]:
        """Get live matches from SportMonks API"""
        try:
            url = f"{self.base_url}/livescores/inplay"
            params = {
                'api_token': self.api_token,
                'include': 'participants;state;statistics;events;league',
                'filters': 'statisticTypes:42,34,41,47'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            matches = data.get('data', [])
            
            logger.info(f"Élő meccsek száma: {len(matches)}")
            return matches
            
        except Exception as e:
            logger.error(f"API hiba: {e}")
            return []

    def filter_matches_by_league(self, matches: List[Dict]) -> List[Dict]:
        """Filter matches by whitelisted leagues"""
        filtered = []
        for match in matches:
            league_id = match.get('league_id')
            if league_id in self.whitelisted_leagues:
                filtered.append(match)
        
        logger.info(f"Szűrt élő meccsek: {len(filtered)}")
        return filtered

    def extract_team_names(self, match: Dict) -> tuple:
        """Extract home and away team names"""
        participants = match.get('participants', [])
        home_team = "Ismeretlen"
        away_team = "Ismeretlen"
        
        for participant in participants:
            meta = participant.get('meta', {})
            location = meta.get('location')
            name = participant.get('name', 'Ismeretlen')
            
            if location == 'home':
                home_team = name
            elif location == 'away':
                away_team = name
        
        return home_team, away_team

    def extract_score(self, match: Dict) -> tuple:
        """Extract current score"""
        participants = match.get('participants', [])
        home_score = 0
        away_score = 0
        
        for participant in participants:
            meta = participant.get('meta', {})
            location = meta.get('location')
            score = meta.get('score', 0)
            
            if location == 'home':
                home_score = score
            elif location == 'away':
                away_score = score
        
        return home_score, away_score

    def extract_league_name(self, match: Dict) -> str:
        """Extract league name"""
        league = match.get('league', {})
        if isinstance(league, dict):
            return league.get('name', 'Ismeretlen liga')
        return 'Ismeretlen liga'

    def extract_statistics(self, match: Dict) -> Dict:
        """Extract match statistics"""
        statistics = match.get('statistics', [])
        stats = {
            'home_yellow_cards': 0,
            'away_yellow_cards': 0,
            'home_red_cards': 0,
            'away_red_cards': 0,
            'home_corners': 0,
            'away_corners': 0
        }
        
        for stat in statistics:
            stat_data = stat.get('data', {})
            location = stat_data.get('location')
            type_id = stat_data.get('type_id')
            value = stat_data.get('value', 0)
            
            if location == 'home':
                if type_id == 42:  # Yellow cards
                    stats['home_yellow_cards'] = value
                elif type_id == 41:  # Red cards
                    stats['home_red_cards'] = value
                elif type_id == 34:  # Corners
                    stats['home_corners'] = value
            elif location == 'away':
                if type_id == 42:  # Yellow cards
                    stats['away_yellow_cards'] = value
                elif type_id == 41:  # Red cards
                    stats['away_red_cards'] = value
                elif type_id == 34:  # Corners
                    stats['away_corners'] = value
        
        return stats

    def extract_events(self, match: Dict) -> List[Dict]:
        """Extract match events"""
        events = match.get('events', [])
        processed_events = []
        
        for event in events:
            event_data = {
                'id': event.get('id'),
                'type_id': event.get('type_id'),
                'minute': event.get('minute'),
                'player_name': event.get('player_name', 'Ismeretlen'),
                'team_name': event.get('participant', {}).get('name', 'Ismeretlen')
            }
            processed_events.append(event_data)
        
        return processed_events

    def has_changes(self, match_id: int, current_data: Dict) -> bool:
        """Check if match data has changed since last check"""
        if match_id not in self.last_sent:
            self.last_sent[match_id] = current_data
            return True
        
        last_data = self.last_sent[match_id]
        
        # Check for score changes
        if (current_data['home_score'] != last_data.get('home_score') or
            current_data['away_score'] != last_data.get('away_score')):
            return True
        
        # Check for statistics changes
        stats_changed = False
        for key in ['home_yellow_cards', 'away_yellow_cards', 'home_red_cards', 'away_red_cards']:
            if current_data.get(key, 0) != last_data.get(key, 0):
                stats_changed = True
                break
        
        if stats_changed:
            return True
        
        return False

    async def send_telegram_message(self, message: str):
        """Send message to Telegram channel"""
        try:
            if self.bot:
                await self.bot.send_message(chat_id=self.channel_id, text=message)
                logger.info(f"Telegram üzenet elküldve: {len(message)} karakter")
            else:
                logger.error("Telegram bot nincs inicializálva")
        except Exception as e:
            logger.error(f"Telegram üzenet küldés hiba: {e}")

    def generate_goal_message(self, match: Dict, event: Dict) -> str:
        """Generate goal message"""
        home_team, away_team = self.extract_team_names(match)
        home_score, away_score = self.extract_score(match)
        league_name = self.extract_league_name(match)
        
        message = f"⚽ GÓL!\n"
        message += f"📆 {datetime.now(self.timezone).strftime('%Y-%m-%d')}\n"
        message += f"🏆 {league_name}\n"
        message += f"🆚 {home_team} {home_score}-{away_score} {away_team}\n"
        message += f"⏱️ {event.get('minute', '?')}. perc\n"
        message += f"👤 {event.get('player_name', 'Ismeretlen')}"
        
        return message

    def generate_card_message(self, match: Dict, event: Dict, card_type: str) -> str:
        """Generate card message"""
        home_team, away_team = self.extract_team_names(match)
        league_name = self.extract_league_name(match)
        
        card_emoji = "🟨" if card_type == "yellow" else "🟥"
        card_text = "SÁRGA LAP" if card_type == "yellow" else "PIROS LAP"
        
        message = f"{card_emoji} {card_text}!\n"
        message += f"📆 {datetime.now(self.timezone).strftime('%Y-%m-%d')}\n"
        message += f"🏆 {league_name}\n"
        message += f"🆚 {home_team} vs {away_team}\n"
        message += f"⏱️ {event.get('minute', '?')}. perc\n"
        message += f"👤 {event.get('player_name', 'Ismeretlen')} ({event.get('team_name', 'Ismeretlen')})"
        
        return message

    async def process_match_events(self, match: Dict):
        """Process events for a single match"""
        match_id = match.get('id')
        events = self.extract_events(match)
        
        for event in events:
            event_id = event.get('id')
            type_id = event.get('type_id')
            
            # Skip if already processed
            if event_id in self.processed_events:
                continue
            
            # Process goal events
            if type_id in self.GOAL_EVENT_IDS:
                message = self.generate_goal_message(match, event)
                await self.send_telegram_message(message)
                self.processed_events.add(event_id)
                logger.info(f"Új gól event: {event_id} - {event.get('minute', '?')}. perc")
            
            # Process card events
            elif type_id in self.CARD_EVENT_IDS:
                card_type = "yellow" if type_id == 19 else "red"
                message = self.generate_card_message(match, event, card_type)
                await self.send_telegram_message(message)
                self.processed_events.add(event_id)
                logger.info(f"Új {card_type} lap event: {event_id} - {event.get('minute', '?')}. perc")

    async def monitoring_loop(self):
        """Main monitoring loop"""
        logger.info("Live monitoring loop elindítva")
        
        while True:
            try:
                # Get live matches
                matches = self.get_live_matches()
                
                if not matches:
                    logger.info("Nincs élő meccs")
                    await asyncio.sleep(self.polling_interval)
                    continue
                
                # Filter by whitelisted leagues
                filtered_matches = self.filter_matches_by_league(matches)
                
                if not filtered_matches:
                    logger.info("Nincs szűrt élő meccs")
                    await asyncio.sleep(self.polling_interval)
                    continue
                
                logger.info(f"Polling ciklus: {len(filtered_matches)} élő meccs")
                
                # Process each match
                for match in filtered_matches:
                    await self.process_match_events(match)
                    
                    # Update cache with current data
                    match_id = match.get('id')
                    home_score, away_score = self.extract_score(match)
                    stats = self.extract_statistics(match)
                    
                    current_data = {
                        'home_score': home_score,
                        'away_score': away_score,
                        **stats
                    }
                    
                    self.last_sent[match_id] = current_data
                
                await asyncio.sleep(self.polling_interval)
                
            except Exception as e:
                logger.error(f"Monitoring loop hiba: {e}")
                await asyncio.sleep(self.polling_interval)

    async def start(self):
        """Start the monitoring bot"""
        logger.info("TippZone Live Monitoring Bot indítása...")
        
        # Initialize Telegram bot
        await self.initialize_telegram()
        
        # Send startup message
        startup_message = "🚀 TippZone Live Bot aktív és készen áll!\n\n"
        startup_message += "⚽ Gólok automatikus értesítése\n"
        startup_message += "🟨 Sárga lapok követése\n"
        startup_message += "🟥 Piros lapok követése\n"
        startup_message += "🔄 2 másodperces frissítés\n"
        startup_message += "🏆 60+ liga figyelése"
        
        try:
            await self.send_telegram_message(startup_message)
        except Exception as e:
            logger.warning(f"Startup üzenet küldés hiba: {e}")
        
        # Start monitoring
        await self.monitoring_loop()

if __name__ == "__main__":
    # Create and run bot
    bot = LiveMonitoringBot()
    
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("Bot leállítva felhasználó által")
    except Exception as e:
        logger.error(f"Bot futtatás hiba: {e}")


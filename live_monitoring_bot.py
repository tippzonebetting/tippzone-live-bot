#!/usr/bin/env python3
"""
TippZone Live Monitoring Bot
Teljes live monitoring rendszer SportMonks API-val és Telegram bot-tal
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
import pytz

import requests
from telegram import Bot
from telegram.ext import Application, CommandHandler, CallbackQueryHandler

# Logging beállítása
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('live_monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LiveMonitoringBot:
    def __init__(self):
        # Környezeti változók betöltése
        self.api_token = os.getenv('API_TOKEN', 'tPmrmBkeuTFf3XBIrMxmyvSpwrxFTtk36ylmeO2Tb5qChpDycm0Ue4dEPaxf')
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN', '7569291815:AAED9PLit3W3vbIrcQyqZpw4lnbk7cFN1ng')
        self.channel_id = os.getenv('TELEGRAM_CHANNEL_ID', '7834082132')
        
        # API beállítások
        self.base_url = "https://api.sportmonks.com/v3/football"
        self.poll_interval = 2  # 2 másodperc polling intervallum
        
        # Cache és duplikáció ellenőrzés
        self.last_sent: Dict[int, Dict] = {}
        self.processed_events: Set[int] = set()
        
        # Whitelist ligák (SportMonks előfizetés alapján)
        self.whitelisted_leagues = {
            2, 5, 2286,  # Champions League, Europa League, Conference League
            8, 9, 12, 14,  # Premier League, Championship, League One, League Two
            82, 85, 88,  # Bundesliga, 2. Bundesliga, 3. Liga
            564, 567,  # La Liga, La Liga 2
            384, 387,  # Serie A, Serie B
            301, 304,  # Ligue 1, Ligue 2
            72, 74,  # Eredivisie, Eerste Divisie
            181, 184,  # Austria: Admiral Bundesliga, 2. Liga
            208, 211,  # Belgium: Pro League, Challenger Pro League
            244,  # Croatia: 1. HNL
            262, 265,  # Czech Republic: Chance Liga, Chance Národní Liga
            271, 274,  # Denmark: Superliga, First Division
            292, 295,  # Finland: Veikkausliiga, Ykkösliiga
            313,  # France: National
            325,  # Greece: Super League
            360, 363,  # Ireland: Premier Division, First Division
            372, 375,  # Israel: Ligat ha'Al, Liga Leumit
            444, 447,  # Norway: Eliteserien, Obos-Ligaen
            453, 456,  # Poland: Ekstraklasa, 1. Liga
            462, 465,  # Portugal: Liga Portugal, Liga Portugal 2
            474,  # Romania: Liga 1
            573, 579,  # Sweden: Allsvenskan, Superettan
            591, 594,  # Switzerland: Super League, Challenge League
            600, 603,  # Turkey: Super Lig, 1. Lig
            636, 645,  # Argentina: Liga Profesional, Primera B Nacional
            648, 651,  # Brazil: Serie A, Serie B
            663,  # Chile: Primera Division
            720,  # WC Qualification Europe
            959,  # UAE League
            968, 983, 989, 992,  # Japan, Canada, China leagues
            1022, 1025, 1034, 1037,  # More Asian leagues
            1203, 1204, 1205  # Italy Serie C groups
        }
        
        # Event type ID-k
        self.GOAL_EVENT_IDS = [14]  # Goal event type
        self.CARD_EVENT_IDS = [19, 20]  # Yellow card, Red card
        
        # Statistic type ID-k (dynamic filters)
        self.STAT_TYPE_IDS = [42, 34, 41, 47]  # Shots, Shots on target, Corners, Yellow cards
        
        # Időzóna
        self.budapest_tz = pytz.timezone('Europe/Budapest')
        
        # Telegram bot
        self.bot = None
        self.application = None
        
        logger.info("LiveMonitoringBot inicializálva")
        logger.info(f"Poll interval: {self.poll_interval} másodperc")
        logger.info(f"Whitelist ligák: {len(self.whitelisted_leagues)} db")
        logger.info(f"Channel ID: {self.channel_id}")

    def _build_url(self, endpoint: str, include: str = "", filters: str = "", per_page: int = 100) -> str:
        """API URL építése"""
        url = f"{self.base_url}/{endpoint}?api_token={self.api_token}"
        if include:
            url += f"&include={include}"
        if filters:
            url += f"&filters={filters}"
        if per_page != 100:
            url += f"&per_page={per_page}"
        return url

    def _make_api_request(self, url: str) -> Optional[Dict]:
        """API kérés végrehajtása"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Rate limit logging
            if 'rate_limit' in data:
                remaining = data['rate_limit'].get('remaining', 'N/A')
                logger.debug(f"Rate limit remaining: {remaining}")
            
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"API kérés hiba: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON dekódolás hiba: {e}")
            return None

    def get_live_matches(self) -> List[Dict]:
        """Élő meccsek lekérése dynamic filters-szel"""
        stat_filters = ",".join(map(str, self.STAT_TYPE_IDS))
        url = self._build_url(
            "livescores/inplay",
            include="participants;state;statistics;events;league",
            filters=f"statisticTypes:{stat_filters}"
        )
        
        logger.debug(f"API URL: {url}")
        data = self._make_api_request(url)
        
        if not data or 'data' not in data:
            logger.warning("Nincs élő meccs adat")
            return []
        
        matches = data['data']
        logger.info(f"Élő meccsek száma: {len(matches)}")
        
        # Whitelist szűrés
        filtered_matches = []
        for match in matches:
            league_id = match.get('league_id')
            if league_id in self.whitelisted_leagues:
                filtered_matches.append(match)
                logger.debug(f"Meccs elfogadva: {match.get('id')} - Liga: {league_id}")
            else:
                logger.debug(f"Meccs kiszűrve: {match.get('id')} - Liga: {league_id}")
        
        logger.info(f"Szűrt élő meccsek: {len(filtered_matches)}")
        return filtered_matches

    def format_datetime(self, dt_str: str) -> str:
        """Dátum formázása Budapest időzónára"""
        try:
            # Parse datetime
            if dt_str.endswith('Z'):
                dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            elif '+' in dt_str or dt_str.endswith('00:00'):
                dt = datetime.fromisoformat(dt_str)
            else:
                # Naive datetime, assume UTC
                dt = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
            
            # Convert to Budapest timezone
            budapest_time = dt.astimezone(self.budapest_tz)
            
            # Hungarian day names
            day_names = {
                'Monday': 'hétfő', 'Tuesday': 'kedd', 'Wednesday': 'szerda',
                'Thursday': 'csütörtök', 'Friday': 'péntek', 'Saturday': 'szombat', 'Sunday': 'vasárnap'
            }
            day_name = day_names.get(budapest_time.strftime('%A'), budapest_time.strftime('%A'))
            
            return budapest_time.strftime(f"%Y-%m-%d %H:%M ({day_name})")
        except Exception as e:
            logger.error(f"Dátum formázás hiba: {e}")
            return dt_str

    def extract_team_names(self, match: Dict) -> tuple:
        """Csapatnevek kinyerése"""
        participants = match.get('participants', [])
        if len(participants) < 2:
            return None, None
        
        home_team = None
        away_team = None
        
        for participant in participants:
            location = participant.get('meta', {}).get('location')
            name = participant.get('name', 'Ismeretlen')
            
            if location == 'home':
                home_team = name
            elif location == 'away':
                away_team = name
        
        return home_team, away_team

    def extract_score(self, match: Dict) -> tuple:
        """Eredmény kinyerése"""
        participants = match.get('participants', [])
        home_score = 0
        away_score = 0
        
        for participant in participants:
            location = participant.get('meta', {}).get('location')
            score = participant.get('meta', {}).get('score', 0)
            
            if location == 'home':
                home_score = score
            elif location == 'away':
                away_score = score
        
        return home_score, away_score

    def extract_match_state(self, match: Dict) -> Dict:
        """Meccs állapot kinyerése"""
        state = match.get('state', {})
        return {
            'minute': state.get('minute'),
            'period': state.get('period'),
            'status': state.get('status')
        }

    def extract_statistics(self, match: Dict) -> Dict:
        """Statisztikák kinyerése location alapján"""
        statistics = match.get('statistics', [])
        stats = {'home': {}, 'away': {}}
        
        stat_mapping = {
            42: 'shots',
            34: 'shots_on_target', 
            41: 'corners',
            47: 'yellow_cards'
        }
        
        for stat in statistics:
            type_id = stat.get('type_id')
            location = stat.get('location', 'home')
            value = stat.get('data', {}).get('value')
            
            if type_id in stat_mapping and value is not None:
                stat_name = stat_mapping[type_id]
                if location in stats:
                    stats[location][stat_name] = value
                    logger.debug(f"Stat: {stat_name} = {value} ({location})")
        
        return stats

    def process_events(self, match: Dict) -> List[Dict]:
        """Events feldolgozása (gólok, lapok)"""
        events = match.get('events', [])
        new_events = []
        
        for event in events:
            event_id = event.get('id')
            type_id = event.get('type_id')
            
            # Skip already processed events
            if event_id in self.processed_events:
                continue
            
            # Process goals
            if type_id in self.GOAL_EVENT_IDS:
                new_events.append({
                    'type': 'goal',
                    'id': event_id,
                    'minute': event.get('minute'),
                    'player': event.get('player_name'),
                    'result': event.get('result'),
                    'participant_id': event.get('participant_id')
                })
                self.processed_events.add(event_id)
                logger.info(f"Új gól event: {event_id}")
            
            # Process cards
            elif type_id in self.CARD_EVENT_IDS:
                card_type = 'yellow' if type_id == 19 else 'red'
                new_events.append({
                    'type': 'card',
                    'card_type': card_type,
                    'id': event_id,
                    'minute': event.get('minute'),
                    'player': event.get('player_name'),
                    'participant_id': event.get('participant_id')
                })
                self.processed_events.add(event_id)
                logger.info(f"Új {card_type} lap event: {event_id}")
        
        return new_events

    def check_for_updates(self, match: Dict) -> List[str]:
        """Frissítések ellenőrzése és üzenetek generálása"""
        fixture_id = match.get('id')
        if not fixture_id:
            return []
        
        # Extract current data
        home_team, away_team = self.extract_team_names(match)
        if not home_team or not away_team:
            return []
        
        home_score, away_score = self.extract_score(match)
        state = self.extract_match_state(match)
        stats = self.extract_statistics(match)
        events = self.process_events(match)
        
        # Get league name
        league_name = match.get('league', {}).get('name', 'Ismeretlen liga')
        starting_at = match.get('starting_at', '')
        
        messages = []
        
        # Initialize cache if not exists
        if fixture_id not in self.last_sent:
            self.last_sent[fixture_id] = {
                'score': f"{home_score}-{away_score}",
                'yellow_home': stats.get('home', {}).get('yellow_cards', 0),
                'yellow_away': stats.get('away', {}).get('yellow_cards', 0),
                'corners_home': stats.get('home', {}).get('corners', 0),
                'corners_away': stats.get('away', {}).get('corners', 0),
                'events_sent': set()
            }
            logger.info(f"Cache inicializálva fixture {fixture_id}-hez")
        
        last_data = self.last_sent[fixture_id]
        
        # Check for goal events
        for event in events:
            if event['type'] == 'goal' and event['id'] not in last_data['events_sent']:
                minute = event.get('minute', '?')
                player = event.get('player', 'Ismeretlen')
                result = event.get('result', f"{home_score}-{away_score}")
                
                message = f"""⚽ GÓL!
📆 {self.format_datetime(starting_at).split(' ')[0]}
🏆 {league_name}
🆚 {home_team} {result} {away_team}
⏱️ {minute}. perc
👤 {player}"""
                
                messages.append(message)
                last_data['events_sent'].add(event['id'])
                logger.info(f"Gól üzenet generálva: {fixture_id} - {minute}. perc")
        
        # Check for card events (sárga lap üzenetek)
        for event in events:
            if event['type'] == 'card' and event['id'] not in last_data['events_sent']:
                minute = event.get('minute', '?')
                player = event.get('player', 'Ismeretlen')
                card_type = event.get('card_type', 'yellow')
                
                # Determine which team
                participant_id = event.get('participant_id')
                team_name = home_team  # default
                if participant_id:
                    # Try to match participant_id to determine team
                    participants = match.get('participants', [])
                    for p in participants:
                        if p.get('id') == participant_id:
                            if p.get('meta', {}).get('location') == 'away':
                                team_name = away_team
                            break
                
                if card_type == 'yellow':
                    card_emoji = "🟨"
                    card_text = "SÁRGA LAP!"
                else:
                    card_emoji = "🟥"
                    card_text = "PIROS LAP!"
                
                message = f"""{card_emoji} {card_text}
📆 {self.format_datetime(starting_at).split(' ')[0]}
🏆 {league_name}
🆚 {home_team} vs {away_team}
⏱️ {minute}. perc
👤 {player} ({team_name})"""
                
                messages.append(message)
                last_data['events_sent'].add(event['id'])
                logger.info(f"{card_type.capitalize()} lap üzenet generálva: {fixture_id} - {minute}. perc")
        
        # Check for score changes (backup for goals)
        current_score = f"{home_score}-{away_score}"
        if current_score != last_data['score']:
            last_data['score'] = current_score
            logger.info(f"Eredmény változás: {fixture_id} - {current_score}")
        
        # Check for significant stat changes (optional)
        current_yellow_home = stats.get('home', {}).get('yellow_cards', 0)
        current_yellow_away = stats.get('away', {}).get('yellow_cards', 0)
        
        if (current_yellow_home > last_data['yellow_home'] or 
            current_yellow_away > last_data['yellow_away']):
            last_data['yellow_home'] = current_yellow_home
            last_data['yellow_away'] = current_yellow_away
            logger.debug(f"Sárga lap változás: {fixture_id}")
        
        return messages

    async def send_telegram_message(self, message: str):
        """Telegram üzenet küldése"""
        try:
            if self.bot:
                await self.bot.send_message(chat_id=self.channel_id, text=message)
                logger.info(f"Telegram üzenet elküldve: {len(message)} karakter")
            else:
                logger.error("Telegram bot nincs inicializálva")
        except Exception as e:
            logger.error(f"Telegram üzenet küldés hiba: {e}")

    async def monitoring_loop(self):
        """Fő monitoring ciklus"""
        logger.info("Monitoring ciklus indítása...")
        
        while True:
            try:
                start_time = time.time()
                
                # Get live matches
                matches = self.get_live_matches()
                logger.info(f"Polling ciklus: {len(matches)} élő meccs")
                
                # Process each match
                total_messages = 0
                for match in matches:
                    fixture_id = match.get('id')
                    messages = self.check_for_updates(match)
                    
                    # Send messages
                    for message in messages:
                        await self.send_telegram_message(message)
                        total_messages += 1
                        await asyncio.sleep(0.5)  # Rate limiting
                
                if total_messages > 0:
                    logger.info(f"Összesen {total_messages} üzenet elküldve")
                
                # Calculate sleep time
                elapsed = time.time() - start_time
                sleep_time = max(0, self.poll_interval - elapsed)
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Monitoring loop hiba: {e}")
                await asyncio.sleep(5)  # Error recovery delay

    async def initialize_bot(self):
        """Telegram bot inicializálása"""
        try:
            self.bot = Bot(token=self.telegram_token)
            
            # Test connection
            bot_info = await self.bot.get_me()
            logger.info(f"Telegram bot inicializálva: @{bot_info.username}")
            
            return True
        except Exception as e:
            logger.error(f"Telegram bot inicializálás hiba: {e}")
            return False

    async def run(self):
        """Fő futtatási metódus"""
        logger.info("TippZone Live Monitoring Bot indítása...")
        
        # Initialize Telegram bot
        if not await self.initialize_bot():
            logger.error("Telegram bot inicializálás sikertelen")
            return
        
        # Send startup message
        startup_message = """🚀 TippZone Live Bot elindult!

⚽ Gólok automatikus értesítése
🟨 Sárga lapok követése  
🟥 Piros lapok követése
🔄 2 másodperces frissítés
🏆 Több mint 60 liga figyelése

Bot aktív és készen áll! 🎯"""
        
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
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot leállítva felhasználó által")
    except Exception as e:
        logger.error(f"Bot futtatás hiba: {e}")

￼

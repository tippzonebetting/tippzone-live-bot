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
        
        # Expanded goal event IDs
        self.GOAL_EVENT_IDS = [14, 15, 16]  # Goal, penalty goal, own goal
        # FINAL POLISH 2: Extended card event IDs including second yellow
        self.CARD_EVENT_IDS = [19, 20, 21]  # Yellow (19), Red (20), Second Yellow (21)
        
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
        """Get live matches from SportMonks API with enhanced retry logic and rate limit handling"""
        try:
            url = f"{self.base_url}/livescores/inplay"
            params = {
                'api_token': self.api_token,
                # OPTIMIZED: Semicolon separator, lineups.player added, statistics removed for speed
                'include': 'participants;state;events;lineups.player;league;scores',
            }
            
            # PRODUCTION FIX 5: Enhanced rate limit handling with exponential backoff
            for attempt in range(3):
                try:
                    response = requests.get(url, params=params, timeout=10)
                    # 429 külön kezelése
                    if response.status_code == 429:
                        wait = 0.8 * (attempt + 1)
                        logger.warning(f"Rate limited (429). Backoff {wait}s")
                        time.sleep(wait)
                        continue
                    response.raise_for_status()
                    break
                except Exception as e:
                    logger.warning(f"API hiba, újrapróbálkozás ({attempt + 1}/3): {e}")
                    if attempt == 2:
                        raise
                    time.sleep(0.5 * (attempt + 1))
            
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
        """Extract current score with simplified logic"""
        home_score = away_score = 0
        
        # Try scores first (most accurate)
        scores = match.get('scores', [])
        for s in scores:
            sc = s.get('score') or {}
            if sc.get('participant') == 'home':
                home_score = sc.get('goals', 0)
            if sc.get('participant') == 'away':
                away_score = sc.get('goals', 0)
        
        # If no scores data, fallback to participants meta
        if home_score == 0 and away_score == 0:
            participants = match.get('participants', [])
            for participant in participants:
                meta = participant.get('meta', {})
                location = meta.get('location')
                score = meta.get('score', 0)
                
                if location == 'home':
                    home_score = score
                elif location == 'away':
                    away_score = score
        
        return home_score, away_score

    def calculate_progressive_score(self, match: Dict, current_event: Dict) -> tuple:
        """PRODUCTION FIX 1: Calculate progressive score with own goal handling and precise sorting"""
        events = match.get('events', [])
        goal_events = [e for e in events if e.get('type_id') in self.GOAL_EVENT_IDS]

        # Pontosabb rendezés: perc, extra perc, event id
        goal_events.sort(key=lambda x: (
            x.get('minute') or 0,
            x.get('extra_minute') or 0,
            x.get('id') or 0
        ))

        participants = match.get('participants', [])
        home_team_id = away_team_id = None
        for p in participants:
            loc = p.get('meta', {}).get('location')
            if loc == 'home':
                home_team_id = p.get('id')
            elif loc == 'away':
                away_team_id = p.get('id')

        # Előre elkérjük a player→team mapet is
        player_team_map, player_to_team_id = self.get_player_team_mapping(match)

        cur_m = current_event.get('minute') or 0
        cur_x = current_event.get('extra_minute') or 0
        cur_id = current_event.get('id') or 0

        h = a = 0
        for g in goal_events:
            gm = g.get('minute') or 0
            gx = g.get('extra_minute') or 0
            gid = g.get('id') or 0
            # csak a jelenlegi eseményig számolunk
            if (gm, gx, gid) > (cur_m, cur_x, cur_id):
                break

            g_type = g.get('type_id')
            tid = g.get('team_id') or g.get('participant_id')
            loc = g.get('location')
            scored_side = None

            # 1) team_id/participant_id
            if tid and tid == home_team_id:
                scored_side = 'home'
            elif tid and tid == away_team_id:
                scored_side = 'away'
            # 2) location
            elif loc == 'home':
                scored_side = 'home'
            elif loc == 'away':
                scored_side = 'away'
            # 3) player → team
            else:
                pid = g.get('player_id')
                if pid and pid in player_to_team_id:
                    if player_to_team_id[pid] == home_team_id:
                        scored_side = 'home'
                    elif player_to_team_id[pid] == away_team_id:
                        scored_side = 'away'
                else:
                    pn = g.get('player_name')
                    tname = player_team_map.get(pn)
                    if tname:
                        for p in participants:
                            if p.get('name') == tname:
                                scored_side = 'home' if p.get('meta', {}).get('location') == 'home' else 'away'
                                break

            # Ha nem tudtuk beazonosítani, lépjünk tovább
            if not scored_side:
                continue

            # ÖNGÓL kezelése: a másik oldalra írjuk
            if g_type == 16:  # own goal
                scored_side = 'home' if scored_side == 'away' else 'away'

            if scored_side == 'home':
                h += 1
            else:
                a += 1

        logger.info(f"Progressive score @ {cur_m}+{cur_x}: {h}-{a}")
        return h, a

    def extract_league_name(self, match: Dict) -> str:
        """Extract league name"""
        league = match.get('league', {})
        if isinstance(league, dict):
            return league.get('name', 'Ismeretlen liga')
        return 'Ismeretlen liga'

    def get_player_team_mapping(self, match: Dict) -> tuple:
        """Create enhanced player-team mapping"""
        player_team_map = {}
        player_to_team_id = {}
        
        # Try lineups first (most accurate)
        lineups = match.get('lineups', [])
        for lineup in lineups:
            team_id = lineup.get('participant_id')
            team_name = lineup.get('participant', {}).get('name', 'Ismeretlen')
            players = lineup.get('players', [])
            
            for player_data in players:
                player = player_data.get('player', {})
                player_name = player.get('name')
                player_id = player.get('id')
                
                if player_name:
                    player_team_map[player_name] = team_name
                if player_id and team_id:
                    player_to_team_id[player_id] = team_id
        
        return player_team_map, player_to_team_id

    def display_minute(self, event: Dict, state: Dict) -> str:
        """Enhanced minute display with proper formatting"""
        m = event.get('minute') or (state or {}).get('minute') or 0
        added = event.get('extra_minute') or (state or {}).get('added_time') or 0
        period = ((state or {}).get('period') or '').upper()

        if period in ('1H', 'H1'):
            return f"45+{added}. perc" if m >= 45 and added > 0 else f"{m}. perc"
        if period in ('2H', 'H2'):
            return f"90+{added}. perc" if m > 90 and added > 0 else f"{m}. perc"
        if period in ('ET', 'AET', 'E1', 'E2'):
            return f"ET {m}. perc"
        if period in ('PEN', 'PSO'):
            return "PEN"
        return f"{m}. perc"

    def extract_events(self, match: Dict) -> List[Dict]:
        """Extract match events with enhanced team identification and VAR filtering"""
        events = match.get('events', [])
        
        # PRODUCTION FIX 2: VAR/cancelled event filtering
        valid_events = []
        for e in events:
            res = (e.get('result') or '').lower()
            # kiszűrjük a visszavont/érvénytelen eseményeket
            if res in ('cancelled', 'disallowed', 'void'):
                continue
            valid_events.append(e)
        events = valid_events
        
        processed_events = []
        
        # Get player-team mappings
        player_team_map, player_to_team_id = self.get_player_team_mapping(match)
        participants = match.get('participants', [])
        
        for event in events:
            player_name = event.get('player_name', 'Ismeretlen')
            
            # Enhanced team identification with multiple fallback methods
            pid = event.get('player_id')
            tid = event.get('team_id') or event.get('participant_id')
            loc = event.get('location')
            team_name = 'Ismeretlen'

            # Try team_id/participant_id first
            if tid:
                team_name = next((p['name'] for p in participants if p.get('id') == tid), team_name)
            # Try player_id mapping
            elif pid and pid in player_to_team_id:
                tid = player_to_team_id[pid]
                team_name = next((p['name'] for p in participants if p.get('id') == tid), team_name)
            # Try location mapping
            elif loc in ('home', 'away'):
                team_name = next((p['name'] for p in participants 
                                if p.get('meta', {}).get('location') == loc), team_name)
            # Fallback to player name mapping
            elif player_name in player_team_map:
                team_name = player_team_map[player_name]
            
            # PRODUCTION FIX 3: Extra fallback for team identification
            if team_name == 'Ismeretlen':
                # extra fallback: néha van team_name közvetlenül az eventben
                tn = event.get('team_name')
                if isinstance(tn, str) and tn.strip():
                    team_name = tn.strip()
            
            # Log "Ismeretlen" team cases for diagnostics (kept as requested)
            if team_name == 'Ismeretlen':
                logger.warning(f"team_resolve_failed fixture={match.get('id')} "
                             f"evt={{'id':{event.get('id')},'type_id':{event.get('type_id')},"
                             f"'player_id':{event.get('player_id')},'participant_id':{event.get('participant_id')},"
                             f"'location':'{event.get('location')}'}}")
            
            event_data = {
                'id': event.get('id'),
                'type_id': event.get('type_id'),
                'minute': event.get('minute'),
                'player_name': player_name,
                'player_id': pid,
                'team_name': team_name
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
        """Generate goal message with progressive score calculation"""
        home_team, away_team = self.extract_team_names(match)
        
        # PRODUCTION: Use progressive score calculation instead of current match score
        home_score, away_score = self.calculate_progressive_score(match, event)
        
        league_name = self.extract_league_name(match)
        state = match.get('state', {})
        
        # Clean format with progressive score
        message = f"⚽ GÓL!\n"
        message += f"📆 {datetime.now(self.timezone).strftime('%Y-%m-%d')}\n"
        message += f"🏆 {league_name}\n\n"
        message += f"{home_team} {home_score}–{away_score} {away_team}\n"
        message += f"⏱️ {self.display_minute(event, state)}\n"
        message += f"👤 {event.get('player_name', 'Ismeretlen')} ({event.get('team_name', 'Ismeretlen')})"
        
        return message

    def generate_card_message(self, match: Dict, event: Dict, card_type: str) -> str:
        """Generate card message with consistent formatting"""
        home_team, away_team = self.extract_team_names(match)
        league_name = self.extract_league_name(match)
        state = match.get('state', {})
        
        # Handle different card types including second yellow
        if card_type == "yellow":
            card_emoji = "🟨"
            card_text = "SÁRGA LAP"
        elif card_type == "second_yellow":
            card_emoji = "🟨🟥"
            card_text = "MÁSODIK SÁRGA LAP"
        else:  # red
            card_emoji = "🟥"
            card_text = "PIROS LAP"
        
        # Consistent format with goal messages
        message = f"{card_emoji} {card_text}!\n"
        message += f"📆 {datetime.now(self.timezone).strftime('%Y-%m-%d')}\n"
        message += f"🏆 {league_name}\n\n"
        message += f"{home_team} vs {away_team}\n"
        message += f"⏱️ {self.display_minute(event, state)}\n"
        message += f"👤 {event.get('player_name', 'Ismeretlen')} ({event.get('team_name', 'Ismeretlen')})"
        
        return message

    async def process_match_events(self, match: Dict):
        """Process events for a single match"""
        match_id = match.get('id')
        events = self.extract_events(match)
        
        for event in events:
            event_id = event.get('id')
            type_id = event.get('type_id')
            
            # Skip if already processed (debounce protection)
            if event_id in self.processed_events:
                continue
            
            # Process goal events
            if type_id in self.GOAL_EVENT_IDS:
                message = self.generate_goal_message(match, event)
                await self.send_telegram_message(message)
                self.processed_events.add(event_id)
                logger.info(f"Új gól event: {event_id} - {self.display_minute(event, match.get('state', {}))}")
            
            # Process card events with enhanced second yellow detection
            elif type_id in self.CARD_EVENT_IDS:
                if type_id == 19:
                    card_type = "yellow"
                elif type_id == 21:
                    card_type = "second_yellow"
                else:  # type_id == 20
                    card_type = "red"
                
                message = self.generate_card_message(match, event, card_type)
                await self.send_telegram_message(message)
                self.processed_events.add(event_id)
                logger.info(f"Új {card_type} lap event: {event_id} - {self.display_minute(event, match.get('state', {}))}")

    async def monitoring_loop(self):
        """Main monitoring loop with enhanced memory cleanup"""
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
                    
                    # Statistics temporarily disabled for speed
                    current_data = {
                        'home_score': home_score,
                        'away_score': away_score,
                    }
                    
                    self.last_sent[match_id] = current_data
                
                # PRODUCTION FIX 4 + FINAL POLISH 1: Enhanced memory cleanup for finished matches
                for match in filtered_matches:
                    st = (match.get('state') or {}).get('status') or ''
                    if st in ('FT', 'AET', 'FT_PEN', 'FT_ET', 'ENDED'):
                        # eltávolítjuk az adott fixture eventjeit a processed_events-ből
                        mids = {e.get('id') for e in (match.get('events') or [])}
                        self.processed_events.difference_update(mids)
                        # FINAL POLISH 1: Clean up last_sent dict as well
                        self.last_sent.pop(match.get('id'), None)
                        logger.info(f"Cleanup: removed {len(mids)} events for finished match {match.get('id')}")
                
                await asyncio.sleep(self.polling_interval)
                
            except Exception as e:
                logger.error(f"Monitoring loop hiba: {e}")
                await asyncio.sleep(self.polling_interval)

    async def start(self):
        """Start the monitoring bot"""
        logger.info("TippZone Live Monitoring Bot indítása...")
        
        # Fail-fast token validation
        if not self.api_token:
            logger.error("API_TOKEN hiányzik – állj le.")
            return
        if not self.telegram_token:
            logger.error("TELEGRAM_BOT_TOKEN hiányzik – állj le.")
            return
        
        # Diagnostic logging at startup
        logger.info(f"Start: poll={self.polling_interval}s, leagues={len(self.whitelisted_leagues)}, channel={self.channel_id}")
        
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


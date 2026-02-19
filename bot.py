```python
import os
import csv
import time
import requests
from io import StringIO
from difflib import SequenceMatcher
from threading import Thread, Lock
from flask import Flask
from telegram import Update, ChatMember
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ChatAction
import re
from datetime import datetime
import sqlite3

# ======================================
# CONFIG
# ======================================

BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
DHV_GROUP_ID = -1003380502617
KNOWLEDGE_BASE_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQzrWJB3NLfDRNaademLctv5Iy11vF9XdywjAYk3zpB1AjsqD7BOryzHYiPuJXR6nTcShC-sIBnDy/pub?output=csv"
UNKNOWN_Q_CSV = "unknown_questions.csv"
FALLBACK_DB = "fallback_memory.db"
KB_REFRESH_INTERVAL = 300
SIMILARITY_THRESHOLD = 0.85

knowledge_base = []
kb_lock = Lock()
unknown_lock = Lock()
db_lock = Lock()
kb_last_loaded = 0

# ======================================
# DATABASE INITIALIZATION
# ======================================

def init_db():
    """Initialize the local fallback memory database"""
    with db_lock:
        conn = sqlite3.connect(FALLBACK_DB)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS
            AUTOINCREMENT,
            question TEXT UNIQUE,
            normalized_question TEXT,
            answer TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        conn.commit()
        conn.close()
        print("[DB] ✅ Fallback memory initialized")

def get_local_fallback(question):
    """Check local SQLite for previously answered questions"""
    normalized_q = normalize_text(question)
    if not normalized_q:
        return None
    
    try:
        with db_lock:
            conn = sqlite3.connect(FALLBACK_DB)
            cursor = conn.cursor()
            
            # Try exact normalized match
            cursor.execute("SELECT answer FROM fallback_memory WHERE normalized_question = ?", (normalized_q,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return row[0]
        
        # Try fuzzy matching within the DB
        with db_lock:
            conn = sqlite3.connect(FALLBACK_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT normalized_question, answer FROM fallback_memory")
            rows = cursor.fetchall()
            conn.close()
        
        best_score = 0
        best_answer = None
        
        for norm_q, ans in rows:
            score = SequenceMatcher(None, normalized_q, norm_q).ratio()
            if score > best_score:
                best_score = score
                best_answer = ans
        
        if best_score >= SIMILARITY_THRESHOLD:
            return best_answer
        
        return None
    
    except Exception as e:
        print(f"[DB] ❌ Error reading local memory: {e}")
        return None

def save_to_local_fallback(question, answer):
    """Save a new question-answer pair to local SQLite"""
    normalized_q = normalize_text(question)
    if not normalized_q or not answer:
        return
    
    try:
        with db_lock:
            conn = sqlite3.connect(FALLBACK_DB)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO
                fallback_memory (question, normalized_question, answer)
                VALUES (?, ?, ?)
            """, (question, normalized_q, answer))
            
            conn.commit()
            conn.close()
            print(f"[DB] ✅ Saved to local memory: {question[:30]}...")
    
    except Exception as e:
        print(f"[DB] ❌ Error saving to local memory: {e}")

# ======================================
# KNOWLEDGE BASE MANAGEMENT
# ======================================

def load_knowledge_base():
    """Load Knowledge base from Google Sheet"""
    global knowledge_base, kb_last_loaded
    try:
        response = requests.get(KNOWLEDGE_BASE_URL, timeout=10)
        response.raise_for_status()
        
        if not response.text.strip():
            print("[KB] ⚠️ Google Sheet returned empty response")
            return False
        
        reader = csv.DictReader(StringIO(response.text))
        
        # Strip BOM and invisible characters from field names
        fieldnames = [name.encode('ascii', 'ignore').decode('ascii').strip() for name in reader.fieldnames or []]
        
        temp_kb = []
        for row in reader:
            # Flexible mapping to handle various header formats
            q = ""
            r = ""
            
            for k, v in row.items():
                clean_k = k.encode('ascii', 'ignore').decode('ascii').strip().lower() for name in (reader.fieldnames or [])
            
            temp_kb = []
            for row in reader:
                q = row.get("Question", "").strip()
                r = row.get("Response", "").strip()
                
                if q:
                    temp_kb.append({
                        'Question': q,
                        'Response': r if r else "I found a match but the response is empty.",
                        'Category': row.get("Category", "").strip(),
                        'Tags': row.get("Tags", "").strip(),
                        'Status': row.get("Status", "").strip()
                    })
        
        with kb_lock:
            knowledge_base = temp_kb
        
        kb_last_loaded = time.time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[KB] ✅ Loaded {len(knowledge_base)} entries at {timestamp}")
        return True
    
    except requests.exceptions.Timeout:
        print("[KB] ❌ Google Sheet request timed out")
        return False
    except requests.exceptions.RequestException as e:
        print(f"[KB] ❌ Network error: {e}")
        return False
    except Exception as e:
        print(f"[KB] ❌ Failed to parse knowledge base: {e}")
        return False

def kb_auto_refresher():
    """Refresh KB with exponential backoff on failures"""
    failure_count = 0
    max_backoff = 1000
    
    while True:
        try:
            if load_knowledge_base():
                failure_count = 0
                time.sleep(KB_REFRESH_INTERVAL)
            else:
                failure_count += 1
                backoff = min(60 * (2 ** failure_count), max_backoff)
                print(f"[KB] 🔁 Retrying in {backoff}s (attempt #{failure_count})")
                time.sleep(backoff)
        except Exception as e:
            print(f"[KB] ❌ Refresher error: {e}")
            time.sleep(60)

# ======================================
# UNKNOWN QUESTIONS LOGGER
# ======================================

def init_unknown_csv():
    """Initialize unknown questions CSV file"""
    if not os.path.exists(UNKNOWN_Q_CSV):
        with open(UNKNOWN_Q_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "User_ID", "Username", "Question", "Source"])
        print(f"[LOGGER] ✅ Created {UNKNOWN_Q_CSV}")

def is_similar(a, b, threshold=SIMILARITY_THRESHOLD):
    """Check if two strings are similar enough"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() > threshold

def log_unknown_question(user_id, username, question, source="sheet"):
    """Log unknown question to CSV"""
    with unknown_lock:
        try:
            # Log the question even if similar exists, as per requirements for tracking
            # with open(UNKNOWN_Q_CSV, "r", newline="", encoding="utf-8") as f:
            #     reader = csv.reader(f)
            #     next(reader, None)  # Skip header
            #     existing = [row[3] if len(row) > 3 else "" for row in reader]
            # except:
            #     existing = []
            
            # # Check if similar question already logged
            # for q in existing:
            #     if is_similar(question, q):
            #         return
            
            with open(UNKNOWN_Q_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                timestamp = datetime.now().isoformat()
                writer.writerow([timestamp, user_id, username, question, source])
            
            print(f"[UNKNOWN] ✅ Logged from @{username} ({user_id}) [Source: {source}]: {question[:50]}...")
        
        except Exception as e:
            print(f"[LOGGER] ❌ Failed to log question: {e}")

# ======================================
# MEMBERSHIP VERIFICATION
# ======================================

async def verify_membership(context, user_id):
    """Verify if user is a valid member of DHV group"""
    try:
        member = await context.bot.get_chat_member(DHV_GROUP_ID, user_id)
        
        # In python-telegram-bot v20+,
        # member.status is a string.
        # Possible values: 'creator', 'administrator', 'member', 'restricted', 'left', 'kicked'
        
        status = member.status
        
        # User is kicked - deny
        if status == "kicked":
            return False, (
                "❌ Your DHV subscription has ended or you are no longer a member of the community.\n\n"
                "Please check your email for the subscription link to continue enjoying our features."
            )
        
        # User is restricted and can't send messages in group
        if status == "restricted":
            if hasattr(member, 'can_send_messages') and not member.can_send_messages is False:
                return False, (
                    "⚠️ Your DHV subscription is currently restricted.\n\n"
                    "Please check your email for the subscription link to regain access and continue enjoying the community's features."
                )
        
        # User isn't in group - deny
        if status == "left":
            return False, "❌ You are not a member of the DHV community.\n"
        
        # User is a valid member (member, administrator, creator, or restricted but can send messages)
        return True, ""
    
    except Exception as e:
        print(f"[VERIFY] ❌ Error verifying user {user_id}: {e}")
        return False, "❌ Could not verify your membership. Please try again later."

# ======================================
# ANSWER FINDING
# ======================================

def normalize_text(text):
    """Normalize text for comparison: strip hidden characters, lowercase, etc"""
    if not text:
        return ""
    # Strip non-printable characters and handle various encodings
    text = "".join(char for char in text if char.isprintable())
    text = text.lower()
    # Replace punctuation with spaces to avoid merging words
    text = re.sub(r"[^\w\s]", " ", text)
    # Collapse multiple spaces and strip
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def get_groq_response(question):
    """Fallback to Groq API when sheet answer isn't found"""
    if not GROQ_API_KEY:
        print("[GROQ] ⚠️ API Key missing")
        return None
    
    try:
        print(f"[GROQ] 🤖 Requesting fallback answer for: {question[:50]}...")
        url = "https://api.groq.com/openai/v1/chat/completions"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {GROQ_API_KEY}"
        }
        
        data = {
            "messages": [
                {"role": "system", "content": "You are DHV OS, a professional AI assistant for the DHV sales and digital hustle community. Provide helpful, concise, and professional advice. Always stay in character as a hustle-focused digital mentor."},
                {"role": "user", "content": question}
            ],
            "model": "llama-3.3-70b-versatile",
            "temperature": 0.5
        }
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if "choices" in result and len(result["choices"]) > 0:
                ans = result["choices"][0]["message"]["content"].strip()
                print(f"[GROQ] ✅ Success with model: {model}")
                save_to_local_fallback(question, ans)
                return ans
        else:
            print(f"[GROQ] ⚠️ Model {model} failed ({response.status_code}): {response.text}")
        
        # If it's a model not found error (400 or 404), we continue to the next model
        if response.status_code in [400, 404]:
            continue
        # For other errors (auth, quota, etc.), stop and return None
        return None
    
    except Exception as e:
        print(f"[GROQ] ❌ Request Exception for {model}: {e}")
        continue
    
    return None

def get_openai_response(question):
    """Fallback to OpenAI API***"""
    if not OPENAI_API_KEY:
        print("[OPENAI] ⚠️ API Key missing")
        return None
    
    try:
        print(f"[OPENAI] 🤖 Requesting fallback answer for: {question[:50]}...")
        url = "https://api.openai.com/v1/chat/completions"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}"
        }
        
        data = {
            "messages": [
                {"role": "system", "content": "You are DHV OS, a professional AI assistant for the DHV sales and digital hustle community. Provide helpful, concise, and professional advice. Always stay in character as a hustle-focused digital mentor."},
                {"role": "user", "content": question}
            ],
            "model": "gpt-4o-mini",
            "temperature": 0.5
        }
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code == 200:
            ans = response.json()["choices"][0]["message"]["content"].strip()
            print(f"[OPENAI] ✅ Success")
            save_to_local_fallback(question, ans)
            return ans
        else:
            print(f"[OPENAI] ⚠️ Failed ({response.status_code}): {response.text}")
    
    except Exception as e:
        print(f"[OPENAI] ❌ Exception: {e}")
    
    return None

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

def get_anthropic_response(question):
    """Fallback to Anthropic API***"""
    if not ANTHROPIC_API_KEY:
        print("[ANTHROPIC] ⚠️ API Key missing")
        return None
    
    try:
        print(f"[ANTHROPIC] 🤖 Requesting fallback answer for: {question[:50]}...")
        url = "https://api.anthropic.com/v1/messages"
        
        headers = {
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01"
        }
        
        data = {
            "model": "claude-3-haiku-20240307",
            "max_tokens": 1024,
            "system": "You are DHV OS, a professional AI assistant for the DHV sales and digital hustle community. Provide helpful, concise, and professional advice. Always stay in character as a hustle-focused digital mentor.",
            "messages": [{"role": "user", "content": question}]
        }
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code == 200:
            ans = response.json()["content"][0]["text"].strip()
            print(f"[ANTHROPIC] ✅ Success")
            save_to_local_fallback(question, ans)
            return ans
        else:
            print(f"[ANTHROPIC] ⚠️ Failed ({response.status_code}): {response.text}")
    
    except Exception as e:
        print(f"[ANTHROPIC] ❌ Exception: {e}")
    
    return None

def find_answer(question):
    """Find best matching answer from knowledge base with multi-AI fallback"""
    try:
        # Step 1: Check Google Sheet Knowledge Base First
        if not knowledge_base:
            print("[FIND] ⚠️ Knowledge base is empty")
        else:
            raw_q = question.strip().lower()
            with kb_lock:
                for entry in knowledge_base:
                    if entry["Question"].strip().lower() == raw_q:
                        return entry["Response"], "sheet"
        
        # 2. Normalized & Fuzzy Matching
        normalized_q = normalize_text(question)
        if not normalized_q:
            return None, None
        
        best_match = None
        best_score = 0
        
        with kb_lock:
            for entry in knowledge_base:
                normalized_kb_q = normalize_text(entry["Question"])
                
                if not normalized_kb_q:
                    continue
                
                if normalized_q == normalized_kb_q:
                    return entry["Response"], "sheet"
                
                if normalized_kb_q in normalized_q or normalized_q in normalized_kb_q:
                    return entry["Response"], "sheet"
                
                score = SequenceMatcher(None, normalized_q, normalized_kb_q).ratio()
                if score > best_score:
                    best_score = score
                    best_match = entry
        
        if best_score >= SIMILARITY_THRESHOLD and best_match:
            return best_match["Response"], "sheet"
        
        # Step 2: Check local fallback memory (SQLite)
        local_ans = get_local_fallback(question)
        if local_ans:
            print(f"[FIND] ✅ Local fallback match found")
            return local_ans, "local"
        
        # Step 3: Multi-AI Sequential Fallback
        # 3.1 Groq (Primary)
        ans = get_groq_response(question)
        if ans: return ans, "groq"
        
        # 3.2 OpenAI (secondary)
        ans = get_openai_response(question)
        if ans: return ans, "openai"
        
        # 3.3 Anthropic (Tertiary)
        ans = get_anthropic_response(question)
        if ans: return ans, "anthropic"
        
        return None, None
    
    except Exception as e:
        print(f"[FIND] ❌ Error: {e}")
        return None, None

# ======================================
# TELEGRAM HANDLERS
# ======================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    try:
        user_id = update.message.from_user.id
        
        # Verify membership
        is_member, error_msg = await verify_membership(context, user_id)
        if not is_member:
            await update.message.reply_text(error_msg)
            return
        
        welcome_text = (
            "👋 Welcome to DHV OS!\n\n"
            "I'm your AI assistant designed to make your digital hustle smarter and faster. 🚀\n\n"
            "Here's what you can do with me:\n"
            "• Ask me anything in DM and get instant guidance from our knowledge base 📚\n"
            "• Tag me in the DHV group to answer questions and provide insights 💡\n"
            "• Stay updated with tips, strategies, and daily growth hacks for your online business 📈\n\n"
            "🚀 Pro Tip: The more you interact with me, the smarter we get. Let's make your digital hustle unstoppable! 💪"
        )
        
        await update.message.reply_text(welcome_text)
        print(f"[START] User {user_id} started the bot")
    
    except Exception as e:
        print(f"[START] ❌ Error: {e}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages"""
    try:
        if not update.message or not update.message.text or not update.message.chat:
            return
        
        user = update.message.from_user
        if not user:
            return
        
        user_id = user.id
        username = update.message.from_user.username or "Unknown"
        text = update.message.text.strip()
        chat_type = update.message.chat.type
        is_group = chat_type in ["group", "supergroup"]
        
        # Verify membership (blocks DM if not a member)
        is_member, error_msg = await verify_membership(context, user_id)
        if not is_member:
            await update.message.reply_text(error_msg)
            except:
                pass
            return
        
        # Group-specific handling
        if is_group:
            # Only respond in the designated group
            if update.message.chat.id != DHV_GROUP_ID:
                return
            
            # Check if bot was mentioned
            try:
                bot_username = (await context.bot.get_me()).username.lower()
            except:
                return
            
            if f"@{bot_username}" not in text.lower():
                return
            
            # Remove mention and get the question
            text = text.lower().replace(f"@{bot_username}", "").strip()
        
        # Empty message after processing
        if not text:
            return
        
        # Show typing indicator
        try:
            await update.message.chat.send_action(ChatAction.TYPING)
        except:
            pass
        
        # Find answer
        answer_result, source = find_answer(text)
        
        # DEBUG: Log result of find_answer
        if answer_result and answer_result.strip():
            print(f"[HANDLER] Found answer for '{text}' [Source: {source}]")
        
        if answer_result and answer_result.strip():
            await update.message.reply_text(answer_result)
            print(f"[ANSWER] Sent to @{username} ({user_id}) in {'group' if is_group else 'DM'}")
        else:
            # Log unknown question (source is sheet because fallback also failed)
            log_unknown_question(user_id, username, text, source="groq, local, anthropic")
            await update.message.reply_text(
                (
                    "🤔 Hmm... I couldn't find an answer for that right now.\n\n"
                    "Don't worry - your question has been logged, and our team or I will update the knowledge base soon!\n\n"
                    "In the meantime, try rephrasing or exploring other topics in the group. 🔍"
                )
            )
            print(f"[HANDLER] No answer found or answer empty for '{text}', logging as unknown")
    
    except Exception as e:
        print(f"[REPLY] ❌ Could not send answer: {e}")
        try:
            await update.message.reply_text("❌ Error sending response. Please try again.")
        except:
            pass
    
    except Exception as e:
        print(f"[HANDLER] ❌ Error: {e}")

# ======================================
# FLASK KEEPALIVE SERVER
# ======================================

app = Flask("DHV_OS_Server")

@app.route("/")
def home():
    """Root endpoint - ping to check if bot is alive"""
    kb_count = len(knowledge_base)
    kb_time = datetime.fromtimestamp(kb_last_loaded).strftime('%Y-%m-%d %H:%M:%S') if kb_last_loaded else "Never"
    
    html = f"""
    <html>
        <body style="font-family: Arial; padding: 20px;">
            <h1>🤖 DHV OS is alive 🚀</h1>
            <p><strong>Status:</strong> Running</p>
            <p><strong>KB Entries:</strong> {kb_count}</p>
            <p><strong>Last KB Load:</strong> {kb_time}</p>
        </body>
    </html>
    """
    return html

@app.route("/health")
def health():
    """Health check endpoint for monitoring"""
    if not knowledge_base:
        return {
            "status": "error",
            "message": "Knowledge base not loaded",
            "entries": 0
        }, 503
    
    return {
        "status": "ok",
        "entries": len(knowledge_base),
        "last_loaded": datetime.fromtimestamp(kb_last_loaded).isoformat() if kb_last_loaded else None,
        "timestamp": datetime.now().isoformat()
    }, 200

def run_flask_server():
    """Run Flask server"""
    try:
        # Port 5000 is required by Replit for web service
        port = 5000
        print(f"[SERVER] Starting on port {port}...")
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    except Exception as e:
        print(f"[SERVER] ❌ Error: {e}")

# ======================================
# MAIN
# ======================================

def main():
    """Main entry point"""
    # Validate environment
    if not BOT_TOKEN:
        print("[ERROR] ❌ TELEGRAM_TOKEN not set!")
        exit(1)
    
    if not DHV_GROUP_ID:
        print("[ERROR] ❌ DHV_GROUP_ID not set!")
        exit(1)
    
    print("[BOOT] 🚀 Starting DHV OS Bot...")
    print(f"[BOOT] Group ID: {DHV_GROUP_ID}")
    
    # Initialize CSV
    init_unknown_csv()
    
    # Initialize DB
    init_db()
    
    # Load KB on startup
    print("[BOOT] Starting background services...")
    load_knowledge_base()
    
    # Start KB auto-refresh
    kb_thread = Thread(target=kb_auto_refresher, daemon=True)
    kb_thread.start()
    print("[BOOT] ✅ KB Refresher started")
    
    flask_thread = Thread(target=run_flask_server, daemon=True)
    flask_thread.start()
    print("[BOOT] ✅ Flask Server started")
    
    # Setup Telegram bot
    print("[BOOT] Setting up Telegram bot...")
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    
    print("[BOOT] ✅ DHV OS is now LIVE on Telegram 🎉")
    print("[BOOT] Press Ctrl+C to stop")
    
    # Start polling
    application.run_polling()

if __name__ == "__main__":
    main()

import os
import logging
import random
import sqlite3
import requests
import json
import io
import csv
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    JobQueue,
)

# ------------------ الإعدادات الأساسية ------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8653217576:AAEzoImMB5C9dbUtAbHrmm3cumxMd653udk")
OWNER_ID = int(os.environ.get("OWNER_ID", "5324135896"))
DB_PATH = "tournament.db"
BACKUP_PATH = "backups/"
LANGUAGES = {'ar': 'العربية', 'en': 'English'}
DEFAULT_LANG = 'ar'

# إعداد تسجيل متقدم
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ------------------ دوال قاعدة البيانات ------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    active BOOLEAN DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    lang TEXT DEFAULT 'ar')''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_team (
                    user_id INTEGER,
                    team_id INTEGER,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, team_id),
                    FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                    FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE CASCADE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phase TEXT NOT NULL,
                    round TEXT,
                    group_name TEXT,
                    team1_id INTEGER NOT NULL,
                    team2_id INTEGER NOT NULL,
                    score1 INTEGER DEFAULT 0,
                    score2 INTEGER DEFAULT 0,
                    played BOOLEAN DEFAULT 0,
                    winner_id INTEGER,
                    status TEXT DEFAULT 'pending',
                    scheduled_time TIMESTAMP,
                    FOREIGN KEY(team1_id) REFERENCES teams(id),
                    FOREIGN KEY(team2_id) REFERENCES teams(id),
                    FOREIGN KEY(winner_id) REFERENCES teams(id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS team_stats (
                    team_id INTEGER PRIMARY KEY,
                    group_name TEXT,
                    played INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    draws INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    points INTEGER DEFAULT 0,
                    correct_answers INTEGER DEFAULT 0,
                    FOREIGN KEY(team_id) REFERENCES teams(id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS match_questions (
                    match_id INTEGER,
                    question_index INTEGER,
                    question_text TEXT,
                    correct_answer TEXT,
                    options TEXT,
                    difficulty TEXT,
                    answered BOOLEAN DEFAULT 0,
                    answered_by INTEGER,
                    PRIMARY KEY (match_id, question_index),
                    FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS player_answers (
                    match_id INTEGER,
                    user_id INTEGER,
                    question_index INTEGER,
                    answer TEXT,
                    is_correct BOOLEAN,
                    answered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (match_id, user_id, question_index),
                    FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS tournament (
                    key TEXT PRIMARY KEY,
                    value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS chat_groups (
                    id INTEGER PRIMARY KEY,
                    chat_id INTEGER UNIQUE)''')
    conn.commit()
    conn.close()

def db_execute(query: str, params: tuple = ()):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(query, params)
    conn.commit()
    result = c.fetchall()
    conn.close()
    return result

def db_insert(query: str, params: tuple) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(query, params)
    conn.commit()
    last_id = c.lastrowid
    conn.close()
    return last_id

# ------------------ دوال المساعدة العامة ------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

def get_team_id(name: str) -> Optional[int]:
    res = db_execute("SELECT id FROM teams WHERE name = ?", (name,))
    return res[0][0] if res else None

def get_team_name(team_id: int) -> Optional[str]:
    res = db_execute("SELECT name FROM teams WHERE id = ?", (team_id,))
    return res[0][0] if res else None

def list_teams() -> List[str]:
    res = db_execute("SELECT name FROM teams WHERE active=1 ORDER BY name")
    return [row[0] for row in res]

def get_team_players(team_id: int) -> List[int]:
    res = db_execute("SELECT user_id FROM user_team WHERE team_id = ?", (team_id,))
    return [row[0] for row in res]

def get_user_team(user_id: int) -> Optional[int]:
    res = db_execute("SELECT team_id FROM user_team WHERE user_id = ?", (user_id,))
    return res[0][0] if res else None

def get_user_lang(user_id: int) -> str:
    res = db_execute("SELECT lang FROM users WHERE user_id = ?", (user_id,))
    return res[0][0] if res else DEFAULT_LANG

def set_user_lang(user_id: int, lang: str):
    db_execute("UPDATE users SET lang = ? WHERE user_id = ?", (lang, user_id))

# ------------------ دوال الترجمة ------------------
translations = {
    'ar': {
        'welcome': "مرحباً بك في بوت البطولة!",
        'choose_team': "اختر فريقك:",
        'joined': "✅ انضممت إلى {team}!",
        'already_in_team': "أنت بالفعل في فريق {team}. استخدم /leave لمغادرته.",
        'left': "✅ غادرت فريق {team}.",
        'not_in_team': "لست في أي فريق.",
        'no_teams': "⚠️ لا توجد فرق متاحة حالياً.",
        'match_start': "⚽ مباراة جديدة!\n{team1} 🆚 {team2}\nسيتم إرسال {num} سؤالاً.",
        'question': "سؤال {current}/{total} (الصعوبة: {difficulty}):\n{question}",
        'correct': "✅ إجابة صحيحة!",
        'wrong': "❌ إجابة خاطئة. الإجابة الصحيحة: {correct}",
        'match_end': "انتهت المباراة! شكراً لمشاركتك.",
        'profile': "📊 ملفك الشخصي:\nالاسم: {name}\nالفريق: {team}\nالمباريات: {matches}\nإجابات صحيحة: {correct}\nإجابات خاطئة: {wrong}\nنسبة النجاح: {percent}%",
        'reminder': "⏰ تذكير: مباراة {team1} vs {team2} ستبدأ بعد نصف ساعة!",
        'mvp': "🏆 أفضل لاعب في المباراة: {name} ({team}) - {correct} إجابات صحيحة!",
    },
    'en': {
        'welcome': "Welcome to the tournament bot!",
        'choose_team': "Choose your team:",
        'joined': "✅ Joined {team}!",
        'already_in_team': "You are already in team {team}. Use /leave to leave.",
        'left': "✅ Left team {team}.",
        'not_in_team': "You are not in any team.",
        'no_teams': "⚠️ No teams available.",
        'match_start': "⚽ New match!\n{team1} 🆚 {team2}\nYou will receive {num} questions.",
        'question': "Question {current}/{total} (difficulty: {difficulty}):\n{question}",
        'correct': "✅ Correct answer!",
        'wrong': "❌ Wrong answer. Correct answer: {correct}",
        'match_end': "Match ended! Thanks for participating.",
        'profile': "📊 Your profile:\nName: {name}\nTeam: {team}\nMatches: {matches}\nCorrect answers: {correct}\nWrong answers: {wrong}\nSuccess rate: {percent}%",
        'reminder': "⏰ Reminder: Match {team1} vs {team2} starts in half an hour!",
        'mvp': "🏆 Man of the match: {name} ({team}) - {correct} correct answers!",
    }
}

def _(user_id: int, key: str, **kwargs) -> str:
    lang = get_user_lang(user_id)
    text = translations[lang].get(key, key)
    return text.format(**kwargs)

# ------------------ دوال الأسئلة ------------------
def fetch_questions(amount: int = 25, difficulty_boost: float = 1.0) -> List[Dict]:
    """
    جلب أسئلة بمستويات صعوبة مختلفة. difficulty_boost يزيد من نسبة الأسئلة الصعبة.
    """
    # نحدد عدد الأسئلة حسب المستوى
    if difficulty_boost > 1.5:
        easy, medium, hard = 5, 8, 12
    elif difficulty_boost > 1.0:
        easy, medium, hard = 7, 8, 10
    else:
        easy, medium, hard = 9, 8, 8
    total = easy + medium + hard
    if total > amount:
        # نعدل النسب
        factor = amount / total
        easy = int(easy * factor)
        medium = int(medium * factor)
        hard = amount - easy - medium
    difficulties = [('easy', easy), ('medium', medium), ('hard', hard)]
    questions = []
    for diff, cnt in difficulties:
        if cnt <= 0:
            continue
        url = f"https://opentdb.com/api.php?amount={cnt}&difficulty={diff}&type=multiple"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if data['response_code'] == 0:
                for item in data['results']:
                    options = item['incorrect_answers'] + [item['correct_answer']]
                    random.shuffle(options)
                    questions.append({
                        'question': item['question'],
                        'correct': item['correct_answer'],
                        'options': options,
                        'difficulty': diff
                    })
            else:
                logger.error(f"فشل جلب الأسئلة الصعوبة {diff}: {data['response_code']}")
        except Exception as e:
            logger.exception(f"خطأ في جلب الأسئلة {diff}: {e}")
    # إذا لم نتمكن من جلب الكمية، نكمل بأسئلة افتراضية
    while len(questions) < amount:
        questions.append({
            'question': "ما عاصمة فرنسا؟",
            'correct': "باريس",
            'options': ["باريس", "لندن", "برلين", "مدريد"],
            'difficulty': 'easy'
        })
    random.shuffle(questions)
    return questions[:amount]

# ------------------ دوال المباريات ------------------
async def start_match_by_id(context: ContextTypes.DEFAULT_TYPE, match_id: int):
    """بدء المباراة برقمها (دالة مساعدة)."""
    match = db_execute('''
        SELECT m.id, m.team1_id, m.team2_id, t1.name, t2.name
        FROM matches m
        JOIN teams t1 ON m.team1_id = t1.id
        JOIN teams t2 ON m.team2_id = t2.id
        WHERE m.id = ? AND m.played = 0 AND m.status = 'pending'
    ''', (match_id,))
    if not match:
        return
    match_id, team1_id, team2_id, team1_name, team2_name = match[0]
    # تحديث الحالة
    db_execute("UPDATE matches SET status = 'active' WHERE id = ?", (match_id,))
    team1_players = get_team_players(team1_id)
    team2_players = get_team_players(team2_id)
    if not team1_players or not team2_players:
        logger.warning(f"المباراة {match_id}: أحد الفريقين بلا لاعبين، لن تبدأ.")
        db_execute("UPDATE matches SET status = 'pending' WHERE id = ?", (match_id,))
        return
    # حساب boost الصعوبة بناءً على أداء الفرق السابق (إن وجد)
    team1_stats = db_execute("SELECT correct_answers, played FROM team_stats WHERE team_id = ?", (team1_id,))
    team2_stats = db_execute("SELECT correct_answers, played FROM team_stats WHERE team_id = ?", (team2_id,))
    avg_correct = 0
    if team1_stats and team1_stats[0][1] > 0:
        avg_correct += team1_stats[0][0] / team1_stats[0][1]
    if team2_stats and team2_stats[0][1] > 0:
        avg_correct += team2_stats[0][0] / team2_stats[0][1]
    avg_correct /= 2
    difficulty_boost = 1.0 + (avg_correct / 25)  # كلما زادت الإجابات الصحيحة، زادت الصعوبة
    questions = fetch_questions(25, difficulty_boost)
    if not questions:
        logger.error(f"فشل جلب أسئلة للمباراة {match_id}")
        db_execute("UPDATE matches SET status = 'pending' WHERE id = ?", (match_id,))
        return
    for idx, q in enumerate(questions):
        options_str = ','.join(q['options'])
        db_insert('''
            INSERT INTO match_questions (match_id, question_index, question_text, correct_answer, options, difficulty, answered)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        ''', (match_id, idx, q['question'], q['correct'], options_str, q['difficulty']))
    # إرسال إشعار للاعبين
    all_players = team1_players + team2_players
    for uid in all_players:
        try:
            await context.bot.send_message(
                uid,
                _(uid, 'match_start', team1=team1_name, team2=team2_name, num=25)
            )
        except Exception as e:
            logger.warning(f"لم نتمكن من إرسال رسالة للمستخدم {uid}: {e}")
    # تخزين بيانات المباراة في الذاكرة
    if 'active_matches' not in context.bot_data:
        context.bot_data['active_matches'] = {}
    context.bot_data['active_matches'][match_id] = {
        'questions': questions,
        'team1_id': team1_id,
        'team2_id': team2_id,
        'team1_name': team1_name,
        'team2_name': team2_name,
        'players': all_players,
        'current_question': 0,
        'answered_questions': set(),
    }
    # إرسال أول سؤال لكل لاعب
    for uid in all_players:
        await send_question_to_player(context, match_id, uid, 0)
    # إعلام المالك
    await context.bot.send_message(OWNER_ID, f"✅ بدأت المباراة المجدولة {match_id}: {team1_name} vs {team2_name}")

async def send_question_to_player(context: ContextTypes.DEFAULT_TYPE, match_id: int, user_id: int, q_index: int):
    match_data = context.bot_data['active_matches'].get(match_id)
    if not match_data:
        return
    questions = match_data['questions']
    if q_index >= len(questions):
        return
    q = questions[q_index]
    keyboard = [[InlineKeyboardButton(opt, callback_data=f"ans_{match_id}_{q_index}_{opt}")] for opt in q['options']]
    try:
        await context.bot.send_message(
            user_id,
            _(user_id, 'question', current=q_index+1, total=len(questions), difficulty=q['difficulty'], question=q['question']),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.warning(f"فشل إرسال السؤال للمستخدم {user_id}: {e}")

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data.split('_')
    if len(data) < 4:
        await query.edit_message_text("حدث خطأ في الإجابة.")
        return
    _, match_id_str, q_index_str, answer = data[0], data[1], data[2], '_'.join(data[3:])
    match_id = int(match_id_str)
    q_index = int(q_index_str)
    match_data = context.bot_data['active_matches'].get(match_id)
    if not match_data:
        await query.edit_message_text("المباراة غير نشطة أو انتهت.")
        return
    # التحقق مما إذا كان هذا السؤال قد أجيب عليه مسبقاً
    answered = db_execute("SELECT answered FROM match_questions WHERE match_id=? AND question_index=?", (match_id, q_index))
    if answered and answered[0][0] == 1:
        await query.edit_message_text("تمت الإجابة على هذا السؤال مسبقاً.")
        return
    # التحقق من صحة الإجابة
    correct_answer = match_data['questions'][q_index]['correct']
    is_correct = (answer == correct_answer)
    # تسجيل الإجابة
    db_insert('''
        INSERT INTO player_answers (match_id, user_id, question_index, answer, is_correct)
        VALUES (?, ?, ?, ?, ?)
    ''', (match_id, user_id, q_index, answer, is_correct))
    # تحديث حالة السؤال إلى مُجاب
    db_execute("UPDATE match_questions SET answered=1, answered_by=? WHERE match_id=? AND question_index=?", (user_id, match_id, q_index))
    # إرسال نتيجة الإجابة للاعب
    if is_correct:
        await query.edit_message_text(_(user_id, 'correct'))
    else:
        await query.edit_message_text(_(user_id, 'wrong', correct=correct_answer))
    # نتحقق مما إذا كانت كل الأسئلة قد أجيب عليها
    total_questions = len(match_data['questions'])
    answered_count = db_execute("SELECT COUNT(*) FROM match_questions WHERE match_id=? AND answered=1", (match_id,))[0][0]
    if answered_count >= total_questions:
        await finalize_match(context, match_id)

async def finalize_match(context: ContextTypes.DEFAULT_TYPE, match_id: int):
    match_data = context.bot_data['active_matches'].pop(match_id, None)
    if not match_data:
        return
    team1_id = match_data['team1_id']
    team2_id = match_data['team2_id']
    team1_name = match_data['team1_name']
    team2_name = match_data['team2_name']
    # حساب إجابات كل فريق
    team1_correct = 0
    team2_correct = 0
    team1_players = get_team_players(team1_id)
    team2_players = get_team_players(team2_id)
    for uid in team1_players:
        res = db_execute("SELECT COUNT(*) FROM player_answers WHERE match_id=? AND user_id=? AND is_correct=1", (match_id, uid))
        if res:
            team1_correct += res[0][0]
    for uid in team2_players:
        res = db_execute("SELECT COUNT(*) FROM player_answers WHERE match_id=? AND user_id=? AND is_correct=1", (match_id, uid))
        if res:
            team2_correct += res[0][0]
    # تحديد أفضل لاعب
    best_player = db_execute('''
        SELECT u.user_id, u.first_name, COUNT(*) as correct
        FROM player_answers pa
        JOIN users u ON pa.user_id = u.user_id
        WHERE pa.match_id = ? AND pa.is_correct = 1
        GROUP BY pa.user_id
        ORDER BY correct DESC
        LIMIT 1
    ''', (match_id,))
    if best_player:
        mvp_id, mvp_name, mvp_correct = best_player[0]
        mvp_team = get_team_name(get_user_team(mvp_id))
    # تحديث إحصائيات الفرق حسب المرحلة
    phase = db_execute("SELECT value FROM tournament WHERE key='phase'")[0][0]
    if phase == 'group':
        if team1_correct > team2_correct:
            winner_id = team1_id
            score1, score2 = 1, 0
            db_execute("UPDATE team_stats SET played=played+1, wins=wins+1, points=points+3, correct_answers=correct_answers+? WHERE team_id=?", (team1_correct, team1_id))
            db_execute("UPDATE team_stats SET played=played+1, losses=losses+1, correct_answers=correct_answers+? WHERE team_id=?", (team2_correct, team2_id))
        elif team2_correct > team1_correct:
            winner_id = team2_id
            score1, score2 = 0, 1
            db_execute("UPDATE team_stats SET played=played+1, wins=wins+1, points=points+3, correct_answers=correct_answers+? WHERE team_id=?", (team2_correct, team2_id))
            db_execute("UPDATE team_stats SET played=played+1, losses=losses+1, correct_answers=correct_answers+? WHERE team_id=?", (team1_correct, team1_id))
        else:
            winner_id = None
            score1, score2 = 0, 0
            db_execute("UPDATE team_stats SET played=played+1, draws=draws+1, points=points+1, correct_answers=correct_answers+? WHERE team_id=?", (team1_correct, team1_id))
            db_execute("UPDATE team_stats SET played=played+1, draws=draws+1, points=points+1, correct_answers=correct_answers+? WHERE team_id=?", (team2_correct, team2_id))
    else:
        # مرحلة خروج المغلوب
        if team1_correct == team2_correct:
            # اختيار عشوائي (يمكن تحسينه)
            winner_id = random.choice([team1_id, team2_id])
        elif team1_correct > team2_correct:
            winner_id = team1_id
        else:
            winner_id = team2_id
        score1, score2 = (1,0) if winner_id == team1_id else (0,1)
        loser_id = team2_id if winner_id == team1_id else team1_id
        db_execute("UPDATE teams SET active=0 WHERE id=?", (loser_id,))
        db_execute("UPDATE team_stats SET correct_answers = correct_answers + ? WHERE team_id=?", (team1_correct, team1_id))
        db_execute("UPDATE team_stats SET correct_answers = correct_answers + ? WHERE team_id=?", (team2_correct, team2_id))
    # تحديث المباراة
    db_execute('''
        UPDATE matches SET played=1, status='finished', score1=?, score2=?, winner_id=?
        WHERE id=?
    ''', (score1, score2, winner_id, match_id))
    # إرسال النتائج للمالك
    result_text = f"✅ انتهت المباراة {match_id}:\n{team1_name} {team1_correct} - {team2_correct} {team2_name}"
    if phase == 'group':
        result_text += f"\nالنتيجة: {score1}:{score2}"
    else:
        result_text += f"\nالفائز: {get_team_name(winner_id)}"
    await context.bot.send_message(OWNER_ID, result_text)
    # إرسال أفضل لاعب
    if best_player:
        await context.bot.send_message(OWNER_ID, _(OWNER_ID, 'mvp', name=mvp_name, team=mvp_team, correct=mvp_correct))
    # إعلام اللاعبين
    for uid in match_data['players']:
        try:
            await context.bot.send_message(uid, _(uid, 'match_end'))
        except:
            pass
    # التحقق من تقدم البطولة
    await check_and_advance_knockout(context)

async def check_and_advance_knockout(context: ContextTypes.DEFAULT_TYPE):
    phase = db_execute("SELECT value FROM tournament WHERE key='phase'")[0][0]
    if phase != 'group':
        return
    pending_groups = db_execute("SELECT COUNT(*) FROM matches WHERE phase='group' AND played=0")
    if pending_groups[0][0] > 0:
        return
    # انتهت المجموعات
    groups = ['A', 'B']
    qualified = []
    for group in groups:
        stats = db_execute('''
            SELECT ts.team_id, ts.points, ts.correct_answers
            FROM team_stats ts
            JOIN teams t ON ts.team_id = t.id
            WHERE ts.group_name = ? AND t.active = 1
            ORDER BY ts.points DESC, ts.correct_answers DESC
            LIMIT 2
        ''', (group,))
        if len(stats) < 2:
            await context.bot.send_message(OWNER_ID, f"⚠️ المجموعة {group} لا يوجد بها فريقان متبقيان!")
            return
        qualified.extend([row[0] for row in stats])
    # إنشاء مباريات نصف النهائي
    db_insert("INSERT INTO matches (phase, round, team1_id, team2_id) VALUES ('knockout', 'semi', ?, ?)",
              (qualified[0], qualified[3]))
    db_insert("INSERT INTO matches (phase, round, team1_id, team2_id) VALUES ('knockout', 'semi', ?, ?)",
              (qualified[2], qualified[1]))
    db_execute("UPDATE tournament SET value='knockout' WHERE key='phase'")
    await context.bot.send_message(OWNER_ID, "🏆 انتهت مرحلة المجموعات! تم إنشاء مباريات نصف النهائي.")

# ------------------ أوامر المالك ------------------
async def owner_add_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("❗ استخدم: /addteam <اسم الفريق>")
        return
    name = " ".join(context.args).strip()
    count = db_execute("SELECT COUNT(*) FROM teams WHERE active=1")[0][0]
    if count >= 8:
        await update.message.reply_text("❌ لا يمكن إضافة المزيد من الفرق، الحد الأقصى 8.")
        return
    try:
        db_insert("INSERT INTO teams (name, active) VALUES (?, 1)", (name,))
        await update.message.reply_text(f"✅ تم إضافة الفريق {name}.")
    except sqlite3.IntegrityError:
        await update.message.reply_text(f"❌ الفريق موجود بالفعل.")

async def owner_del_team(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("❗ استخدم: /delteam <اسم الفريق>")
        return
    name = " ".join(context.args).strip()
    team_id = get_team_id(name)
    if not team_id:
        await update.message.reply_text("❌ الفريق غير موجود.")
        return
    db_execute("DELETE FROM teams WHERE id = ?", (team_id,))
    await update.message.reply_text(f"✅ تم حذف الفريق {name}.")

async def owner_start_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    teams = db_execute("SELECT id, name FROM teams WHERE active=1")
    team_ids = [row[0] for row in teams]
    if len(team_ids) < 2:
        await update.message.reply_text("❌ يجب وجود فريقين على الأقل.")
        return
    db_execute("DELETE FROM matches")
    db_execute("DELETE FROM team_stats")
    db_execute("DELETE FROM tournament")
    db_execute("INSERT INTO tournament (key, value) VALUES ('phase', 'group')")
    random.shuffle(team_ids)
    mid = (len(team_ids) + 1) // 2
    group_a_ids = team_ids[:mid]
    group_b_ids = team_ids[mid:]
    for tid in group_a_ids:
        db_insert("INSERT INTO team_stats (team_id, group_name) VALUES (?, 'A')", (tid,))
    for tid in group_b_ids:
        db_insert("INSERT INTO team_stats (team_id, group_name) VALUES (?, 'B')", (tid,))
    for i in range(len(group_a_ids)):
        for j in range(i+1, len(group_a_ids)):
            db_insert("INSERT INTO matches (phase, round, group_name, team1_id, team2_id) VALUES (?, ?, ?, ?, ?)",
                      ("group", "group", "A", group_a_ids[i], group_a_ids[j]))
    for i in range(len(group_b_ids)):
        for j in range(i+1, len(group_b_ids)):
            db_insert("INSERT INTO matches (phase, round, group_name, team1_id, team2_id) VALUES (?, ?, ?, ?, ?)",
                      ("group", "group", "B", group_b_ids[i], group_b_ids[j]))
    names_a = [get_team_name(tid) for tid in group_a_ids]
    names_b = [get_team_name(tid) for tid in group_b_ids]
    text = "✅ بدأت البطولة!\nالمجموعة A:\n" + "\n".join(f"• {n}" for n in names_a)
    text += "\n\nالمجموعة B:\n" + "\n".join(f"• {n}" for n in names_b)
    await update.message.reply_text(text)

async def owner_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if len(context.args) < 3:
        await update.message.reply_text("❗ استخدم: /schedule <رقم المباراة> <اليوم> <الساعة:الدقيقة>\nالأيام: الجمعة, السبت")
        return
    try:
        match_id = int(context.args[0])
        day = context.args[1]
        time_str = context.args[2]
        hour, minute = map(int, time_str.split(':'))
        if day not in ['الجمعة', 'السبت']:
            await update.message.reply_text("❌ اليوم يجب أن يكون الجمعة أو السبت.")
            return
        now = datetime.now()
        days_until = {
            'الجمعة': (4 - now.weekday()) % 7,
            'السبت': (5 - now.weekday()) % 7
        }[day]
        scheduled_date = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_until if days_until > 0 else 7)
        db_execute("UPDATE matches SET scheduled_time = ? WHERE id = ?", (scheduled_date.isoformat(), match_id))
        await update.message.reply_text(f"✅ تم جدولة المباراة {match_id} في {day} {time_str}.")
        # جدولة تذكير قبل نصف ساعة
        reminder_time = scheduled_date - timedelta(minutes=30)
        context.job_queue.run_once(remind_match, reminder_time, data=match_id)
    except Exception as e:
        await update.message.reply_text(f"❌ خطأ في الإدخال: {e}")

async def owner_reschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if len(context.args) < 4:
        await update.message.reply_text("❗ استخدم: /reschedule <رقم المباراة> <اليوم> <الساعة:الدقيقة>")
        return
    try:
        match_id = int(context.args[0])
        day = context.args[1]
        time_str = context.args[2]
        hour, minute = map(int, time_str.split(':'))
        if day not in ['الجمعة', 'السبت']:
            await update.message.reply_text("❌ اليوم يجب أن يكون الجمعة أو السبت.")
            return
        now = datetime.now()
        days_until = {
            'الجمعة': (4 - now.weekday()) % 7,
            'السبت': (5 - now.weekday()) % 7
        }[day]
        scheduled_date = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_until if days_until > 0 else 7)
        db_execute("UPDATE matches SET scheduled_time = ? WHERE id = ?", (scheduled_date.isoformat(), match_id))
        await update.message.reply_text(f"✅ تم تعديل موعد المباراة {match_id} إلى {day} {time_str}.")
    except Exception as e:
        await update.message.reply_text(f"❌ خطأ في الإدخال: {e}")

async def owner_unschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("❗ استخدم: /unschedule <رقم المباراة>")
        return
    try:
        match_id = int(context.args[0])
        db_execute("UPDATE matches SET scheduled_time = NULL WHERE id = ?", (match_id,))
        await update.message.reply_text(f"✅ تم إلغاء جدولة المباراة {match_id}.")
    except:
        await update.message.reply_text("❌ رقم غير صالح.")

async def owner_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("❗ استخدم: /broadcast <الرسالة>")
        return
    message = " ".join(context.args)
    users = db_execute("SELECT user_id FROM users")
    sent = 0
    failed = 0
    for (uid,) in users:
        try:
            await context.bot.send_message(uid, message)
            sent += 1
        except:
            failed += 1
    await update.message.reply_text(f"✅ تم الإرسال: {sent} نجح، {failed} فشل.")

async def owner_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    try:
        from datetime import datetime
        backup_filename = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        if not os.path.exists(BACKUP_PATH):
            os.makedirs(BACKUP_PATH)
        shutil.copy2(DB_PATH, os.path.join(BACKUP_PATH, backup_filename))
        with open(os.path.join(BACKUP_PATH, backup_filename), 'rb') as f:
            await update.message.reply_document(f, filename=backup_filename)
        await update.message.reply_text("✅ تم إنشاء نسخة احتياطية وإرسالها.")
    except Exception as e:
        await update.message.reply_text(f"❌ فشل النسخ الاحتياطي: {e}")

async def owner_matches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    matches = db_execute('''
        SELECT m.id, m.phase, m.round, t1.name, t2.name, m.played, m.status, m.scheduled_time
        FROM matches m
        JOIN teams t1 ON m.team1_id = t1.id
        JOIN teams t2 ON m.team2_id = t2.id
        ORDER BY m.id
    ''')
    if not matches:
        await update.message.reply_text("لا توجد مباريات.")
        return
    lines = []
    for m in matches:
        status_emoji = "✅" if m[5] else "🔄" if m[6]=='active' else "⏳"
        scheduled = f" (مجدولة: {m[7]})" if m[7] else ""
        lines.append(f"{status_emoji} ID {m[0]} | {m[1]} - {m[2]}: {m[3]} vs {m[4]}{scheduled}")
    await update.message.reply_text("📅 المباريات:\n" + "\n".join(lines))

async def owner_standings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    phase = db_execute("SELECT value FROM tournament WHERE key='phase'")[0][0]
    if phase == 'group':
        groups = ['A', 'B']
        text = "📊 ترتيب المجموعات:\n"
        for group in groups:
            stats = db_execute('''
                SELECT t.name, ts.played, ts.wins, ts.draws, ts.losses, ts.points, ts.correct_answers
                FROM team_stats ts
                JOIN teams t ON ts.team_id = t.id
                WHERE ts.group_name = ? AND t.active = 1
                ORDER BY ts.points DESC, ts.correct_answers DESC
            ''', (group,))
            if stats:
                text += f"\nالمجموعة {group}:\n"
                for row in stats:
                    text += f"{row[0]}: {row[5]} نقاط (لعب {row[1]}، فوز {row[2]}، تعادل {row[3]}، خسارة {row[4]}، إجابات صحيحة {row[6]})\n"
    else:
        text = "🏆 مرحلة خروج المغلوب:\n"
        matches = db_execute('''
            SELECT m.id, m.round, t1.name, t2.name, m.played, m.winner_id
            FROM matches m
            JOIN teams t1 ON m.team1_id = t1.id
            JOIN teams t2 ON m.team2_id = t2.id
            WHERE m.phase='knockout'
            ORDER BY m.id
        ''')
        for m in matches:
            status = "✅" if m[4] else "⏳"
            if m[4]:
                winner = get_team_name(m[5])
                text += f"{status} {m[1]}: {m[2]} vs {m[3]} -> الفائز {winner}\n"
            else:
                text += f"{status} {m[1]}: {m[2]} vs {m[3]}\n"
    await update.message.reply_text(text)

async def owner_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return
    text = (
        "⚽ أوامر المالك:\n"
        "/addteam <اسم> - إضافة فريق\n"
        "/delteam <اسم> - حذف فريق\n"
        "/start_tournament - بدء البطولة (تقسيم المجموعات)\n"
        "/schedule <match_id> <اليوم> <الساعة:الدقيقة> - جدولة مباراة\n"
        "/reschedule <match_id> <اليوم> <الساعة:الدقيقة> - تعديل موعد\n"
        "/unschedule <match_id> - إلغاء جدولة\n"
        "/broadcast <رسالة> - إرسال للجميع\n"
        "/backup - نسخة احتياطية\n"
        "/matches - عرض المباريات\n"
        "/standings - عرض الترتيب\n"
        "/help - هذه المساعدة"
    )
    await update.message.reply_text(text)

# ------------------ أوامر اللاعبين ------------------
async def player_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_execute("INSERT OR IGNORE INTO users (user_id, username, first_name, lang) VALUES (?, ?, ?, ?)",
               (user.id, user.username, user.first_name, DEFAULT_LANG))
    teams = list_teams()
    if not teams:
        await update.message.reply_text(_(user.id, 'no_teams'))
        return
    current = get_user_team(user.id)
    if current:
        team = get_team_name(current)
        await update.message.reply_text(_(user.id, 'already_in_team', team=team))
        return
    keyboard = [[InlineKeyboardButton(team, callback_data=f"join_{team}")] for team in teams]
    await update.message.reply_text(_(user.id, 'choose_team'), reply_markup=InlineKeyboardMarkup(keyboard))

async def player_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data.split('_', 1)
    if len(data) != 2 or data[0] != 'join':
        await query.edit_message_text("خطأ.")
        return
    team_name = data[1]
    team_id = get_team_id(team_name)
    if not team_id:
        await query.edit_message_text("الفريق غير موجود.")
        return
    if get_user_team(user_id):
        await query.edit_message_text(_(user_id, 'already_in_team', team=team_name))
        return
    try:
        db_insert("INSERT INTO user_team (user_id, team_id) VALUES (?, ?)", (user_id, team_id))
        await query.edit_message_text(_(user_id, 'joined', team=team_name))
        user = update.effective_user
        mention = f"@{user.username}" if user.username else f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
        await context.bot.send_message(
            OWNER_ID,
            f"📢 لاعب جديد انضم!\n"
            f"اللاعب: {mention}\n"
            f"الاسم: {user.first_name}\n"
            f"المعرف: {user.id}\n"
            f"الفريق: {team_name}",
            parse_mode="HTML"
        )
    except sqlite3.IntegrityError:
        await query.edit_message_text("حدث خطأ في الانضمام.")

async def player_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    team_id = get_user_team(user_id)
    if not team_id:
        await update.message.reply_text(_(user_id, 'not_in_team'))
        return
    team_name = get_team_name(team_id)
    db_execute("DELETE FROM user_team WHERE user_id = ?", (user_id,))
    await update.message.reply_text(_(user_id, 'left', team=team_name))

async def player_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    team_id = get_user_team(user_id)
    team_name = get_team_name(team_id) if team_id else "—"
    matches = db_execute("SELECT COUNT(DISTINCT match_id) FROM player_answers WHERE user_id=?", (user_id,))[0][0]
    correct = db_execute("SELECT COUNT(*) FROM player_answers WHERE user_id=? AND is_correct=1", (user_id,))[0][0]
    wrong = db_execute("SELECT COUNT(*) FROM player_answers WHERE user_id=? AND is_correct=0", (user_id,))[0][0]
    total = correct + wrong
    percent = (correct / total * 100) if total > 0 else 0
    user_info = db_execute("SELECT first_name FROM users WHERE user_id=?", (user_id,))[0][0]
    await update.message.reply_text(
        _(user_id, 'profile', name=user_info, team=team_name, matches=matches,
          correct=correct, wrong=wrong, percent=round(percent, 2))
    )

async def player_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if context.args and context.args[0] in LANGUAGES:
        set_user_lang(user_id, context.args[0])
        await update.message.reply_text(f"✅ تم تغيير اللغة إلى {LANGUAGES[context.args[0]]}")
    else:
        await update.message.reply_text(f"❗ استخدم: /lang ar أو /lang en")

async def player_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        _(user_id, 'help', fallback="⚽ أوامر اللاعبين:\n"
        "/start - عرض الفرق والانضمام\n"
        "/leave - مغادرة الفريق\n"
        "/profile - عرض ملفك الشخصي\n"
        "/lang - تغيير اللغة (ar/en)\n"
        "/help - هذه المساعدة")
    )

# ------------------ المهام المجدولة ------------------
async def remind_match(context: ContextTypes.DEFAULT_TYPE):
    match_id = context.job.data
    match = db_execute('''
        SELECT t1.name, t2.name FROM matches m
        JOIN teams t1 ON m.team1_id = t1.id
        JOIN teams t2 ON m.team2_id = t2.id
        WHERE m.id = ?
    ''', (match_id,))
    if not match:
        return
    team1, team2 = match[0]
    players = db_execute('''
        SELECT DISTINCT user_id FROM user_team ut
        JOIN matches m ON ut.team_id IN (m.team1_id, m.team2_id)
        WHERE m.id = ?
    ''', (match_id,))
    for (uid,) in players:
        try:
            await context.bot.send_message(uid, _(uid, 'reminder', team1=team1, team2=team2))
        except:
            pass

async def check_scheduled_matches(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now().isoformat()
    matches = db_execute('''
        SELECT id FROM matches
        WHERE status = 'pending' AND played = 0 AND scheduled_time IS NOT NULL AND scheduled_time <= ?
    ''', (now,))
    for (match_id,) in matches:
        await start_match_by_id(context, match_id)

# ------------------ التشغيل الرئيسي ------------------
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # أوامر المالك
    app.add_handler(CommandHandler("addteam", owner_add_team))
    app.add_handler(CommandHandler("delteam", owner_del_team))
    app.add_handler(CommandHandler("start_tournament", owner_start_tournament))
    app.add_handler(CommandHandler("schedule", owner_schedule))
    app.add_handler(CommandHandler("reschedule", owner_reschedule))
    app.add_handler(CommandHandler("unschedule", owner_unschedule))
    app.add_handler(CommandHandler("broadcast", owner_broadcast))
    app.add_handler(CommandHandler("backup", owner_backup))
    app.add_handler(CommandHandler("matches", owner_matches))
    app.add_handler(CommandHandler("standings", owner_standings))
    app.add_handler(CommandHandler("help", owner_help))

    # أوامر اللاعبين
    app.add_handler(CommandHandler("start", player_start))
    app.add_handler(CommandHandler("leave", player_leave))
    app.add_handler(CommandHandler("profile", player_profile))
    app.add_handler(CommandHandler("lang", player_lang))
    app.add_handler(CommandHandler("help", player_help))

    # معالجات الأزرار
    app.add_handler(CallbackQueryHandler(player_join_callback, pattern="^join_"))
    app.add_handler(CallbackQueryHandler(handle_answer, pattern="^ans_"))

    # المهام المجدولة
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_repeating(check_scheduled_matches, interval=60, first=10)

    # تشغيل البوت
    if os.environ.get('PYTHONANYWHERE_DOMAIN'):
        # على PythonAnywhere نستخدم webhook
        app.run_webhook(
            listen="0.0.0.0",
            port=8080,
            url_path=BOT_TOKEN,
            webhook_url=f"https://{os.environ['PYTHONANYWHERE_DOMAIN']}/{BOT_TOKEN}"
        )
    else:
        # محلياً أو على منصة أخرى نستخدم polling
        logger.info("البوت يعمل بوضع polling...")
        app.run_polling()

if __name__ == "__main__":
    main()

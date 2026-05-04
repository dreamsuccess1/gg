"""
database.py — Full Featured SQLite Database
Added: Sections (Subject + Topic wise), Sectional Leaderboard
"""

import sqlite3
import json
import logging
import threading
from typing import Optional

DB_PATH = "quiz_bot.db"
_local  = threading.local()
logger  = logging.getLogger(__name__)


def _conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA synchronous=NORMAL")
    return _local.conn


def init_db():
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id         INTEGER PRIMARY KEY,
            name       TEXT,
            username   TEXT,
            joined     TEXT DEFAULT (datetime('now')),
            is_premium INTEGER DEFAULT 0,
            is_banned  INTEGER DEFAULT 0
        );

        -- SECTIONAL: Subject table (e.g. Maths, Science, History)
        CREATE TABLE IF NOT EXISTS subjects (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT NOT NULL UNIQUE,
            emoji   TEXT DEFAULT '📚',
            created TEXT DEFAULT (datetime('now'))
        );

        -- SECTIONAL: Topic table (e.g. Algebra under Maths)
        CREATE TABLE IF NOT EXISTS topics (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id INTEGER REFERENCES subjects(id) ON DELETE CASCADE,
            name       TEXT NOT NULL,
            created    TEXT DEFAULT (datetime('now')),
            UNIQUE(subject_id, name)
        );

        CREATE TABLE IF NOT EXISTS question_sets (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL,
            owner_id   INTEGER,
            is_private INTEGER DEFAULT 0,
            subject_id INTEGER REFERENCES subjects(id) ON DELETE SET NULL,
            topic_id   INTEGER REFERENCES topics(id)   ON DELETE SET NULL,
            created    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS questions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            set_id      INTEGER REFERENCES question_sets(id) ON DELETE CASCADE,
            question    TEXT NOT NULL,
            options     TEXT NOT NULL,
            correct     INTEGER NOT NULL,
            explanation TEXT DEFAULT '',
            timer       INTEGER DEFAULT 20,
            photo_id    TEXT
        );

        CREATE TABLE IF NOT EXISTS answers (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER,
            user_name  TEXT,
            poll_id    TEXT,
            chosen     INTEGER,
            correct    INTEGER,
            time_taken REAL,
            ts         TEXT DEFAULT (datetime('now'))
        );

        -- SECTIONAL: section_tag = "subject_<id>" or "topic_<id>" or "overall"
        CREATE TABLE IF NOT EXISTS leaderboard (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id      INTEGER,
            user_id      INTEGER,
            name         TEXT,
            score        INTEGER DEFAULT 0,
            correct      INTEGER DEFAULT 0,
            wrong        INTEGER DEFAULT 0,
            quizzes      INTEGER DEFAULT 1,
            section_tag  TEXT DEFAULT 'overall',
            ts           TEXT DEFAULT (datetime('now')),
            UNIQUE(chat_id, user_id, section_tag)
        );

        CREATE TABLE IF NOT EXISTS scheduled_quizzes (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    INTEGER,
            set_id     INTEGER,
            run_at     TEXT,
            created_by INTEGER,
            done       INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS broadcasts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            message    TEXT,
            sent_by    INTEGER,
            sent_at    TEXT DEFAULT (datetime('now')),
            sent_count INTEGER DEFAULT 0
        );
    """)
    c.commit()


# ── Users ────────────────────────────────────────────────────────────────────

def register_user(user_id: int, name: str, username: Optional[str]):
    c = _conn()
    c.execute("INSERT OR IGNORE INTO users(id,name,username) VALUES(?,?,?)",
              (user_id, name, username))
    c.execute("UPDATE users SET name=?, username=? WHERE id=?",
              (name, username, user_id))
    c.commit()

def get_all_users() -> list:
    rows = _conn().execute("SELECT id,name FROM users WHERE is_banned=0").fetchall()
    return [dict(r) for r in rows]

def get_user_count() -> int:
    return _conn().execute("SELECT COUNT(*) FROM users").fetchone()[0]

def ban_user(user_id: int):
    c = _conn(); c.execute("UPDATE users SET is_banned=1 WHERE id=?", (user_id,)); c.commit()

def unban_user(user_id: int):
    c = _conn(); c.execute("UPDATE users SET is_banned=0 WHERE id=?", (user_id,)); c.commit()

def is_banned(user_id: int) -> bool:
    row = _conn().execute("SELECT is_banned FROM users WHERE id=?", (user_id,)).fetchone()
    return bool(row and row["is_banned"])

def set_premium(user_id: int, value: int = 1):
    c = _conn(); c.execute("UPDATE users SET is_premium=? WHERE id=?", (value, user_id)); c.commit()

def is_premium(user_id: int) -> bool:
    row = _conn().execute("SELECT is_premium FROM users WHERE id=?", (user_id,)).fetchone()
    return bool(row and row["is_premium"])


# ── Subjects ─────────────────────────────────────────────────────────────────

def create_subject(name: str, emoji: str = "📚") -> int:
    c = _conn()
    c.execute("INSERT OR IGNORE INTO subjects(name,emoji) VALUES(?,?)", (name, emoji))
    c.commit()
    row = c.execute("SELECT id FROM subjects WHERE name=?", (name,)).fetchone()
    return row["id"]

def get_all_subjects() -> list:
    rows = _conn().execute("SELECT * FROM subjects ORDER BY name").fetchall()
    return [dict(r) for r in rows]

def get_subject(subject_id: int) -> Optional[dict]:
    row = _conn().execute("SELECT * FROM subjects WHERE id=?", (subject_id,)).fetchone()
    return dict(row) if row else None

def delete_subject(subject_id: int):
    c = _conn(); c.execute("DELETE FROM subjects WHERE id=?", (subject_id,)); c.commit()

def rename_subject(subject_id: int, new_name: str, new_emoji: str = None):
    c = _conn()
    if new_emoji:
        c.execute("UPDATE subjects SET name=?, emoji=? WHERE id=?", (new_name, new_emoji, subject_id))
    else:
        c.execute("UPDATE subjects SET name=? WHERE id=?", (new_name, subject_id))
    c.commit()


# ── Topics ───────────────────────────────────────────────────────────────────

def create_topic(subject_id: int, name: str) -> int:
    c = _conn()
    c.execute("INSERT OR IGNORE INTO topics(subject_id,name) VALUES(?,?)", (subject_id, name))
    c.commit()
    row = c.execute(
        "SELECT id FROM topics WHERE subject_id=? AND name=?", (subject_id, name)
    ).fetchone()
    return row["id"]

def get_topics(subject_id: int) -> list:
    rows = _conn().execute(
        "SELECT * FROM topics WHERE subject_id=? ORDER BY name", (subject_id,)
    ).fetchall()
    return [dict(r) for r in rows]

def get_topic(topic_id: int) -> Optional[dict]:
    row = _conn().execute("SELECT * FROM topics WHERE id=?", (topic_id,)).fetchone()
    return dict(row) if row else None

def delete_topic(topic_id: int):
    c = _conn(); c.execute("DELETE FROM topics WHERE id=?", (topic_id,)); c.commit()


# ── Sets ─────────────────────────────────────────────────────────────────────

def create_set(name: str, owner_id: int = 0, is_private: int = 0,
               subject_id: int = None, topic_id: int = None) -> int:
    c = _conn()
    cur = c.execute(
        "INSERT INTO question_sets(name,owner_id,is_private,subject_id,topic_id)"
        " VALUES(?,?,?,?,?)",
        (name, owner_id, is_private, subject_id, topic_id)
    )
    c.commit()
    return cur.lastrowid

def get_all_sets(subject_id: int = None, topic_id: int = None) -> list:
    c = _conn()
    if topic_id:
        rows = c.execute("""
            SELECT s.id, s.name, s.is_private, s.subject_id, s.topic_id,
                   COUNT(q.id) as count,
                   sub.name as subject_name, sub.emoji as subject_emoji,
                   t.name as topic_name
            FROM question_sets s
            LEFT JOIN questions q ON q.set_id=s.id
            LEFT JOIN subjects sub ON sub.id=s.subject_id
            LEFT JOIN topics t ON t.id=s.topic_id
            WHERE s.topic_id=?
            GROUP BY s.id ORDER BY s.id DESC
        """, (topic_id,)).fetchall()
    elif subject_id:
        rows = c.execute("""
            SELECT s.id, s.name, s.is_private, s.subject_id, s.topic_id,
                   COUNT(q.id) as count,
                   sub.name as subject_name, sub.emoji as subject_emoji,
                   t.name as topic_name
            FROM question_sets s
            LEFT JOIN questions q ON q.set_id=s.id
            LEFT JOIN subjects sub ON sub.id=s.subject_id
            LEFT JOIN topics t ON t.id=s.topic_id
            WHERE s.subject_id=?
            GROUP BY s.id ORDER BY s.id DESC
        """, (subject_id,)).fetchall()
    else:
        rows = c.execute("""
            SELECT s.id, s.name, s.is_private, s.subject_id, s.topic_id,
                   COUNT(q.id) as count,
                   sub.name as subject_name, sub.emoji as subject_emoji,
                   t.name as topic_name
            FROM question_sets s
            LEFT JOIN questions q ON q.set_id=s.id
            LEFT JOIN subjects sub ON sub.id=s.subject_id
            LEFT JOIN topics t ON t.id=s.topic_id
            GROUP BY s.id ORDER BY s.id DESC
        """).fetchall()
    return [dict(r) for r in rows]

def get_set(set_id: int) -> Optional[dict]:
    row = _conn().execute("""
        SELECT s.*, sub.name as subject_name, sub.emoji as subject_emoji,
               t.name as topic_name
        FROM question_sets s
        LEFT JOIN subjects sub ON sub.id=s.subject_id
        LEFT JOIN topics t ON t.id=s.topic_id
        WHERE s.id=?
    """, (set_id,)).fetchone()
    return dict(row) if row else None

def rename_set(set_id: int, new_name: str):
    c = _conn(); c.execute("UPDATE question_sets SET name=? WHERE id=?", (new_name, set_id)); c.commit()

def delete_set(set_id: int):
    c = _conn(); c.execute("DELETE FROM question_sets WHERE id=?", (set_id,)); c.commit()

def update_set_section(set_id: int, subject_id: int = None, topic_id: int = None):
    c = _conn()
    c.execute("UPDATE question_sets SET subject_id=?, topic_id=? WHERE id=?",
              (subject_id, topic_id, set_id))
    c.commit()

def shuffle_set(set_id: int):
    import random
    c   = _conn()
    rows = c.execute("SELECT id FROM questions WHERE set_id=?", (set_id,)).fetchall()
    ids  = [r["id"] for r in rows]
    random.shuffle(ids)
    for new_pos, qid in enumerate(ids):
        c.execute("UPDATE questions SET id=? WHERE id=?", (new_pos+90000, qid))
    c.commit()
    rows2 = c.execute(
        "SELECT id FROM questions WHERE set_id=? ORDER BY id", (set_id,)
    ).fetchall()
    for new_pos, r in enumerate(rows2):
        c.execute("UPDATE questions SET id=? WHERE id=?",
                  (new_pos+set_id*1000+1, r["id"]))
    c.commit()


# ── Questions ────────────────────────────────────────────────────────────────

def add_question(set_id: int, question: str, options: list,
                 correct: int, explanation: str = "",
                 timer: int = 20, photo_id: str = None):
    c = _conn()
    c.execute(
        "INSERT INTO questions(set_id,question,options,correct,explanation,timer,photo_id)"
        " VALUES(?,?,?,?,?,?,?)",
        (set_id, question, json.dumps(options, ensure_ascii=False),
         correct, explanation, timer, photo_id)
    )
    c.commit()

def get_questions(set_id: int) -> list:
    rows = _conn().execute(
        "SELECT * FROM questions WHERE set_id=? ORDER BY id", (set_id,)
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r); d["options"] = json.loads(d["options"]); result.append(d)
    return result

def get_question(q_id: int) -> Optional[dict]:
    row = _conn().execute("SELECT * FROM questions WHERE id=?", (q_id,)).fetchone()
    if not row: return None
    d = dict(row); d["options"] = json.loads(d["options"]); return d

def delete_question(q_id: int):
    c = _conn(); c.execute("DELETE FROM questions WHERE id=?", (q_id,)); c.commit()

def update_question_timer(set_id: int, timer: int):
    c = _conn()
    c.execute("UPDATE questions SET timer=? WHERE set_id=?", (timer, set_id))
    c.commit()


# ── Answers ──────────────────────────────────────────────────────────────────

def record_answer(user_id: int, user_name: str, poll_id: str,
                  chosen: int, correct: int, time_taken: float):
    c = _conn()
    c.execute(
        "INSERT INTO answers(user_id,user_name,poll_id,chosen,correct,time_taken)"
        " VALUES(?,?,?,?,?,?)",
        (user_id, user_name, poll_id, chosen, correct, time_taken)
    )
    c.commit()

def cleanup_old_answers(days: int = 30):
    c = _conn()
    c.execute("DELETE FROM answers WHERE ts < datetime('now', ? || ' days')", (f"-{days}",))
    c.commit()


# ── Leaderboard ──────────────────────────────────────────────────────────────

def _section_tags(set_info: dict) -> list:
    """Quiz ke liye kaun kaun se section_tags update karein."""
    tags = ["overall"]
    if set_info and set_info.get("subject_id"):
        tags.append(f"subject_{set_info['subject_id']}")
    if set_info and set_info.get("topic_id"):
        tags.append(f"topic_{set_info['topic_id']}")
    return tags

def save_leaderboard(chat_id: int, sorted_scores: list, set_info: dict = None):
    c    = _conn()
    tags = _section_tags(set_info)
    for uid, s in sorted_scores:
        for tag in tags:
            c.execute("""
                INSERT INTO leaderboard(chat_id,user_id,name,score,correct,wrong,quizzes,section_tag)
                VALUES(?,?,?,?,?,?,1,?)
                ON CONFLICT(chat_id,user_id,section_tag) DO UPDATE SET
                    score  = score   + excluded.score,
                    correct= correct + excluded.correct,
                    wrong  = wrong   + excluded.wrong,
                    quizzes= quizzes + 1,
                    name   = excluded.name,
                    ts     = datetime('now')
            """, (chat_id, uid, s["name"], s["score"], s["correct"], s["wrong"], tag))
    c.commit()

def get_leaderboard(chat_id: int, limit: int = 50,
                    section_tag: str = "overall") -> list:
    rows = _conn().execute("""
        SELECT name, score, correct, wrong, quizzes
        FROM leaderboard WHERE chat_id=? AND section_tag=?
        ORDER BY score DESC, wrong ASC LIMIT ?
    """, (chat_id, section_tag, limit)).fetchall()
    return [dict(r) for r in rows]

def get_subject_leaderboard(chat_id: int, subject_id: int, limit: int = 50) -> list:
    return get_leaderboard(chat_id, limit, f"subject_{subject_id}")

def get_topic_leaderboard(chat_id: int, topic_id: int, limit: int = 50) -> list:
    return get_leaderboard(chat_id, limit, f"topic_{topic_id}")

def get_user_rank(chat_id: int, user_id: int,
                  section_tag: str = "overall") -> Optional[dict]:
    row = _conn().execute("""
        SELECT name, score, correct, wrong, quizzes,
               (SELECT COUNT(*)+1 FROM leaderboard
                WHERE chat_id=? AND section_tag=? AND score > l.score) as rank
        FROM leaderboard l
        WHERE chat_id=? AND user_id=? AND section_tag=?
    """, (chat_id, section_tag, chat_id, user_id, section_tag)).fetchone()
    return dict(row) if row else None

def reset_leaderboard(chat_id: int, section_tag: str = None):
    c = _conn()
    if section_tag:
        c.execute("DELETE FROM leaderboard WHERE chat_id=? AND section_tag=?",
                  (chat_id, section_tag))
    else:
        c.execute("DELETE FROM leaderboard WHERE chat_id=?", (chat_id,))
    c.commit()

def get_global_leaderboard(limit: int = 20) -> list:
    """Sabhi chats ka combined leaderboard — DM mein use karo."""
    rows = _conn().execute("""
        SELECT name,
               SUM(score)   AS score,
               SUM(correct) AS correct,
               SUM(wrong)   AS wrong,
               SUM(quizzes) AS quizzes
        FROM leaderboard
        GROUP BY user_id
        ORDER BY score DESC, wrong ASC
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(r) for r in rows]

def get_user_global_rank(user_id: int) -> Optional[dict]:
    c   = _conn()
    agg = c.execute("""
        SELECT name, SUM(score) as score, SUM(correct) as correct,
               SUM(wrong) as wrong, SUM(quizzes) as quizzes
        FROM leaderboard WHERE user_id=? AND section_tag='overall'
        GROUP BY user_id
    """, (user_id,)).fetchone()
    if not agg: return None
    user_total = agg["score"]
    rank_row   = c.execute("""
        SELECT COUNT(DISTINCT user_id)+1 as rank FROM leaderboard
        WHERE user_id!=? AND section_tag='overall'
          AND (SELECT SUM(score) FROM leaderboard
               WHERE user_id=leaderboard.user_id AND section_tag='overall') > ?
    """, (user_id, user_total)).fetchone()
    return {
        "name"   : agg["name"],
        "score"  : agg["score"],
        "correct": agg["correct"],
        "wrong"  : agg["wrong"],
        "quizzes": agg["quizzes"],
        "rank"   : rank_row["rank"] if rank_row else 1,
    }

def get_global_stats() -> dict:
    c = _conn()
    return {
        "users"    : c.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "subjects" : c.execute("SELECT COUNT(*) FROM subjects").fetchone()[0],
        "topics"   : c.execute("SELECT COUNT(*) FROM topics").fetchone()[0],
        "sets"     : c.execute("SELECT COUNT(*) FROM question_sets").fetchone()[0],
        "questions": c.execute("SELECT COUNT(*) FROM questions").fetchone()[0],
        "answers"  : c.execute("SELECT COUNT(*) FROM answers").fetchone()[0],
    }


# ── Scheduling ───────────────────────────────────────────────────────────────

def schedule_quiz(chat_id: int, set_id: int, run_at: str, created_by: int) -> int:
    c = _conn()
    cur = c.execute(
        "INSERT INTO scheduled_quizzes(chat_id,set_id,run_at,created_by) VALUES(?,?,?,?)",
        (chat_id, set_id, run_at, created_by)
    )
    c.commit()
    return cur.lastrowid

def get_pending_schedules() -> list:
    rows = _conn().execute("""
        SELECT * FROM scheduled_quizzes
        WHERE done=0 AND run_at <= datetime('now')
    """).fetchall()
    return [dict(r) for r in rows]

def get_all_schedules(chat_id: int) -> list:
    rows = _conn().execute("""
        SELECT s.*, qs.name as set_name
        FROM scheduled_quizzes s
        JOIN question_sets qs ON qs.id=s.set_id
        WHERE s.chat_id=? AND s.done=0 ORDER BY s.run_at
    """, (chat_id,)).fetchall()
    return [dict(r) for r in rows]

def mark_schedule_done(schedule_id: int):
    c = _conn()
    c.execute("UPDATE scheduled_quizzes SET done=1 WHERE id=?", (schedule_id,))
    c.commit()

def delete_schedule(schedule_id: int):
    c = _conn(); c.execute("DELETE FROM scheduled_quizzes WHERE id=?", (schedule_id,)); c.commit()


# Auto-init
init_db()

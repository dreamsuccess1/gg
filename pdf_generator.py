"""
pdf_generator.py — Dream Success style Quiz PDF
Hindi (Devanagari) full support — FreeSans fonts registered at module load time
"""

import io
import os
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, HRFlowable
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ── Config ───────────────────────────────────────────────────────────────────
try:
    from config import BOT_NAME, BOT_USER, TARGET_TXT
except ImportError:
    BOT_NAME   = "Quiz Bot"
    BOT_USER   = "@quizbot"
    TARGET_TXT = "Target Exams"

# ── Font Registration — module load hote hi register karo ────────────────────
# Yahi bug tha: pehle generate_result_pdf() ke andar register hota tha
# lekin style objects pehle hi ban jaate the Helvetica ke saath
_FONT  = "Helvetica"
_FONTB = "Helvetica-Bold"

for _r, _b in [
    ("/usr/share/fonts/truetype/freefont/FreeSans.ttf",
     "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf"),
    ("/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
     "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf"),
]:
    if os.path.exists(_r) and os.path.exists(_b):
        try:
            pdfmetrics.registerFont(TTFont("HF",  _r))
            pdfmetrics.registerFont(TTFont("HFB", _b))
            pdfmetrics.registerFontFamily("HF",
                normal="HF", bold="HFB", italic="HF", boldItalic="HFB")
            _FONT  = "HF"
            _FONTB = "HFB"
            break
        except Exception:
            continue

# ── Colors ───────────────────────────────────────────────────────────────────
CLR_BLUE       = colors.HexColor("#1565C0")
CLR_BLUE_LIGHT = colors.HexColor("#1976D2")
CLR_GREEN_TXT  = colors.HexColor("#2E7D32")
CLR_GREY_ROW   = colors.HexColor("#F5F5F5")
CLR_WHITE      = colors.white
CLR_BLACK      = colors.HexColor("#212121")
CLR_BORDER     = colors.HexColor("#BDBDBD")
CLR_RED_TXT    = colors.HexColor("#C62828")
CLR_GREEN_BG   = colors.HexColor("#E8F5E9")
CLR_RED_BG     = colors.HexColor("#FFEBEE")
LABELS         = ["A", "B", "C", "D"]

_sc = [0]
def _s(bold=False, **kw):
    _sc[0] += 1
    return ParagraphStyle(f"s{_sc[0]}", fontName=_FONTB if bold else _FONT, **kw)

def _hdr(txt):
    t = Table([[Paragraph(f"  {txt}", _s(bold=True, fontSize=11, textColor=CLR_WHITE))]])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), CLR_BLUE_LIGHT),
        ("TOPPADDING",    (0,0),(-1,-1), 7),
        ("BOTTOMPADDING", (0,0),(-1,-1), 7),
    ]))
    return t

def generate_result_pdf(
    quiz_title      : str,
    quiz_day        : str,
    quiz_date       : str,
    total_questions : int,
    scoring         : str,
    leaderboard     : list,
    questions       : list,
    student_answers : dict,
    student_name    : str = None,
) -> io.BytesIO:

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        rightMargin=1.2*cm, leftMargin=1.2*cm,
        topMargin=1.2*cm,   bottomMargin=1.2*cm)
    story = []

    # HEADER
    story.append(Paragraph(
        f"{BOT_NAME} \u2014 {quiz_title}",
        _s(bold=True, fontSize=17, textColor=CLR_BLUE, alignment=1, spaceAfter=2)
    ))
    story.append(Paragraph(
        quiz_day,
        _s(bold=True, fontSize=13, textColor=CLR_BLUE, alignment=1, spaceAfter=3)
    ))
    story.append(Paragraph(
        f"{TARGET_TXT}  |  {quiz_date}  |  {total_questions} Questions  |  {scoring}",
        _s(fontSize=9, textColor=CLR_BLACK, alignment=1, spaceAfter=5)
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=CLR_BLUE, spaceAfter=8))

    # LEADERBOARD
    story.append(_hdr("LEADERBOARD"))
    story.append(Spacer(1, 3))

    cs = _s(bold=True, fontSize=9, textColor=CLR_WHITE,  alignment=1)
    ce = _s(           fontSize=8, textColor=CLR_BLACK)
    cb = _s(bold=True, fontSize=9, textColor=CLR_BLUE)

    lb_data = [[
        Paragraph("Rank", cs), Paragraph("Participant", cs),
        Paragraph("Score", cs), Paragraph("Wrong", cs),
        Paragraph("Acc%", cs), Paragraph("Time", cs),
    ]]
    for row in leaderboard[:60]:
        lb_data.append([
            Paragraph(str(row.get("rank",  "")),       ce),
            Paragraph(str(row.get("name",  ""))[:32],  ce),
            Paragraph(str(row.get("score", "")),       cb),
            Paragraph(str(row.get("wrong", "")),       ce),
            Paragraph(f"{row.get('acc', '')}%",        ce),
            Paragraph(str(row.get("time",  "")),       ce),
        ])

    lb = Table(lb_data,
               colWidths=[1.2*cm, 7.5*cm, 2*cm, 1.5*cm, 1.5*cm, 2.3*cm],
               repeatRows=1)
    lb.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1, 0), CLR_BLUE_LIGHT),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [CLR_WHITE, CLR_GREY_ROW]),
        ("GRID",          (0,0),(-1,-1), 0.4, CLR_BORDER),
        ("TOPPADDING",    (0,0),(-1,-1), 4),
        ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ("LEFTPADDING",   (0,0),(-1,-1), 5),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
    ]))
    story.append(lb)
    story.append(Spacer(1, 12))

    # QUESTIONS & ANSWERS
    story.append(_hdr("QUESTIONS & ANSWERS"))
    story.append(Spacer(1, 6))

    qn_s  = _s(bold=True, fontSize=8,   textColor=CLR_WHITE)
    qt_s  = _s(bold=True, fontSize=8,   textColor=CLR_BLACK,     leading=13)
    oc_s  = _s(bold=True, fontSize=7.5, textColor=CLR_GREEN_TXT, leading=11)
    ow_s  = _s(           fontSize=7.5, textColor=CLR_BLACK,     leading=11)
    oww_s = _s(           fontSize=7.5, textColor=CLR_RED_TXT,   leading=11)

    def make_q(idx, q):
        chosen  = -1
        if student_answers:
            chosen = student_answers.get(idx, student_answers.get(str(idx), -1))
        correct = q.get("correct", 0)
        opts    = q.get("options", [])

        qh = Table([[
            Paragraph(f" Q{idx+1}", qn_s),
            Paragraph(str(q.get("question", ""))[:300], qt_s),
        ]], colWidths=[1*cm, 7.8*cm])
        qh.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(0,0), CLR_BLUE),
            ("BACKGROUND",    (1,0),(1,0), CLR_GREY_ROW),
            ("TOPPADDING",    (0,0),(-1,-1), 5),
            ("BOTTOMPADDING", (0,0),(-1,-1), 5),
            ("LEFTPADDING",   (0,0),(-1,-1), 4),
            ("VALIGN",        (0,0),(-1,-1), "TOP"),
        ]))

        rows = [[qh]]
        for j, opt in enumerate(opts[:4]):
            lbl = f"{LABELS[j]}) {opt}"
            if j == correct:
                st, bg = oc_s, CLR_GREEN_BG
            elif j == chosen:
                st, bg = oww_s, CLR_RED_BG
            else:
                st, bg = ow_s, CLR_WHITE

            or_ = Table([[Paragraph(f"  {lbl}", st)]], colWidths=[8.8*cm])
            or_.setStyle(TableStyle([
                ("BACKGROUND",    (0,0),(-1,-1), bg),
                ("TOPPADDING",    (0,0),(-1,-1), 3),
                ("BOTTOMPADDING", (0,0),(-1,-1), 3),
                ("LEFTPADDING",   (0,0),(-1,-1), 6),
                ("LINEBELOW",     (0,0),(-1,-1), 0.3, CLR_BORDER),
            ]))
            rows.append([or_])

        ct = Table(rows, colWidths=[8.8*cm])
        ct.setStyle(TableStyle([
            ("BOX",           (0,0),(-1,-1), 0.5, CLR_BORDER),
            ("TOPPADDING",    (0,0),(-1,-1), 0),
            ("BOTTOMPADDING", (0,0),(-1,-1), 0),
            ("LEFTPADDING",   (0,0),(-1,-1), 0),
            ("RIGHTPADDING",  (0,0),(-1,-1), 0),
        ]))
        return ct

    pairs = []
    for i in range(0, len(questions), 2):
        left  = make_q(i, questions[i])
        right = make_q(i+1, questions[i+1]) if i+1 < len(questions) else ""
        pairs.append([left, right])

    if pairs:
        grid = Table(pairs, colWidths=[9*cm, 9*cm], hAlign="LEFT")
        grid.setStyle(TableStyle([
            ("VALIGN",        (0,0),(-1,-1), "TOP"),
            ("TOPPADDING",    (0,0),(-1,-1), 4),
            ("BOTTOMPADDING", (0,0),(-1,-1), 4),
            ("LEFTPADDING",   (0,0),(-1,-1), 2),
            ("RIGHTPADDING",  (0,0),(-1,-1), 2),
        ]))
        story.append(grid)

    # FOOTER
    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=1, color=CLR_BORDER))
    now    = datetime.now().strftime("%d %b %Y, %I:%M %p")
    footer = f"Generated by {BOT_USER}  \u2022  Pro Report Edition  \u2022  {now}"
    if student_name:
        footer = f"Result for: {student_name}  \u2022  " + footer
    story.append(Paragraph(
        footer,
        _s(fontSize=8, textColor=colors.grey, alignment=1, spaceBefore=4)
    ))

    doc.build(story)
    buf.seek(0)
    return buf

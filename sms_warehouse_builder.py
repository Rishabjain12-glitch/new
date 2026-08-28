"""
===============================================================================
SMS WAREHOUSE BUILDER — v7 (Commented & Consolidated)
===============================================================================

PURPOSE:
    Reads a CSV of raw SMS messages from Indian stock-market brokers,
    depositories (NSDL / CDSL), exchanges (NSE / BSE / MCX), and fintech
    apps (Groww, Zerodha, Angel One, MOFSL, Upstox, Dhan, etc.).

    Each SMS is classified by regex rules into a labelled category, parsed
    for structured fields (dates, amounts, quantities, security names …),
    and routed into one of 12 normalised output tables.

    All output tables are written as sheets inside ONE Excel workbook.

OUTPUT SHEETS (rename as needed — "change when required"):
    • Enriched_Master        – every SMS with its classifier & parsed fields
    • Broker_Associations    – broker ↔ client registrations & account events
    • Transactions           – share movements, trades, payouts, corp-actions
    • Pledge_Activity        – pledge / unpledge / invocation records
    • EOD_Balances           – end-of-day fund & securities balances
    • Mutual_Funds           – SIP, lump-sum, MF conversion activity
    • IPO_Lifecycle          – IPO application → allotment tracking
    • KYC_Changes            – profile / KYC field modifications
    • Account_Events         – account open / close / TPIN / voting …
    • Margin_Risk_Alerts     – margin calls, MTM loss, shortfall warnings
    • Statements_Docs        – CAS, contract-note, PnL report delivery
    • Portfolio_Valuations   – point-in-time portfolio value snapshots
    • Advisory_Promo         – promos, advisories, uncategorised SMS
    • Row_Summary            – row count per table (QA helper)
    • Multi_Rule_Hits        – SMS that matched more than one rule (QA)
    • Uncategorised          – SMS that matched no rule at all (QA)

CHANGE LOG (vs v6):
    • Added comprehensive comments with "change when required" hints.
    • Renamed all tables to short, readable names.
    • Consolidated all CSV + XLSX outputs into a single .xlsx workbook.
    • Fixed `if __name__` guard typo.

USAGE:
    python sms_warehouse_v7.py
    (edit INPUT_CSV and OUTPUT_DIR at the bottom before running)

===============================================================================
"""

# ──────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ──────────────────────────────────────────────────────────────────────────────
import re          # regex engine for SMS classification
import os          # file-system helpers
import time        # wall-clock timing
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor  # optional parallel classify

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL CONFIG  — change when required
# ──────────────────────────────────────────────────────────────────────────────
USE_MULTIPROCESSING = False          # set True for large files (>500 k rows)
CLASSIFY_CHUNK_SIZE = 5_000          # rows per chunk when multiprocessing


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 1 — UTILITY FUNCTIONS                                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


def _p(pattern):
    """Whenever I make a regex for SMS matching, always make it case-insensitive and allow it to match across multiple lines.
    """
    return re.compile(pattern, re.I | re.S)


def clean_text(x):
    """Normalise whitespace: collapse runs of spaces / nbsp into one space.
    Returns empty string for NaN / None.
    — change when required (e.g. strip emojis, HTML tags)
    """
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).replace("\u00a0", " ")).strip()


def to_num(x):
    """Extract a float from a string like '1,23,456.78' or '-99.5'.
    Returns np.nan when nothing numeric is found.
    — change when required (e.g. handle lakhs / crores labels)
    """
    if x is None or pd.isna(x):
        return np.nan
    m = re.search(r"-?[\d,]+(?:\.\d+)?", str(x))
    return float(m.group().replace(",", "")) if m else np.nan


def parse_date(x, fallback=pd.NaT):
    """Try multiple Indian-style date formats; return a tz-naive Timestamp.
    Falls back to pd.to_datetime with dayfirst=True as last resort.
    — change when required (add new date formats here)
    """
    if x is None or pd.isna(x) or not str(x).strip():
        return fallback
    s = str(x).strip().replace("/", "-").replace(".", "-")
    # List of formats commonly seen in Indian broker / depository SMS
    for fmt in (
        "%d-%m-%Y", "%d-%m-%y", "%d%b%Y", "%d%b%y", "%d-%b-%Y",
        "%d-%b-%y", "%d-%B-%Y", "%Y-%m-%d", "%d %b %Y",
        "%b %d %Y", "%d%m%Y",
    ):
        try:
            return pd.Timestamp(datetime.strptime(s, fmt)).normalize()
        except ValueError:
            pass
    # Pandas fallback
    z = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return z.normalize() if pd.notna(z) else fallback


def pick_column(df, candidates, required=True):
    """Find the first column name in `df` that matches any of `candidates`
    (case-insensitive).  Raises ValueError when `required` and nothing found.
    — change when required (add alternate header spellings)
    """
    lower = {str(c).lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    if required:
        raise ValueError(
            f"Missing one of {candidates}. Columns present: {list(df.columns)}"
        )
    return None


def canon(s):
    """Canonicalise a classifier label: strip non-alphanum, upper-case,
    then map through ALIASES dict so comparisons are consistent.
    """
    s = re.sub(r"[^A-Z0-9]+", "", str(s).upper())
    return ALIASES.get(s, s)


def norm_fields(d):
    """Post-process named-group dict from regex match:
    1. Strip `rx_` prefix that every named group carries.
    2. Clean whitespace.
    3. Merge synonym keys (e.g. 'ac_no' → 'acno').
    — change when required (add new synonym pairs)
    """
    d = {
        re.sub(r"^rx_", "", k).lower(): clean_text(v)
        for k, v in d.items() if v not in (None, "")
    }
    # Synonym mapping: left key is aliased to right key if right is absent
    alias_pairs = {
        "date2": "date", "ac_no": "acno", "ac_masked": "acmasked",
        "pledgee_ac": "pledgeeac", "fund_bal": "fundbal",
        "fund_balance": "fundbal", "sec_bal": "secbal",
        "securities_balance": "secbal", "mcx_bal": "mcxbal",
        "nse_fo": "nsefo", "led_bal": "ledbal", "buy_val": "buyval",
        "sell_val": "sellval", "buy_qty": "buyqty", "sell_qty": "sellqty",
        "memcode": "membercode", "dp_id": "dpid",
        "client_id": "parsed_client_id", "client_code": "clientcode",
        "broker_name": "brokername", "eq_value": "eqvalue",
        "fno_value": "fnovalue", "currency_value": "currencyvalue",
        "sip_id": "sipid", "app_id": "appid",
        "application_no": "applicationno", "issue_name": "issuename",
        "dp_name": "dpname", "close_date": "closedate",
        "new_mobile": "newmobile", "bo_id": "boid", "bo_last4": "bolast4",
        "ac_last4": "aclast4", "pan_masked": "panmasked",
        "amount2": "amount", "util2": "util", "account2": "account",
        "old_bank_ac": "oldbankac", "new_bank_ac": "newbankac",
        "old_bank": "oldbank", "new_bank": "newbank",
        "client2": "client",
    }
    for a, b in alias_pairs.items():
        if a in d and b not in d:
            d[b] = d[a]
    return d


def val(f, *keys):
    """Return the first non-empty value from dict `f` for given keys.
    Useful when a field may appear under different names depending on
    which regex matched.
    """
    for k in keys:
        z = f.get(k)
        if z not in (None, "") and not pd.isna(z):
            return z
    return np.nan


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 2 — SECURITY-NAME / SEGMENT HELPERS                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


def clean_security_name(sec):
    """Strip common prefixes / suffixes from a raw security-name string
    extracted by regex (qty prefix, ISIN suffix, 'Ordinary Shares -' etc.)
    — change when required (add more noise patterns)
    """
    if pd.isna(sec) or not clean_text(sec):
        return np.nan
    s = clean_text(sec)
    # Remove leading "qty  share-type  -" boilerplate
    s = re.sub(
        r"^(?:\d+(?:\.\d+)?\s+)?"
        r"(?:ordinary\s+shares?|equity\s+shares?|n\s+equity\s+shares?"
        r"|units?\s+of|shares?\s+of|rights?\s+entitlement\s+of"
        r"|open\s+ended\s+mutual\s+fund)"
        r"\s*[-–\s]+",
        "", s, flags=re.I)
    # Remove leading qty dash
    s = re.sub(r"^\d+(?:\.\d+)?\s*[-–]\s*", "", s)
    # Remove trailing date
    s = re.sub(r"\s+on\s+\d{2}[/-]?\d{2}[/-]?\d{2,4}\s*$", "", s, flags=re.I)
    s = re.sub(r"\s+on\s+\d{2}[A-Za-z]{3}\d{0,4}\s*$", "", s, flags=re.I)
    # Remove trailing ISIN count
    s = re.sub(r",?\s*Total\s+\d+\s+ISINs?.*$", "", s, flags=re.I)
    # Remove trailing depository label
    s = re.sub(r"\s*-\s*(?:CDSL|NSDL)\s*$", "", s, flags=re.I)
    # Remove trailing ISIN code
    s = re.sub(r"\s+IN[EF]\d{9}\s*$", "", s)
    # Remove trailing Face Value
    s = re.sub(
        r"\s*(?:Face|FV|F\.V\.)\s*(?:Value)?\s*(?:Rs\.?\s*)?\d+/?-?\s*$",
        "", s, flags=re.I)
    # Strip leading/trailing punctuation
    s = re.sub(r"^[\s\-–.:,]+|[\s\-–.:,]+$", "", s)
    return s if s else np.nan


# ── ETF keyword list — change when required (add new ETF names) ──
_ETF_KEYWORDS = [
    "ETF", "BEES", "LIQUIDBEES", "GOLDBEES", "BANKBEES", "NIFTYBEES",
    "JUNIORBEES", "SILVERBEES", "CPSEETF", "BHARAT22ETF", "CPSE ETF",
    "NIPPON INDIA ETF", "SBI ETF", "KOTAK BANKING ETF",
    r"MOTILAL OSWAL.*ETF", "ICICIETF", "KOTAKBKETF", "SBIETF",
    r"NIFTY.*ETF", r"SENSEX.*ETF", "MON100", "MOM100", "NETF",
    "HDFCNIFETF", "HDFCSENETF", "UTINIFTETF",
]
_ETF_RE = re.compile("|".join(_ETF_KEYWORDS), re.I)


def detect_segment(security_name, cls_text=""):
    """Return 'ETF' if security matches an ETF keyword, else 'EQ'.
    — change when required (add 'MF', 'REIT', 'InvIT' detection)
    """
    combined = str(security_name) + " " + str(cls_text)
    if _ETF_RE.search(combined):
        return "ETF"
    return "EQ"


def detect_mf_or_etf(fund_name, sms_text=""):
    """Distinguish Mutual-Fund from ETF based on name keywords.
    — change when required
    """
    combined = str(fund_name) + " " + str(sms_text)
    if _ETF_RE.search(combined):
        return "ETF"
    return "MF"


def holdings(s):
    """Parse a comma-separated holdings string like
    '100 - Reliance Industries, 50 - TCS'  into list of (qty, name) tuples.
    """
    if pd.isna(s) or not clean_text(s):
        return [(np.nan, np.nan)]
    ans = []
    for p in re.split(r",\s*(?![^()]*\))", clean_text(s)):
        m = re.match(r"\s*(\d+(?:\.\d+)?)\s*[- ]\s*(.+?)\s*$", p)
        ans.append((
            to_num(m.group(1)) if m else np.nan,
            clean_text(m.group(2) if m else p),
        ))
    return ans


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 3 — BROKER NORMALISATION                                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# Master list of broker-name keywords → canonical display names
# — change when required (add new brokers / spelling variants)
BROKERS = [
    ("MOTILAL OSWAL", "Motilal Oswal"), ("MOTILALOSWAL", "Motilal Oswal"),
    ("ANGEL ONE", "Angel One"), ("ANGELONE", "Angel One"),
    ("ZERODHA", "Zerodha"), ("GROWW", "Groww"), ("UPSTOX", "Upstox"),
    ("KOTAK", "Kotak"), ("ICICI SECURITIES", "ICICI Securities"),
    ("ICICI DIRECT", "ICICI Direct"), ("ICICIDIRECT", "ICICI Direct"),
    ("HDFC SECURITIES", "HDFC Securities"), ("HDFC SKY", "HDFC Sky"),
    ("SHAREKHAN", "Sharekhan"), ("IIFL", "IIFL"), ("MOSL", "Motilal Oswal"),
    ("L.F.C.SECURITIES", "L.F.C. Securities"),
    ("LFC SECURITIES", "L.F.C. Securities"),
    ("DHAN", "Dhan"), ("5PAISA", "5Paisa"), ("5 PAISA", "5Paisa"),
    ("AXIS SECURITIES", "Axis Securities"), ("PAYTM MONEY", "Paytm Money"),
    ("SBI SECURITIES", "SBI Securities"), ("SBISEC", "SBI Securities"),
    ("GEOJIT", "Geojit"), ("EDELWEISS", "Edelweiss"), ("NUVAMA", "Nuvama"),
    ("FYERS", "Fyers"), ("CHOICE", "Choice Broking"),
    ("SMC GLOBAL", "SMC Global"), ("RELIGARE", "Religare"),
    ("JM FINANCIAL", "JM Financial"), ("JMFINANCIAL", "JM Financial"),
    ("BONANZA", "Bonanza Portfolio"), ("BONANZAPORTFOLIO", "Bonanza Portfolio"),
    ("ANAND RATHI", "Anand Rathi"), ("ANANDRATHI", "Anand Rathi"),
    ("NIRMAL BANG", "Nirmal Bang"), ("NIRMALBANG", "Nirmal Bang"),
    ("ALICE BLUE", "Alice Blue"), ("ALICEBLUE", "Alice Blue"),
    ("KARVY", "Karvy"), ("VENTURA", "Ventura"),
    ("ADITYA BIRLA", "Aditya Birla Money"), ("SAMCO", "Samco"),
    ("TRUSTLINE", "Trustline"), ("MASTERTRUST", "Master Trust"),
    ("NSE INVEST", "NSE Invest"), ("NSEINVEST", "NSE Invest"),
]


def normalize_broker_name(name):
    """Map a raw broker string to its canonical display name using the
    BROKERS lookup table.  Falls back to title-casing.
    — change when required (add new broker aliases above)
    """
    if pd.isna(name) or not clean_text(name):
        return np.nan
    raw_u = clean_text(name).upper()
    for k, v in BROKERS:
        if k in raw_u:
            return v
    # Fallback: clean up and title-case
    compact = re.sub(r"\s+", " ", clean_text(name)).strip(" -,.")
    titled = " ".join(
        w.capitalize() if not w.isupper() else w for w in compact.split()
    )
    return titled


def broker_from_sms(text, f):
    """Determine the broker name from an SMS body + parsed fields.
    Tries:  1) explicit field from regex  2) keyword scan of body
            3) structural patterns like 'Regards, <Broker>'
    — change when required (add new detection patterns)
    """
    # 1. Try the parsed field first
    direct = val(f, "broker", "brokername", "dp", "dpname")
    direct_norm = normalize_broker_name(direct)
    if pd.notna(direct_norm):
        return direct_norm

    # 2. Keyword scan
    u = clean_text(text).upper()
    if "NSE INVEST" in u or "NSEINVEST" in u:
        return "NSE Invest"
    for k, v in BROKERS:
        if k in u:
            return v

    # 3. Structural patterns in the SMS body
    patterns = [
        r"with broker\s+([A-Z][A-Z0-9 .&\-]{2,})",
        r"with\s+([A-Z][A-Z0-9 .&\-]{2,})\s*-\s*DP ID",
        r"([A-Z][A-Za-z .&']+?)\s+at EOD\s+\d",
        r"Dear Investor,\s*your (?:DP|stock broker)\s+(.+?)\s+has initiated",
        r"On\s+\d{2}/?\d{2}/?\d{2},\s*(.+?)\s+was registered as",
        r"(?:Regards|Team)\s*,?\s*([A-Z][A-Za-z ]+?)(?:\.|$)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            found = normalize_broker_name(m.group(1))
            if pd.notna(found):
                return found

    return np.nan


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 4 — SMALL DETECTION HELPERS                                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# ── SIP detection in SMS body ──
_SIP_BODY_RE = re.compile(
    r"\bSIP\b|systematic\s+investment|recurring\s+(?:plan|investment)"
    r"|upcoming\s+(?:SIP|payment)|SIP\s+(?:set-?up|registration|due)"
    r"|XSIP|x-?sip|Auto-?pay.*?SIP", re.I)


def is_sip_from_text(cls, text):
    """Return True if the classifier label or SMS body indicates SIP."""
    if "SIP" in canon(cls):
        return True
    return bool(_SIP_BODY_RE.search(str(text)))


def detect_trading_segment(text, f):
    """Infer the trading segment (Cash Equity / F&O / Commodity / Currency)
    from the SMS body or the parsed 'segment' field.
    — change when required (add new segment keywords)
    """
    u = str(text).upper()
    seg = val(f, "segment")
    if pd.notna(seg):
        return clean_text(seg)
    if "COMMODITY" in u:
        return "Commodity Derivatives"
    if re.search(r"\bF\s*&?\s*O\b|\bFNO\b|\bFUTURES?\b|\bOPTION", u):
        return "F&O"
    if re.search(r"\bCASH EQUITY\b|\bCM\s+SEGMENT\b", u):
        return "Cash Equity"
    if re.search(r"\bCURRENCY\b|\bCD\s+SEGMENT\b", u):
        return "Currency Derivatives"
    return np.nan


def sms_says_not_allotted(text):
    """Check if the SMS body indicates IPO non-allotment."""
    u = str(text).upper()
    return bool(re.search(
        r"NOT\s+BEEN\s+ALLOT|NO\s+ALLOT|NOT\s+ALLOT|UNSUCCESSFUL\s+ALLOT"
        r"|REGRET|COULD\s+NOT\s+BE\s+ALLOT|UNABLE\s+TO\s+ALLOT"
        r"|YOU\s+HAVE\s+NOT\s+BEEN\s+ALLOTTED", u))


def family_depository(fam):
    """Return 'NSDL' or 'CDSL' if the classifier family is a depository,
    else np.nan.
    """
    return fam if fam in {"NSDL", "CDSL"} else np.nan


def exchange_from(cls, text):
    """Best-effort exchange detection from classifier + SMS body."""
    u = (str(cls) + " " + str(text)).upper()
    if "MCX" in u:
        return "MCX"
    if "BSE" in u:
        return "BSE"
    if "NSE" in u:
        return "NSE"
    return np.nan


# ── Regex helpers for extracting security names from specific SMS patterns ──
_TPIN_BODY_RE = re.compile(r"TPIN|e-?DIS\s+facility|BOID|BO\s+ID", re.I)
_EVOTING_COMPANY_RE = re.compile(
    r"e-?Voting for\s+(?P<company>.+?)\s+(?:will be open|begins)", re.I)
_VOTE_CAST_COMPANY_RE = re.compile(
    r"casted your vote for\s+\d+\s+resolutions of\s+(?P<company>.+?)\s*\.\s*",
    re.I)
_PLEDGE_COMPANY_RE = re.compile(
    r"instruction for\s+\d+\s+(?P<company>.+?),", re.I)


def extract_security_from_sms(text, f):
    """Try to pull a company / security name from parsed fields or body."""
    sec = val(f, "security", "company", "issuename")
    if pd.notna(sec):
        cleaned = clean_security_name(sec)
        if pd.notna(cleaned):
            return cleaned
    for pat in (_EVOTING_COMPANY_RE, _VOTE_CAST_COMPANY_RE, _PLEDGE_COMPANY_RE):
        m = pat.search(str(text))
        if m:
            return clean_security_name(m.group("company"))
    return np.nan


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 5 — CLASSIFICATION RULES                                         ║
# ║                                                                            ║
# ║  Every entry is  (label, compiled_regex_with_named_groups).                ║
# ║  Named groups use an `rx_` prefix that gets stripped by norm_fields().     ║
# ║                                                                            ║
# ║  — change when required: add / edit / remove rules within any family.     ║
# ║    The first matching rule in the FIRST matching family wins.              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

RULES = {

    # ── ACCOUNT-LEVEL EVENTS (broker-agnostic) ──
    "ACCOUNT": [
        ("ACCOUNT_CLOSED", _p(
            r"account (?:has been\s+)?closed successfully"
            r"|(?:Trading|Demat)[^.]*?account[^.]*?closed successfully")),
        ("ACCOUNT_CLOSURE_REQUEST", _p(
            r"closure request (?:has been )?(?:received|processed)"
            r"|(?:received|request for)[^.]*?closure|account closure request")),
        ("ACCOUNT_SUSPENDED", _p(
            r"marked inactive|temporarily suspended")),
        ("ACCOUNT_ACTIVATED", _p(
            r"activated successfully|account is now active"
            r"|account activation (?:has been )?(?:completed|complete)"
            r"|activation is complete|activation has been completed"
            r"|activation is in progress|activation is under process"
            r"|Your account activation")),
        ("ACCOUNT_OPENED", _p(
            r"successfully opened|opened successfully"
            r"|opening (?:request )?(?:has been )?(?:successfully )?(?:approved|completed|complete)"
            r"|opening process (?:has been )?(?:completed|complete|is complete)"
            r"|opening is complete"
            r"|(?:UCC|Unique Client Code|Client Code)[^.]*?(?:has been\s+)?(?:successfully )?registered"
            r"|(?:has been\s+)?(?:successfully )?registered (?:successfully )?with"
            r"|BO ID[^.]*?(?:activated|opened) successfully")),
    ],

    # ── NSDL DEPOSITORY SMS ──
    "NSDL": [
        # Share debited for pay-in settlement
        ("PAY_IN_DEBIT", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?)"
            r"\s+[Dd]ebited from your (?:a/?c|ac|demat\s+a/?c\s*no\.?)\s+(?P<rx_ac_masked>xxxx\d+)"
            r".*?(?:for\s+)?(?:Pay-in|Pay\s*in).*?NSDL")),
        # Securities blocked for early pay-in / eDIS
        ("BLOCKED_EARLY_PAYIN", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?)"
            r"\s+Blocked in your a/?c\s+(?P<rx_ac_masked>xxxx\d+)"
            r"\s+for\s+(?:eDIS\s+)?Early Pay-?in.*?NSDL")),
        # Securities blocked for debit
        ("BLOCKED_FOR_DEBIT", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?)"
            r"\s+has been blocked for debiting from your NSDL\s+(?:a/?c|ac)\s+"
            r"(?P<rx_ac_masked>xxxx\d+)")),
        # IPO / Bonus / Public Offer credit with reason
        ("CREDIT_PUBLIC_OFFER", _p(
            r"(?P<rx_qty>[\d.]+)\s+(?:shares?|Debentures?|units?)\s+of\s+(?P<rx_security>.+?)"
            r"\s+credited to your demat a/?c\s*no\.?\s+(?P<rx_ac_masked>xxxx\d+)"
            r"\s+on\s+(?P<rx_date>\d{2}/\d{2}/\d{2})"
            r"\s+in respect of\s+"
            r"(?P<rx_reason>Public Offer|Bonus|Dividend|Rights|IPO|FPO|ESOP)"
            r".*?NSDL")),
        # Generic credit into demat
        ("CREDIT_GENERAL", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?)"
            r"\s+[Cc]redited (?:to|in) your (?:a/?c|ac|demat\s+a/?c\s*no\.?)\s+"
            r"(?P<rx_ac_masked>xxxx\d+).*?NSDL")),
        # Debit due to pledge invocation
        ("DEBIT_INVOCATION", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?)"
            r"\s+debited due to invocation of pledged securities"
            r"\s+from\s+(?:ur|your)\s+demat a/?c\s*no\.?\s+(?P<rx_ac_masked>xxxx\d+)"
            r".*?NSDL")),
        # Generic debit from demat
        ("DEBIT_MISC", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?)"
            r"\s+[Dd]ebited from your (?:a/?c|ac)\s+(?P<rx_ac_masked>xxxx\d+).*?NSDL")),
        # Securities blocked for buyback offer
        ("BLOCKED_BUYBACK", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?)"
            r"\s+Blocked in your a/?c\s+(?P<rx_ac_masked>xxxx\d+)"
            r"\s+for Buyback Offer.*?NSDL")),
        # Pledge initiation confirmation
        ("PLEDGE_INITIATION", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2})\s+Pledge initiation instruction for"
            r"\s+(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?),"
            r"\s+has been confirmed in respect of.*?(?:demat\s+)?a/?c(?:c)?\s*no\.?\s*"
            r"(?P<rx_ac_masked>xxxx\d+).*?NSDL")),
        # Auto-pledge (CUSPA) initiation
        ("AUTO_PLEDGE_CUSPA", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2})\s+Auto Pledge initiation\s*\(CUSPA\)"
            r".*?instruction for\s+(?P<rx_qty>[\d.]+)\s+(?P<rx_security>.+?),"
            r".*?(?:demat\s+)?a/?c\s*no\.?\s*(?P<rx_ac_masked>xxxx\d+).*?NSDL")),
        # e-Voting notice
        ("EVOTING_NOTICE", _p(
            r"Dear Investor,\s+e-Voting for\s+(?P<rx_security>.+?)"
            r"\s+will be open from\s+(?P<rx_start_dt>\d{2}-[A-Za-z]+-\d{4}\s+[\d:]+\s*[AP]M)"
            r".*?end on\s+(?P<rx_end_dt>\d{2}-[A-Za-z]+-\d{4}\s+[\d:]+\s*[AP]M).*?NSDL")),
        # Margin pledge notification from DP
        ("MARGIN_PLEDGE_NOTIFICATION", _p(
            r"Dear Investor,\s+your (?:DP|stock broker)\s+(?P<rx_dp>.+?)"
            r"\s+has initiated\s+margin pledge from your NSDL demat account"
            r"\s+(?P<rx_dp_id>IN\S+)\s+(?P<rx_ac_masked>xxxx\d+)")),
        # Beneficiary account addition / deletion
        ("BENEFICIARY_ADDITION", _p(
            r"your DP\s+(?P<rx_dp>.+?)\s+has initiated Beneficiary account"
            r"\s+addition/deletion request.*?NSDL demat account\s+(?P<rx_dp_id>IN\S+)")),
        # Monthly CAS statement sent
        ("CAS_STATEMENT", _p(
            r"Your NSDL CAS for the month of\s+(?P<rx_months>.+?)"
            r"\s+has been sent on\s+your registered email ID\s+(?P<rx_email>\S+)")),
        # Bonus / corporate-action credit
        ("BONUS_CREDIT", _p(
            r"(?P<rx_qty>[\d.]+)\s+shares of\s+(?P<rx_security>.+?)"
            r"\s+Credited in your\s+demat (?:a/?c|ac)\s+(?P<rx_ac_masked>xxxx\d+)"
            r"\s+on\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2})"
            r"\s+in respect of\s+"
            r"(?P<rx_reason>Bonus|Dividend|Rights|IPO|FPO|ESOP|Public Offer)")),
        # Vote cast confirmation
        ("VOTE_CAST_CONFIRMATION", _p(
            r"You have successfully casted your vote for\s+(?P<rx_resolutions>\d+)"
            r"\s+resolutions of\s+(?P<rx_security>.+?)\s*\.\s*(?:\(|Regards)")),
        # Mobile number changed
        ("MOBILE_UPDATED_NSDL", _p(
            r"As per ur request,\s*ur mobile no\. for receiving SMS Alerts"
            r".*?for demat a/?c\s+(?P<rx_ac_masked>xxxx\d+)"
            r"\s+is changed to\s+(?P<rx_new_mobile>[\d.eE+]+)")),
        # MF units debited from demat
        ("MF_UNITS_DEBIT", _p(
            r"(?P<rx_qty>[\d.]+)\s+units? of\s+(?P<rx_security>.+?)"
            r"\s+[Dd]ebited from\s+your demat (?:a/?c|ac) no\s+"
            r"(?P<rx_ac_masked>xxxx\d+)")),
        # MF units credited to demat
        ("MF_UNITS_CREDIT", _p(
            r"(?P<rx_qty>[\d.]+)\s+units? of\s+(?P<rx_security>.+?)"
            r"\s+credited on\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2})"
            r"\s+to your demat a/?c no\s+(?P<rx_ac_masked>xxxx\d+).*?NSDL")),
        # Designated-person TWCP restriction
        ("DESIGNATED_PERSON_TWCP", _p(
            r"As a designated person,\s+transaction are restricted for ISIN:?\s+"
            r"(?P<rx_isin>\S+).*?during TWCP.*?NSDL")),
        # TWCP restriction removed
        ("DESIGNATED_PERSON_TWCP_REMOVED", _p(
            r"Transaction which were restricted for ISIN\s+(?P<rx_isin>\S+)"
            r"\s+in your Demat Account DP ID\s+(?P<rx_dp_id>\S+)"
            r"\s+Client ID\s+(?P<rx_client_id>\d+)"
            r"\s+during TWCP.*?has been removed.*?NSDL")),
        # Welcome to NSDL SMS facility
        ("WELCOME_SMS_ALERT", _p(
            r"Welcome to NSDL SMS Alert Facility for your Demat Account")),
        # DP closure notice
        ("DP_CLOSURE_NOTICE", _p(
            r"(?P<rx_dp_name>.+?)\s+has informed NSDL that its demat operations"
            r"\s+will close on\s+(?P<rx_close_date>\d{2}-\d{2}-\d{4})")),
        # MF conversion request submitted
        ("MF_CONVERSION", _p(
            r"Your Mutual Fund Conversion request in Demat account"
            r"\s+\((?P<rx_dp_id>\w+),\s+\*{4}(?P<rx_ac_last4>\d{4})\)"
            r"\s+is submitted successfully to MF-RTA"
            r".*?Reference Number:\s+CRN\s+(?P<rx_crn>\d+)")),
        # MF conversion approved
        ("MF_CONVERSION_APPROVED", _p(
            r"Your Mutual Fund Conversion Request linked to DP ID\s+(?P<rx_dp_id>\S+)"
            r"\s+and Client ID\s+(?P<rx_client_id>\d+)"
            r"\s+has been successfully approved")),
        # MF conversion link
        ("MF_CONVERSION_LINK", _p(
            r"To convert existing mutual funds in your demat account"
            r".*?eservices\.nsdl\.com")),
        # Power of Attorney registered
        ("POA_REGISTERED", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_broker>.+?)"
            r"\s+was registered as Power of Attorney in your demat a/?c no\s+"
            r"(?P<rx_ac_masked>xxxx\d+).*?NSDL")),
        # DDPI registered
        ("DDPI_REGISTERED", _p(
            r"On\s+(?P<rx_date>\d{2}/?\d{2}/?\d{2}),\s*(?P<rx_broker>.+?)"
            r"\s+was registered as Demat Debit and Pledge Instruction"
            r"\s*\(?DDPI\)?\s+in your demat a/?c no\s+"
            r"(?P<rx_ac_masked>xxxx\d+).*?NSDL")),
        # Insta-demat account registered
        ("INSTA_DEMAT_REGISTERED", _p(
            r"Thank you for opening NSDL Insta Demat A/?c with\s+(?P<rx_broker>.+?)"
            r"\s*-\s*DP ID\s+(?P<rx_dp_id>\S+)"
            r"\s*\.?\s*Your Client ID\s+(?P<rx_client_id>\d+)"
            r"\s+is in Registered status")),
        # ISIN request generated
        ("ISIN_REQUEST_GENERATED", _p(
            r"ISIN request with URN\s+(?P<rx_urn>\d+)\s+is generated.*?NSDL")),
        # Redemption certificate from IPA
        ("REDEMPTION_CERTIFICATE", _p(
            r"Redemption certificate for ISIN\s+(?P<rx_isin>\S+)"
            r"\s+sent by IPA\s+(?P<rx_ipa>.+?)\s*\.?\s*NSDL")),
    ],

    # ── CDSL DEPOSITORY SMS ──
    "CDSL": [
        # Bank account number changed in demat
        ("BANK_ACCOUNT_CHANGED", _p(
            r"CDSL:?\s*In your demat a/c\s+\*(?P<rx_ac_no>\d+)"
            r",bank a/c no changed frm\s+(?P<rx_old_bank_ac>\S+)"
            r"\s+To\s+(?P<rx_new_bank_ac>\S+)"
            r"\s+on\s+(?P<rx_date>\d{2}-\d{2}-\d{4})")),
        # Bank name changed in demat
        ("BANK_NAME_CHANGED", _p(
            r"CDSL:?\s*In your demat a/c\s+\*(?P<rx_ac_no>\d+)"
            r",bank name changed frm\s+(?P<rx_old_bank>.+?)"
            r"\s+To\s+(?P<rx_new_bank>.+?)"
            r"\s+on\s+(?P<rx_date>\d{2}-\d{2}-\d{4})")),
        # Short debit (single ISIN)
        ("DEBIT_SHORT", _p(
            r"CDSL:?\s*Debit in a/c\s+\*(?P<rx_ac_no>\d+)\s+for\s+(?P<rx_holdings>.+?)"
            r"\s+on\s+(?P<rx_date>\d{2}[A-Z]{3})")),
        # Bulk debit (multiple ISINs)
        ("DEBIT_BULK", _p(
            r"CDSL:?\s*Debit in a/c\s+\*(?P<rx_ac_no>\d+)\s+for\s+(?P<rx_holdings>.+?),"
            r"\s*Total\s+(?P<rx_isin_count>\d+)\s+ISINs debited"
            r"\s+on\s+(?P<rx_date>\d{2}[A-Z]{3})")),
        # Generic credit
        ("CREDIT_GENERAL_CDSL", _p(
            r"CDSL:?\s*Credit in a/c\s+\*(?P<rx_ac_no>\d+)\s+for\s+(?P<rx_holdings>.+?)"
            r"\s+on\s+(?P<rx_date>\d{2}[A-Z]{3})")),
        # Bulk pledge accepted
        ("PLEDGE_BULK_ACCEPTED", _p(
            r"CDSL:?\s*Pledge Accepted by pledgee a/c\s+\*(?P<rx_pledgee_ac>\d+)"
            r"\s+for\s+(?P<rx_holdings>.+?),\s*Total\s+(?P<rx_isin_count>\d+)"
            r"\s+ISINs Accepted\s+on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})")),
        # Single pledge accepted
        ("PLEDGE_ACCEPTED", _p(
            r"CDSL:?\s*Pledge Accepted by pledgee a/c\s+\*(?P<rx_pledgee_ac>\d+)"
            r"\s+for\s+(?P<rx_qty>\d+)\s+(?:Ordinary Shares\s*-\s*"
            r"|Equity Shares\s*-\s*|shares? of\s+|units? of\s+)?(?P<rx_security>.+?)"
            r"(?:\s+on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})"
            r"|\s+on\s+(?P<rx_date2>\d{2}[A-Z]{3}\d{2,4}))")),
        # Unpledge accepted
        ("UNPLEDGE_ACCEPTED", _p(
            r"CDSL:?\s*Unpledge Accepted by pledgee a/c\s+\*(?P<rx_pledgee_ac>\d+)"
            r"\s+for\s+(?P<rx_holdings>.+?)"
            r"(?:\s+on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})"
            r"|\s+on\s+(?P<rx_date2>\d{2}[A-Z]{3}\d{2,4}))")),
        # Pledge invoked
        ("PLEDGE_INVOKED", _p(
            r"CDSL[\s-]*Pledge invoked by pledgee in a/c\s+\*(?P<rx_pledgee_ac>\d+)"
            r"\s+for\s+ISIN\s+(?P<rx_isin>\S+)\s+and for\s+(?P<rx_qty>\d+)\s+qty")),
        # IPO / FPO credit
        ("CREDIT_IPO_FPO", _p(
            r"CDSL:?\s*Credit in a/c\s+\*(?P<rx_ac_no>\d+)"
            r"\s+through\s+(?P<rx_issue_type>IPO|FPO)"
            r"\s+for\s+(?P<rx_holdings>.+?)\s+on\s+(?P<rx_date>\d{2}[A-Z]{3})")),
        # Stock split
        ("STOCK_SPLIT", _p(
            r"CDSL:?STOCK SPLIT BY\s+(?P<rx_security>.+?)"
            r"\s+(?P<rx_qty_out>\d+)\s+SHARES DEBITED"
            r"\s+AND\s+(?:[\d]*\s+)?SHARES CREDITED\s+A/C\s+\*(?P<rx_ac_no>\d+)"
            r"\s+ON\s+(?P<rx_date>\S+)")),
        # Scheme-of-arrangement / bonus credit
        ("CREDITED_SCHEME", _p(
            r"CDSL:?CREDITED IN A/C\s+\*(?P<rx_ac_no>\d+)"
            r"\s+(?:(?P<rx_qty>\d+)\s+)?(?:SHARES|UNITS|RIGHTS ENTITLEMENT) OF"
            r"\s+(?P<rx_security>.+?)\s+TOWARDS\s+"
            r"(?P<rx_reason>SCHEME OF ARRANGEMENT|BONUS|SPLIT|MERGER|[^O][^\n]+?)"
            r"\s+ON\s+(?P<rx_date>\S+)")),
        # Extinguishment / redemption debit
        ("DEBITED_EXTINGUISHMENT", _p(
            r"CDSL:?DEBITED IN A/C\s+\*(?P<rx_ac_no>\d+)"
            r"\s+(?P<rx_qty>\d+)\s+(?:SHARES|PTCS|TREASURE BILL) OF\s+(?P<rx_security>.+?)"
            r"\s+TOWARDS\s+(?P<rx_reason>EXTINGUISHMENT|REDEMPTION)"
            r"\s+ON\s+(?P<rx_date>\S+)")),
        # Temporary ISIN conversion
        ("TEMP_ISIN_CONVERSION", _p(
            r"CDSL:?(?:DEBIT|CREDIT) IN A/C\s+\*(?P<rx_ac_no>\d+)"
            r"\s+FOR\s+(?P<rx_qty>\d+)\s+(?:SHARES|PTCS).*?TEMPERO?RY ISIN"
            r".*?LISTED ISIN OF\s+(?P<rx_security>.+?)\s+ON\s+(?P<rx_date>\S+)")),
        # Consolidation
        ("CONSOLIDATION", _p(
            r"CDSL:?CONSOLIDATION BY\s+(?P<rx_security>.+?)"
            r"\s+(?P<rx_qty_out>\d+)\s+SHARES DEBITED"
            r"\s+AND\s+(?:[\d]+\s+)?SHARES CREDITED\s+A/C\s+\*(?P<rx_ac_no>\d+)"
            r"\s+ON\s+(?P<rx_date>\S+)")),
        # TPIN for eDIS
        ("TPIN_EDIS", _p(
            r"CDSL[\s-]+TPIN\s+to\s+avail\s+(?:CDSL'?s?\s+)?e-?DIS\s+facility"
            r"(?:\s+of\s+CDSL)?\s+for\s+BO\s*-?\s*ID\s+"
            r"(?P<rx_bo_id>\*+(?P<rx_bo_last4>\d{4}))"
            r"(?:\s+with\s+(?P<rx_dp>.+?)[\s-]+DP)?\s+is\s*[-–\s]*"
            r"(?P<rx_tpin>\d{6})")),
        # TPIN generated (generic)
        ("TPIN_GENERATED", _p(
            r"CDSL[\s-]+(?:Your\s+Generated\s+)?TPIN"
            r"(?:\s+(?:code\s+)?to\s+avail\s+(?:CDSL'?s?\s+)?e-?DIS\s+facility"
            r"(?:\s+of\s+CDSL)?\s+for\s+BO\s*-?\s*ID\s+"
            r"(?P<rx_bo_id>\*+(?P<rx_bo_last4>\d{4})).*?is\s*[-–\s]*)?"
            r"(?P<rx_tpin>\d{6})")),
        # e-Voting with meeting date
        ("VOTING_WITH_MEETING", _p(
            r"Dear Investor,\s*e-Voting for\s+(?P<rx_security>.+?)"
            r"\s+begins from\s+(?P<rx_start_dt>\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2})"
            r"\s+and ends at\s+(?P<rx_end_dt>\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2})"
            r"\s+Meeting on\s+(?P<rx_meeting_dt>\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2})-CDSL")),
        # e-Voting generic
        ("VOTING", _p(
            r"Dear Investor,\s*(?:e-Voting|Your vote is successfully cast) for\s+"
            r"(?P<rx_security>.+?)\s+(?:begins from|[-–])")),
        # Monthly CAS
        ("CAS_STATEMENT", _p(
            r"Dear investor,\s+your Monthly CAS of PAN\s+(?P<rx_pan_masked>\S+)"
            r"\s+for the (?:quarter|month) ending\s+(?P<rx_period>\S+)"
            r"\s+is emailed on\s+(?P<rx_email>\S+)\s*-\s*CDSL")),
        # Half-yearly CAS
        ("HALF_YEARLY_CAS", _p(
            r"Dear investor,\s+your Half Yearly (?:Holding )?CAS of PAN\s+"
            r"(?P<rx_pan_masked>\S+)"
            r"\s+for the half year ending\s+(?P<rx_period>\S+)"
            r"\s+is emailed on\s+(?P<rx_email>\S+)\s*-\s*CDSL")),
        # Nominee update reminder
        ("NOMINEE_UPDATE", _p(
            r"Contact your DP to update the nominee.*?BO ID:\s+(?P<rx_bo_id>\S+)\s*-\s*CDSL")),
        # Portfolio valuation
        ("PORTFOLIO_VALUATION", _p(
            r"CDSL-The valuation of securities in your demat (?:a/?c|ac)"
            r"\s+\*(?P<rx_ac_no>\d+)\s+as on\s+(?P<rx_date>\d{8})"
            r"\s+is Rs\.(?P<rx_value>[\d.,]+)")),
        # Mobile number updated
        ("MOBILE_UPDATED_CDSL", _p(
            r"CDSL:?\s*Your new mobile number has been (?:regd|registered)"
            r".*?(?:a/c|ac)\s+\*(?P<rx_ac_no>\d+)")),
        # Warrants credited
        ("WARRANTS_CREDITED", _p(
            r"CDSL:?CREDITED IN A/C\s+\*(?P<rx_ac_no>\d+)"
            r"\s+WARRANTS OF\s+(?P<rx_security>.+?)\s+ON\s+(?P<rx_date>\S+)")),
        # Account frozen
        ("ACCOUNT_FROZEN", _p(
            r"CDSL-Your Demat Account\s+\*{4}(?P<rx_ac_last4>\d{4})\s+is frozen"
            r"\s+for\s+(?P<rx_freeze_type>[^,]+)")),
        # Signature updated
        ("SIGNATURE_UPDATED", _p(
            r"CDSL:?\s*Signature updated in your demat account\s+\*(?P<rx_ac_no>\d+)"
            r"\s+on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})")),
        # Address changed
        ("ADDRESS_CHANGED", _p(
            r"CDSL:?\s*Change of address has been carried out"
            r"\s+in your demat account\s+\*(?P<rx_ac_no>\d+)"
            r"\s+on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})")),
        # UCC linked to demat
        ("UCC_LINKED", _p(
            r"CDSL-UCC is linked to your demat account ending with\s+"
            r"(?:XXXX)?(?P<rx_ac_last4>\d{4})")),
        # DIS slips issued
        ("DIS_ISSUED", _p(
            r"Delivery Instruction Slips?\s*\[?DIS\]?\s*have been issued"
            r"\s+for your demat account\s+\*(?P<rx_ac_no>\d+)"
            r"\s+on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})")),
        # SMS alerts deregistered
        ("SMS_DEREGISTERED", _p(
            r"CDSL:?\s*SMS alerts for a/c\s+\*(?P<rx_ac_no>\d+)\s+deregistered")),
        # Positive consent confirmation
        ("POSITIVE_CONSENT", _p(
            r"CDSL-Based on your earlier positive consent"
            r".*?CDSL demat A/c no\.\*(?P<rx_ac_no>\d+)")),
        # Demat account opened (CDSL)
        ("DEMAT_ACCOUNT_OPENED_CDSL", _p(
            r"(?:Greetings from CDSL"
            r"|Thank you for opening demat account with us)")),
    ],

    # ── NSE EXCHANGE SMS ──
    "NSE": [
        # Broker fund balance reported to NSE
        ("BROKER_FUND_BALANCE", _p(
            r"(?P<rx_broker>.+?)\s+on\s+(?P<rx_date>\d{2}-\d{2}-\d{2,4})"
            r"\s+reported your Fund bal Rs\.?\s*(?P<rx_fund_bal>-?[\d.,]+)"
            r".*?Securities bal\s*(?P<rx_sec_bal>-?[\d.,]+)"
            r".*?(?:-NSE|National Stock Exchange)")),
        # Traded value summary
        ("TRADED_VALUE", _p(
            r"Dear\s+(?P<rx_pan_masked>[A-Z0-9]+),Your traded value for\s+"
            r"(?P<rx_date>\d{2}-[A-Z]{3}-\d{2})"
            r"\s+(?P<rx_segment>CM|FO|CD|All Segment|MCX Segment)?"
            r".*?Rs\s*(?P<rx_value>[\d.,]+).*?(?:National Stock Exchange|NSE)")),
        # UCC registration with broker
        ("UCC_REGISTRATION", _p(
            r"(?P<rx_pan_masked>[A-Z0-9]+)\s+registered in\s+"
            r"(?P<rx_segment>Cash Equity|Commodity Derivatives|Futures & Options)"
            r"\s+segment with broker\s+(?P<rx_broker>.+?)"
            r"\s+with Unique Client Code\s*(?:\(UCC\))?(?P<rx_ucc>[A-Z0-9]+)"
            r".*?NSE will start sending")),
        # Investor advisory (tips warning)
        ("INVESTOR_ADVISORY_NSE", _p(
            r"Beware while dealing based on unsolicited tips"
            r".*?(?:-NSE|National Stock Exchange)")),
        # NSE Invest lump-sum authorisation
        ("NSE_INVEST_AUTHORIZE", _p(
            r"(?:Thank you for investing on|Greetings from)\s*NSE\s*INVEST"
            r".*?(?:authorize|click)\s+(?:the\s+)?(?:link|transaction)"
            r".*?(?P<rx_ref>https?://\S+)")),
        # NSE Invest SIP cancelled
        ("NSE_INVEST_SIP_CANCELLED", _p(
            r"your SIP Reg no\s+(?P<rx_sip_id>\d+)\s+registered on NSE INVEST"
            r"\s+stands\s+(?P<rx_status>Cancelled|cancelled)")),
        # SIP authorisation link
        ("SIP_AUTHORIZE", _p(
            r"Thank you for investing on NSE Invest.*?(?:authorize|click).+?"
            r"(?:XSIP reg no|link)\s+(?P<rx_ref>\S+)")),
        # MF Invest order submitted
        ("NSE_MF_INVEST_SUBMITTED", _p(
            r"Dear investor,\s*Greetings from NSE\.\s*(?P<rx_order_id>\d+)"
            r"\s+has been submitted on NSE MF INVEST")),
        # NSE UCC welcome
        ("NSE_UCC_WELCOME", _p(
            r"Dear [Ii]nvestor,\s+Welcome to NSE\.\s+Your Unique Client Code"
            r"\s*\(?UCC\)?\s+is\s+(?P<rx_ucc>[A-Za-z0-9.eE+]+)\.")),
        # NSE defaulter notice
        ("NSE_DEFAULTER_NOTICE", _p(
            r"(?P<rx_company>.+?)\s+has been declared defaulter.*?NSE")),
    ],

    # ── BSE EXCHANGE SMS ──
    "BSE": [
        # Trade confirmation
        ("TRADE_CONFIRMATION", _p(
            r"BSE Trade Confirmation Client Code\s+(?P<rx_client_code>[A-Z0-9]+)"
            r".*?-?\s*Broker\s+(?P<rx_broker>\d+)"
            r".*?EQ\s+Value Rs\s+(?P<rx_eq_value>[\d.\-]+)"
            r".*?FNO Value Rs\s+(?P<rx_fno_value>[\d.\-]+)"
            r".*?Currency Value Rs\.?\s+(?P<rx_currency_value>[\d.\-]+)"
            r".*?Dated\s+(?P<rx_date>\d{2}-\d{2}-\d{4})")),
        # EOD fund balance (BSE)
        ("FUND_BALANCE_EOD", _p(
            r"(?P<rx_broker>.+?)\s+at EOD\s+"
            r"(?P<rx_date>\d{2}/\d{2}/\d{4}|\d{8})"
            r"\s+reported your Fund bal Rs\s*(?P<rx_fund_bal>-?[\d.,]+)"
            r"\s*(?:&|and)?\s*Securities bal\s*(?P<rx_sec_bal>-?[\d.,]+).*?-BSE")),
        # Unsolicited tips warning
        ("UNSOLICITED_TIPS", _p(
            r"Beware dealing on unsolicited tips through.*?(?:bseindia|BSE)")),
        # IPO application submitted via BSE
        ("APPLICATION_SUBMITTED", _p(
            r"DEAR INVESTOR,APPLICATION NO\.(?P<rx_application_no>[A-Z0-9]+)"
            r"\s+OF\s+(?P<rx_company>[A-Z0-9 .&\\'-]+?)\.?"
            r"\s+HAS BEEN SUBMITTED ON BSE")),
        # No allotment
        ("NO_ALLOTMENT", _p(
            r"DEAR INVESTOR,\s*No allotment to Application No\."
            r"(?P<rx_application_no>[A-Z0-9]+)"
            r"\s+FOR\s+(?P<rx_company>[A-Z0-9 .&\\'-]+?)\.")),
        # Successful allotment
        ("SUCCESSFUL_ALLOTMENT", _p(
            r"DEAR INVESTOR,SUCCESSFUL ALLOTMENT OF\s+(?P<rx_qty>\d+)\s+SHARES?"
            r"\s+AT Rs\.(?P<rx_price>[\d.]+)"
            r"\s+TO APPLICATION NO\.(?P<rx_application_no>[A-Z0-9]+)"
            r"\s+FOR\s+(?P<rx_company>[A-Z0-9 .&\\'-]+)")),
        # Issue bidding open
        ("ISSUE_BIDDING_OPEN", _p(
            r"The issue\s*:\s*(?P<rx_issue_name>.+?)\s+is initiated by\s+(?P<rx_issuer>.+?)"
            r"\s+is open for bidding from\s+(?P<rx_start_time>[\d:]+)"
            r"\s+(?P<rx_end_time>[\d:]+),"
            r"\s*(?P<rx_date>\d{2}\s+[A-Za-z]+\s+\d{4})")),
    ],

    # ── ANGEL ONE BROKER SMS — change when required (add new patterns) ──
    "ANGELONE": [
        ("MARGIN_UTILIZATION_WARNING", _p(
            r"Dear (?:Customer|Client)\s*(?P<rx_client>[^,]+),"
            r"your account has reached\s*(?P<rx_util>[\d.]+)%"
            r".*?as on\s*(?P<rx_asof>\d{4}-\d{2}-\d{2}.*?)(?:\.|$)")),
        ("MTM_LOSS_WARNING", _p(
            r"Dear\s*(?P<rx_client>.+?)Your MTM loss has reached\s*"
            r"(?P<rx_losspct>[\d.]+)%")),
        ("TOTAL_MARGIN_SHORTFALL", _p(
            r"(?:Dear\s+)?(?:Customer|Client)\s*\((?P<rx_client>[^)]+)\)"
            r".*?Total Margin shortfall.*?is Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r".*?(?:Angel One|Regards)")),
        ("TRADING_ACCOUNT_SHORTAGE", _p(
            r"Dear Client\s*(?P<rx_client>[^,]+),\s*there is a shortage of"
            r" Rs\.?\s*(?P<rx_amount>[\d.]+)"
            r".*?(?:your\s+)?shares\s+being\s+sold.*?(?:Angel One|Regards)")),
        ("PAY_TO_AVOID_SHARE_SALE", _p(
            r"Dear Client\s*(?P<rx_client>[^,]+),\s*Please pay"
            r" Rs\.?\s*(?P<rx_amount>[\d.]+)"
            r".*?(?:your\s+)?shares\s+being\s+sold.*?(?:Angel One|Regards)")),
        ("SHARES_WILL_BE_SOLD_TODAY", _p(
            r"(?:Dear\s+(?:Client|Customer)\s+(?P<rx_client>\S+).*?)?"
            r"shares\s+worth\s+(?:Rs\.?\s*)?(?P<rx_amount>[\d.,]+)"
            r"\s+will be sold today.*?(?:Angel One|Regards)")),
        ("SHARES_SOLD_SHORTAGE", _p(
            r"Dear\s+(?:Customer|Client)\s+(?P<rx_client>\S+)"
            r".*?shortage of Rs\.?\s*(?P<rx_shortfall>[\d.,]+)"
            r".*?shares\s+worth\s+(?:Rs\.?\s*)?(?P<rx_amount>[\d.,]+)"
            r"\s+will be sold today.*?(?:Angel One|Regards)")),
        ("AUTO_PLEDGE_OUTSTANDING", _p(
            r"Your purchased shares on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})"
            r"\s+are auto-pledged in your Demat a/?c due to outstanding balance"
            r".*?(?:Angel One|Regards)")),
        ("MARGIN_INCREASE_HAIRCUT", _p(
            r"Your margin has been increased successfully by"
            r" Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r"\s+after haircut.*?(?:Angel One|Regards)")),
        ("MTF_MARGIN_SHORTAGE", _p(
            r"(?:in your\s+)?Angel One (?:A/C|a/c)\s+(?P<rx_client>\S+)"
            r".*?MTF margin shortage of Rs\.?\s*(?P<rx_shortfall>[\d.,]+)")),
        ("DEMAT_ACCOUNT_DEBIT_WARNING", _p(
            r"Dear Customer your a/c\s+(?P<rx_client>\S+)\s+is in debit"
            r".*?Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r"\s+as on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})"
            r".*?(?:Angel One|Regards)")),
        ("FO_PHYSICAL_DELIVERY_WARNING", _p(
            r"(?:your position|you have open).*?F&O"
            r".*?(?:compulsory|physical delivery)"
            r".*?(?:delivery margin|square off|Team Angel One)")),
        ("FO_PHYSICAL_DELIVERY_V2", _p(
            r"Dear Client\s+(?P<rx_client>\S+),\s*your\s+\d+[A-Z]{3}\d{4}"
            r"\s+stock F&O positions"
            r".*?physical delivery.*?(?:Angel One|Regards)")),
        ("OPTIONS_EXPIRY_ALERT", _p(
            r"(?:NSECOM|NSE)\s*Alert for\s+(?P<rx_client>\S+):"
            r"\s*Your options expiring.*?(?:Angel One|Regards)")),
        ("POSITION_MARGIN_ALERT", _p(
            r"Dear Client\s+(?P<rx_client>\S+),\s*Your position in"
            r".*?additional margin of\s*(?P<rx_amount>[\d.]+)"
            r".*?(?:Angel One|Regards)")),
        ("FO_MARGIN_PENALTY", _p(
            r"F&O margin shortage of Rs\.?\s*(?P<rx_shortfall>[\d.,]+)"
            r".*?penalty of Rs\.?\s*(?P<rx_penalty>[\d.,]+)")),
        ("PAYOUT_PROCESSED", _p(
            r"(?:Dear Client\s*(?P<rx_client>\S+)\s*[-,]\s*)?"
            r"(?:Funds withdrawal|amount|payout) of\s*Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r"\s+(?:has been\s+)?(?:processed|transferred|paid)\s+successfully"
            r".*?(?:UTR\s*(?:Number|No\.?)\s*(?P<rx_utr>\S+))?"
            r".*?(?:Angel One|Regards)")),
        ("FUNDS_PAYOUT_REQUEST", _p(
            r"Rs\.(?P<rx_amount>[\d.,]+)\s+of your Funds Payout Request"
            r" will be credited to"
            r".*?Bank\s*A/C\s*no\.(?P<rx_ac_masked>[xX\d]+).*?Angel One")),
        ("MANDATORY_SEBI_PAYOUT", _p(
            r"(?:Dear\s+)?(?P<rx_client>\S+),?\s*[Tt]he mandatory SEBI payout of"
            r"\s*Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r"\s+has been successfully processed"
            r".*?(?:UTR\s*No\.?\s*(?P<rx_utr>\S+))?"
            r".*?(?:Angel One|Team Angel One)")),
        ("MANDATORY_SEBI_PAYOUT_GENERIC", _p(
            r"mandatory SEBI payout of funds will be processed"
            r".*?(?:adjusting|margins).*?Angel One")),
        ("WITHDRAWAL_REJECTED", _p(
            r"Dear Client\s+(?P<rx_client>\S+)"
            r".*?Withdrawal request for Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r"\s+has been rejected.*?(?:Angel One|Regards)")),
        ("MONTHLY_SETTLEMENT", _p(
            r"Monthly Settlement of Rs\s+(?P<rx_amount>[\d.,]+)\s+paid on")),
        ("SIP_SETUP_COMPLETED", _p(
            r"SIP set-up completed for\s*(?P<rx_symbol>[A-Z0-9]+)")),
        ("SIP_DUE_REMINDER", _p(
            r"SIP for\s+(?P<rx_symbol>\S+)\s+is due tomorrow"
            r".*?Maintain Rs\.\s*(?P<rx_amount>[\d.,]+)")),
        ("SIP_UPCOMING_DEBIT", _p(
            r"Upcoming Payment[!,\s]+(?:Dear\s+[^,]+,\s*)?"
            r"Rs\s*(?P<rx_amount>[\d.,]+)\s+will be debited\s+on\s+"
            r"(?P<rx_date>\d{2}\s+[A-Za-z]+\s+\d{4})"
            r".*?upcoming SIP\s*#?\s*(?P<rx_sip_id>\d+)"
            r"\s+in\s+(?P<rx_fund>.+?)\.\s+Ensure")),
        ("SIP_AUTOPAY_PAUSED", _p(
            r"mutual fund SIP is due.*?linked with a paused Autopay"
            r".*?Angel One")),
        ("MF_SIP_MISSED_TXN", _p(
            r"(?:Your MFD|requested a transaction for)\s+(?:missed\s+)?SIP of\s+"
            r"(?:₹|Rs\.?\s*)(?P<rx_amount>[\d.,]+)"
            r"\s+in\s+(?P<rx_fund>.+?)\."
            r".*?(?:Angel One|Regards)")),
        ("BANK_UPDATE_REQUEST", _p(
            r"(?:Dear\s+(?P<rx_name>[^,]+),\s*)?"
            r"Your Bank (?:updation|update) request\s+"
            r"(?P<rx_ref>[A-Z0-9]+)\s+has been successfully received"
            r".*?(?:Angel One|Regards)")),
        ("BANK_MODIFICATION_APPROVED", _p(
            r"(?:Dear\s+)?(?P<rx_name>\S+)\s*[-–]\s*"
            r"Your bank modification request\s+"
            r"(?P<rx_ref>[A-Z0-9]+)\s+is approved"
            r".*?(?:Angel One|Regards)")),
        ("CONTACT_DETAILS_MODIFICATION", _p(
            r"(?:Dear\s+)?(?P<rx_name>\S+),?\s*"
            r"Your contact details modification request\s+"
            r"(?P<rx_ref>[A-Z0-9]+)\s+has been successfully received"
            r".*?(?:Angel One|Regards)")),
        ("INCOME_UPDATION_REQUEST", _p(
            r"Your Income (?:updation|update) request\s+"
            r"(?P<rx_ref>[A-Z0-9]+)"
            r"\s+has been successfully received.*?(?:Angel One|Regards)")),
        ("NOMINEE_ADDED", _p(
            r"The following persons? (?:are|is) added as nominees?"
            r" for your account.*?(?:Angel One|Regards)")),
        ("NOMINEE_CHOSEN_LATER", _p(
            r"You have chosen to add nominees for your account later"
            r".*?(?:Angel One|Regards)")),
        ("NOMINEE_PREFERENCE_UPDATED", _p(
            r"Your nominee preference is updated as per your request\s+"
            r"(?P<rx_ref>[A-Z0-9]+).*?(?:Angel One|Regards)")),
        ("SEGMENT_ACTIVATION_REQUEST", _p(
            r"Your segment activation request\s+(?P<rx_ref>[A-Z0-9]+)"
            r"\s+is successfully placed.*?(?:Angel One|Regards)")),
        ("DDPI_PROCESSING", _p(
            r"Your DDPI request\s+(?P<rx_ref>[A-Z0-9]+)"
            r"\s+is being processed.*?(?:Angel One)")),
        ("DOCUMENT_REVIEW", _p(
            r"Our team at Angel One is reviewing your documents"
            r".*?(?:Angel One|Regards)")),
        ("ESIGN_PENDING", _p(
            r"your Angel One e-Sign is pending")),
        ("IPV_PENDING", _p(
            r"your Angel One IPV is pending")),
        ("CONTRACT_NOTE_UNDELIVERED", _p(
            r"Your contract note emails are undelivered"
            r".*?(?:Angel One|Regards)")),
        ("IPO_APPLICATION_AP", _p(
            r"Your AP\s+(?P<rx_ap>.+?)\s+has initiated an IPO application for\s+"
            r"(?P<rx_company>.+?)"
            r"\s+to buy\s+(?P<rx_lots>\d+)\s+lots?\s+at\s+(?:price\s+)?"
            r"Rs\.?\s*(?P<rx_price>[\d.]+)"
            r".*?(?:Angel One|NXT Angel One)")),
        ("IPO_ALLOTMENT_CONGRATS", _p(
            r"you have been allotted\s+(?P<rx_qty>\d+)\s+shares of\s+"
            r"(?P<rx_company>.+?)"
            r"\s+\(App ID:\s*(?P<rx_app_id>\d+)\).*?(?:Angel One|Regards)")),
        ("IPO_ALLOTMENT_STATUS", _p(
            r"(?:you have not been allotted|allotment).*?shares of\s+"
            r"(?P<rx_company>.+?)"
            r"\s+\(App ID:\s*(?P<rx_app_id>\d+)\).*?Angel One")),
        ("BUYBACK_BID_ACCEPTED", _p(
            r"Dear Client\s+(?P<rx_client>\S+),\s*Your Buyback Bid for\s+"
            r"(?P<rx_qty>\d+)"
            r"\s+shares of\s+(?P<rx_company>\S+)\s+has been accepted"
            r".*?(?:Angel One|Regards)")),
        ("BUYBACK_PLACE_REQUEST", _p(
            r"request has been received to place a buyback order"
            r".*?(?:Angel One|NXT Angel One)")),
        ("CLIENT_TRANSFER_REQUEST", _p(
            r"(?:Dear Client\s*\(?(?:client code\s*[-–]\s*)?(?P<rx_client>\S+)\)?"
            r"|Client Transfer request initiated for the client code"
            r"\s*[-–]\s*(?P<rx_client2>\S+))"
            r".*?transfer.*?(?:Angel One|Regards)")),
        ("SHORT_DELIVERY_WARNING", _p(
            r"out of the\s+(?P<rx_security>\S+)"
            r".*?(?P<rx_missing>\d+)\s+shares have not been received"
            r".*?(?:Angel One|Regards)")),
        ("REFERRAL_THANK_YOU", _p(
            r"Thank you for referring.*?(?:AngelOne|Angel One)")),
        ("USERID_CREATED", _p(
            r"Your user ID is\s*(?P<rx_uid>[A-Z0-9]+)")),
        ("DEMAT_APPLICATION_INCOMPLETE", _p(
            r"Your application to open a demat account with AngelOne"
            r" is almost complete")),
        ("ACCOUNT_DORMANCY_WARNING", _p(
            r"your Angel One account will become dormant on\s+"
            r"(?P<rx_date>\S+)")),
        ("ENABLE_NOTIFICATIONS", _p(
            r"enable Angel One app notifications to receive timely updates")),
        # Catch-all promotional patterns — change when required
        ("ANGEL_PROMO", _p(
            r"(?:Angel One|AngelOne).*?(?:demat account|top gainers"
            r"|IPO update|Gold prices"
            r"|Silver shines|ETFs|Mutual Funds|MTF interest"
            r"|investing smartly|State elections"
            r"|app notifications|Check your Angel One balance"
            r"|Check out the News section"
            r"|Rs\.\s*100 is enough|research.based stock"
            r"|Resume your onboarding"
            r"|trending stock picks|Complete your KYC"
            r"|smart trading|Advisory se milega"
            r"|India's auto market|Big IPO update"
            r"|We're sad to see you go"
            r"|Get 0% MTF interest|Explore here|Click here)")),
    ],

    # ── GROWW BROKER SMS ──
    "GROWW": [
        ("QUARTERLY_SETTLEMENT_GROWW", _p(
            r"Quarterly settlement:\s+Transfer successful from Groww"
            r"\s+Amount:\s+Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r"(?:.*?UTR\s+no\.?:\s*(?P<rx_utr>\S+))?"
            r"(?:.*?Bank:\s+(?P<rx_bank>.+?)"
            r"\s+\((?P<rx_ac_masked>[xX\d*]{4,})\))?")),
        ("GROWW_UPI_REGISTRATION", _p(
            r"we got a request for registering your account on Groww UPI")),
        ("MARGIN_SHORTFALL", _p(
            r"(?:GROWW|Groww):?\s*Margin shortfall of"
            r" Rs\.?\s*(?P<rx_amount>[\d.,]+)"
            r".*?(?:auto square-off|squared off)")),
        ("UPI_CREDIT_RECEIVED", _p(
            r"(?P<rx_amount>[\d.]+).*?was credited to\s*(?P<rx_bank>.+?)"
            r"\s*A/C\s*(?P<rx_ac_masked>[A-Z0-9xX]+)"
            r".*?linked to VPA\s*(?P<rx_vpa>\S+).*?Groww")),
        ("SIP_PAYMENT_UNSUCCESSFUL", _p(
            r"SIP payment for\s*(?P<rx_fund>.+?)\s+for Rs\s*(?P<rx_amount>[\d.]+)"
            r".*?unsuccessful")),
        ("NEGATIVE_BALANCE_WARNING", _p(
            r"Your balance has been negative"
            r".*?(?:pledged share|liquidation|add funds)"
            r".*?(?:Groww|groww)")),
        ("PAYOUT_PROCESSED_GROWW", _p(
            r"(?:Rs\.?\s*(?P<rx_amount>[\d.,]+))"
            r".*?(?:withdrawal|payout|transferred|credited)"
            r".*?(?:bank|A/C).*?(?P<rx_ac_masked>[xX\d*]{4,})"
            r".*?(?:Groww|GROWW)")),
        ("CONTRACT_NOTE_GROWW", _p(
            r"(?:Here's|Here is) your contract note for"
            r".*?groww\.in.*?-\s*Groww")),
        ("INTRADAY_ALERT", _p(
            r"\[?Intraday alert\]?\.*\s*Exit all open intraday positions"
            r" on GROWW")),
        ("BROKER_FUND_BALANCE_GROWW", _p(
            r"(?P<rx_broker>GROWWINVESTTECHPRIVATELIMITED|GROWW)"
            r".+?on\s+(?P<rx_date>\d{2}-\d{2}-\d{4})"
            r"\s+reported your Fund bal Rs\.?\s*(?P<rx_fund_bal>-?[\d.,]+)"
            r".*?Securities bal\s*(?P<rx_sec_bal>-?[\d.,]+).*?-NSE")),
        ("WELCOME_ACCOUNT_SETUP", _p(r"Welcome to GROWW")),
        ("WEBINAR_INVITE", _p(r"You are invited to attend.*?GROWW")),
        ("GROWW_CREDIT_EMI", _p(
            r"(?:Your EMI of|An agent from).*?(?:Groww Credit|Groww)")),
    ],

    # ── MOTILAL OSWAL (MOFSL) BROKER SMS ──
    "MOFSL": [
        # Standard TRD summary (CM + NSE FO)
        ("MOFSL_TRD", _p(
            r"MOFSL\s*:?\s*TRD for\s*(?P<rx_ucc>[A-Za-z0-9]+)"
            r"\s+DT\s+(?P<rx_date>\d{2}-[A-Za-z]{3}-\d{4}"
            r"|[A-Za-z]{3}\s+\d{1,2}\s+\d{3,4})"
            r"\s+in\s+CM=(?P<rx_cm>[-\d.,]+),NSE FO=(?P<rx_nse_fo>[-\d.,]+)"
            r",LED BAL IS\s*(?P<rx_led_bal>[-\d.,]+).*?Motilal Oswal")),
        # TRD for specific segment (NSECOM / NSECD / BSECOM etc.)
        ("MOFSL_TRD_SEGMENT", _p(
            r"MOFSL\s*:?\s*TRD for\s*(?P<rx_ucc>[A-Za-z0-9]+)"
            r"\s+DT\s+(?P<rx_date>\d{2}-[A-Za-z]{3}-\d{4}"
            r"|[A-Za-z]{3}\s+\d{1,2}\s+\d{3,4})"
            r"\s+in\s+(?P<rx_segment>NSECOM|NSECD|BSECOM|BSEFO|MCX|NCDEX|SLBM)"
            r"\s+Segment"
            r",LED BAL IS\s*(?P<rx_led_bal>[-\d.,]+).*?Motilal Oswal")),
        # TRD for MCX / NCDEX / SLBM
        ("MOFSL_TRD_MCX", _p(
            r"MOFSL\s*:?\s*TRD for\s*(?P<rx_ucc>[A-Za-z0-9]+)"
            r"\s+DT\s+(?P<rx_date>\d{2}-[A-Za-z]{3}-\d{4}"
            r"|[A-Za-z]{3}\s+\d{1,2}\s+\d{3,4})"
            r"\s+in\s+(?:MCX Segment|NCDEX Segment|SLBM Segment)"
            r",LED BAL IS\s*(?P<rx_led_bal>[-\d.,]+).*?Motilal Oswal")),
        # TRD with link (All Segment)
        ("MOFSL_TRD_LINK", _p(
            r"MOFSL\s*:?\s*TRD for\s*(?P<rx_ucc>[A-Za-z0-9]+)"
            r"\s+DT\s+(?P<rx_date>[A-Za-z]{3,9}\s+\d{1,2}\s+\d{3,4}"
            r"|\d{2}-[A-Za-z]{3}-\d{4})"
            r"\s+in\s+All Segment.*?Motilal Oswal")),
        # High-value single-trade alert
        ("MOFSL_HIGH_VALUE_TXN", _p(
            r"MOFSL:?\s*Dear Customer,\s*in your trading a/c\s+"
            r"(?P<rx_ucc>[A-Za-z0-9]+)"
            r".*?high value transaction.*?(?:NSE|BSE|MCX|FNO)"
            r"\s*\((?P<rx_security>\S+)\s+(?P<rx_side>SOLD|BOUGHT)"
            r"\s+(?P<rx_qty>\d+)"
            r"\s*@\s*(?P<rx_price>[\d.]+)\)")),
        # Derivative P&L summary link
        ("MOFSL_DERIVATIVE_PNL", _p(
            r"(?:Dear\s+)?(?P<rx_ucc>[A-Za-z0-9]+)\s*:?\s*"
            r"Your Derivative trx & PnL"
            r".*?Team Motilal Oswal")),
        # Trade confirmation call prompt
        ("MOFSL_TRADE_CONFIRM_CALL", _p(
            r"Dear\s+(?P<rx_ucc>[A-Za-z0-9]+),\s*To confirm the trade"
            r".*?Team MOFSL")),
        # NSE fund balance reported by MOFSL
        ("FUND_BALANCE", _p(
            r"(?P<rx_broker>MOTILALOSWALFINANCIALSERVICESL|MOTILAL OSWAL)"
            r".*?reported your Fund bal Rs\.?\s*(?P<rx_fund_bal>[-\d.,]+)"
            r".*?Securities bal\s*(?P<rx_sec_bal>[-\d.,]+).*?-NSE")),
        # BSE fund balance reported by MOFSL
        ("FUND_BALANCE_BSE", _p(
            r"(?P<rx_broker>MOTILAL OSWAL).*?at EOD\s+(?P<rx_date>[\d/]+)"
            r"\s+reported your Fund bal Rs\s*(?P<rx_fund_bal>[-\d.,]+)"
            r"\s*(?:&|and)?\s*Securities bal\s*(?P<rx_sec_bal>[-\d.,]+)"
            r".*?-BSE")),
        # Payout from MOFSL
        ("PAYOUT_PROCESSED_MOFSL", _p(
            r"(?:Rs\.?\s*(?P<rx_amount>[\d.,]+))"
            r".*?(?:payout|withdrawal|transferred|credited)"
            r".*?(?:bank|A/C).*?(?P<rx_ac_masked>[xX\d*]{4,})"
            r".*?(?:Motilal|MOFSL|MOSL)")),
        # Trade confirmation call alert
        ("CALL_ALERT", _p(
            r"Dear Client.*?trade confirmation calls to monitor"
            r" and verify trades.*?Motilal Oswal")),
    ],

    # ── ZERODHA BROKER SMS ──
    "ZERODHA": [
        # Margin utilisation warning (handles "has reached X%" format)
        ("MARGIN_UTILISATION", _p(
            r"(?:EQ\s+)?margin utilisation\s+"
            r"(?:for\s+(?P<rx_account>[A-Z0-9]+)\s+has reached"
            r"|of)\s*(?P<rx_util>[\d.]+)%"
            r".*?(?:-\s*Zerodha|-\s*ZERODHA)")),
        ("DEBIT_BALANCE_MTF", _p(
            r"Your account has a debit balance of"
            r" Rs\.?\s*(?P<rx_amount>-?[\d.,]+)"
            r".*?(?:insufficient margin|MTF position).*?-\s*Zerodha")),
        ("SHORTFALL_WARNING", _p(
            r"Clear your shortfall.*?avoid"
            r".*?(?:confiscation|liquidation|square).*?-\s*Zerodha")),
        ("BUYBACK_ORDER_PLACED", _p(
            r"Your buyback order for\s+(?P<rx_company>.+?)"
            r"\s+has been placed.*?-\s*Zerodha")),
        ("ACCOUNT_LOCKED", _p(
            r"Your account\s+\((?P<rx_account>[A-Z0-9]+)\)"
            r"\s+is locked.*?-\s*Zerodha")),
        ("ORDER_EXECUTED", _p(
            r"your order no\s+(?P<rx_order>\d+)\s+to\s+"
            r"(?P<rx_side>SELL|BUY)\s+(?P<rx_qty>\d+)"
            r"\s+qty of\s+(?P<rx_security>\S+)"
            r".*?(?:completely|partially)\s+traded"
            r"\s*@\s*Rs\.?(?P<rx_price>[\d.]+).*?-\s*ZERODHA")),
        ("GIFT_STOCKS", _p(
            r"(?P<rx_gifter>.+?)\s+has gifted you some stock"
            r".*?-\s*ZERODHA")),
        ("SEBI_TRANSFER", _p(
            r"We(?:'|)ve transferred unused funds"
            r".*?bank account\s+as per SEBI")),
        ("PHYSICAL_DELIVERY_WARNING", _p(
            r"you have open F&O positions with compulsory physical delivery"
            r".*?-Zerodha")),
        ("BROKER_FUND_BALANCE_ZERODHA", _p(
            r"ZERODHABROKINGLIMITED\s+on\s+(?P<rx_date>\d{2}-\d{2}-\d{4})"
            r"\s+reported your Fund bal Rs\.?\s*(?P<rx_fund_bal>-?[\d.,]+)"
            r".*?Securities bal\s*(?P<rx_sec_bal>-?[\d.,]+).*?-NSE")),
        ("PAYOUT_PROCESSED_ZERODHA", _p(
            r"(?:Rs\.?\s*(?P<rx_amount>[\d.,]+))"
            r".*?(?:payout|withdrawal|transferred|settlement)"
            r".*?(?:bank|A/C).*?(?P<rx_ac_masked>[xX\d*]{4,})"
            r".*?(?:Zerodha|ZERODHA)")),
        ("QUARTERLY_SETTLEMENT_ZERODHA", _p(
            r"(?:quarterly|running account)\s+settlement"
            r".*?(?:Rs\.?\s*(?P<rx_amount>[\d.,]+))"
            r".*?(?:Zerodha|ZERODHA)")),
    ],

    # ── MCX EXCHANGE SMS ──
    "MCX": [
        ("MCX_PRICE_ALERT", _p(
            r"Dear Registered user"
            r".*?(?:GOLD|SILVER|COPPER|CRUDE|NATURAL\s*GAS|MENTHA|CARDAMOM)"
            r".*?as on\s+(?P<rx_date>\d{2}[A-Za-z]{3}\d{2})\s+MCX\s+Ltd")),
        ("MCX_OBLIGATION", _p(
            r"Obligation Date\s+(?P<rx_date>\d{2}-\d{2}-\d{4})"
            r".*?MTM Payin.*?MCXCNS")),
        ("MCX_TRADE_EXECUTED", _p(
            r"Your trades executed on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})"
            r"\s+(?P<rx_buy_qty>\d+)\s+buy Rs\s+(?P<rx_buy_val>[\d.,]+)"
            r"\s+(?P<rx_sell_qty>\d+)\s+sell Rs\s+(?P<rx_sell_val>[\d.,]+)"
            r"\s+CLCode\s+(?P<rx_clcode>\S+)"
            r"\s+Mem\.code\s+(?P<rx_memcode>\d+)")),
        ("MCX_CLIENT_FUNDS", _p(
            r"Client Funds on\s+(?P<rx_date>\d{2}/\d{2}/\d{4})\.MCX"
            r"\s+(?P<rx_mcx_bal>-?[\d.,]+)\.?\s*Net Exchanges"
            r"\s+(?P<rx_net>-?[\d.,]+)\.?\s*Clear Exchanges"
            r"\s+(?P<rx_clear>-?[\d.,]+)\.?MCX M-ID"
            r"\s+(?P<rx_mid>\d+)-(?P<rx_broker>.+?)"
            r"\.UCC\s+(?P<rx_ucc>\S+)")),
        ("MCX_UCC_REGISTRATION", _p(
            r"(?P<rx_pan_masked>[A-Z0-9]+)\s+registered in"
            r" Commodity Derivatives"
            r"\s+[Ss]egment with broker\s+(?P<rx_broker>.+?)"
            r"\s+with Unique Client Code\s*(?:\(UCC\)\s*)?"
            r"(?P<rx_ucc>[A-Z0-9]+)")),
    ],

    # ── UPSTOX BROKER SMS ──
    "UPSTOX": [
        ("BROKER_FUND_BALANCE_UPSTOX", _p(
            r"(?P<rx_broker>UPSTOX(?:\s+SECURITIES[^)]*)?)"
            r".*?(?:at EOD\s+)?(?P<rx_date>\d{2}/\d{2}/\d{4})"
            r"\s+reported your Fund bal Rs\s*(?P<rx_fund_bal>-?[\d.,]+)"
            r".*?Securities bal\s*(?P<rx_sec_bal>-?[\d.,]+).*?-BSE")),
        ("PAYOUT_PROCESSED_UPSTOX", _p(
            r"(?:Rs\.?\s*(?P<rx_amount>[\d.,]+))"
            r".*?(?:payout|withdrawal|transferred|credited)"
            r".*?(?:bank|A/C).*?(?P<rx_ac_masked>[xX\d*]{4,})"
            r".*?(?:Upstox|UPSTOX)")),
    ],

    # ── DHAN BROKER SMS ──
    "DHAN": [
        ("DHAN_AUTH_CODE", _p(
            r"(?P<rx_code>\d{4})\s+is your temporary support code"
            r" for Dhan Client Authentication")),
        ("DHAN_ACCOUNT_BLOCKED", _p(
            r"your Dhan account has been blocked for trading")),
        ("DHAN_RUNNING_SETTLEMENT", _p(
            r"We have transferred Rs\s*(?P<rx_amount>[\d.,]+)"
            r"\s+to your bank account"
            r".*?running account settlement.*?Dhan")),
        ("PAYOUT_DHAN", _p(
            r"(?:Rs\.?\s*(?P<rx_amount>[\d.,]+))"
            r".*?(?:payout|withdrawal|transferred|settlement)"
            r".*?(?:bank|A/C).*?(?P<rx_ac_masked>[xX\d*]{4,})"
            r".*?(?:Dhan|DHAN)")),
    ],

    # ── GENERIC CATCH-ALL ──
    "GENERIC": [
        ("PAYOUT_GENERIC", _p(
            r"(?:payout|withdrawal|funds?\s+transfer(?:red)?|settlement)"
            r"\s*(?:of\s*)?"
            r"(?:Rs\.?\s*)?(?P<rx_amount>[\d.,]+)"
            r".*?(?:bank\s*(?:a/c|account)|A/C|credited to)\s*(?:no\.?\s*)?"
            r"(?:ending\s*(?:with\s*)?)?(?P<rx_ac_masked>[xX\d*]{4,})")),
    ],
}

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 6 — LABEL ALIASES (canonicalisation map)                         ║
# ║                                                                            ║
# ║  Maps the UPPER_NO_PUNCT form of every rule label to a single canonical    ║
# ║  key used in if-else routing inside build_tables().                        ║
# ║  — change when required: when you add a new rule, add its alias here too. ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

ALIASES = {
    # --- NSDL ---
    "PAY_IN_DEBIT": "PAYINDEBIT", "BLOCKED_EARLY_PAYIN": "BLOCKEDEARLYPAYIN",
    "BLOCKED_FOR_DEBIT": "BLOCKEDFORDEBIT", "DEBIT_MISC": "DEBITMISC",
    "CREDIT_GENERAL": "CREDITGENERAL",
    "CREDIT_PUBLIC_OFFER": "CREDITPUBLICOFFER",
    "DEBIT_INVOCATION": "DEBITINVOCATION",
    "PLEDGE_INITIATION": "PLEDGEINITIATION",
    "AUTO_PLEDGE_CUSPA": "AUTOPLEDGECUSPA",
    "BONUS_CREDIT": "BONUSCREDIT",
    "MARGIN_PLEDGE_NOTIFICATION": "MARGINPLEDGENOTIFICATION",
    "BENEFICIARY_ADDITION": "BENEFICIARYADDITION",
    "CAS_STATEMENT": "CASSTATEMENT",
    "MF_UNITS_DEBIT": "MFUNITSDEBIT", "MF_UNITS_CREDIT": "MFUNITSCREDIT",
    "MF_CONVERSION": "MFCONVERSION",
    "MF_CONVERSION_APPROVED": "MFCONVERSIONAPPROVED",
    "MF_CONVERSION_LINK": "MFCONVERSIONLINK",
    "POA_REGISTERED": "POAREGISTERED",
    "DDPI_REGISTERED": "DDPIREGISTERED",
    "INSTA_DEMAT_REGISTERED": "INSTADEMATREGISTERED",
    "ISIN_REQUEST_GENERATED": "ISINREQUESTGENERATED",
    "REDEMPTION_CERTIFICATE": "REDEMPTIONCERTIFICATE",
    "VOTE_CAST_CONFIRMATION": "VOTECASTCONFIRMATION",
    "EVOTING_NOTICE": "EVOTINGNOTICE",
    "WELCOME_SMS_ALERT": "WELCOMESMSALERT",
    "DP_CLOSURE_NOTICE": "DPCLOSURENOTICE",
    "DESIGNATED_PERSON_TWCP": "DESIGNATEDPERSONTWCP",
    "DESIGNATED_PERSON_TWCP_REMOVED": "DESIGNATEDPERSONTWCPREMOVED",
    "MOBILE_UPDATED_NSDL": "MOBILEUPDATEDNSDL",
    "BLOCKED_BUYBACK": "BLOCKEDBUYBACK",
    # --- CDSL ---
    "CREDIT_GENERAL_CDSL": "CREDITGENERALCDSL",
    "BANK_ACCOUNT_CHANGED": "BANKACCOUNTCHANGED",
    "BANK_NAME_CHANGED": "BANKNAMECHANGED",
    "DEBIT_SHORT": "DEBITSHORT", "DEBIT_BULK": "DEBITBULK",
    "CREDIT_IPO_FPO": "CREDITIPOFPO",
    "CREDITED_SCHEME": "CREDITEDSCHEME",
    "DEBITED_EXTINGUISHMENT": "DEBITEDEXTINGUISHMENT",
    "STOCK_SPLIT": "STOCKSPLIT",
    "TEMP_ISIN_CONVERSION": "TEMPISINCONVERSION",
    "CONSOLIDATION": "CONSOLIDATION",
    "PLEDGE_BULK_ACCEPTED": "PLEDGEBULKACCEPTED",
    "PLEDGE_ACCEPTED": "PLEDGEACCEPTED",
    "UNPLEDGE_ACCEPTED": "UNPLEDGEACCEPTED",
    "PLEDGE_INVOKED": "PLEDGEINVOKED",
    "TPIN_GENERATED": "TPINGENERATED", "TPIN_EDIS": "TPINEDIS",
    "VOTING_WITH_MEETING": "VOTINGWITHMEETING", "VOTING": "VOTING",
    "HALF_YEARLY_CAS": "HALFYEARLYCAS",
    "NOMINEE_UPDATE": "NOMINEEUPDATE",
    "PORTFOLIO_VALUATION": "PORTFOLIOVALUATION",
    "MOBILE_UPDATED_CDSL": "MOBILEUPDATEDCDSL",
    "SIGNATURE_UPDATED": "SIGNATUREUPDATED",
    "ADDRESS_CHANGED": "ADDRESSCHANGED",
    "ACCOUNT_FROZEN": "ACCOUNTFROZEN",
    "WARRANTS_CREDITED": "WARRANTSCREDITED",
    "UCC_LINKED": "UCCLINKED", "DIS_ISSUED": "DISISSUED",
    "SMS_DEREGISTERED": "SMSDEREGISTERED",
    "POSITIVE_CONSENT": "POSITIVECONSENT",
    "DEMAT_ACCOUNT_OPENED_CDSL": "DEMATACCOUNTOPENEDCDSL",
    # --- NSE ---
    "BROKER_FUND_BALANCE": "BROKERFUNDBALANCE",
    "TRADED_VALUE": "TRADEDVALUE",
    "UCC_REGISTRATION": "UCCREGISTRATION",
    "NSE_UCC_WELCOME": "UCCWELCOME",
    "INVESTOR_ADVISORY_NSE": "INVESTORADVISORYNSE",
    "NSE_INVEST_AUTHORIZE": "NSEINVESTAUTHORIZE",
    "NSE_INVEST_SIP_CANCELLED": "NSEINVESTSIPCANCELLED",
    "SIP_AUTHORIZE": "SIPAUTHORIZE",
    "NSE_MF_INVEST_SUBMITTED": "NSEMFINVESTSUBMITTED",
    "NSE_DEFAULTER_NOTICE": "NSEDEFAULTERNOTICE",
    # --- BSE ---
    "TRADE_CONFIRMATION": "TRADECONFIRMATION",
    "FUND_BALANCE_EOD": "FUNDBALANCEEOD",
    "UNSOLICITED_TIPS": "UNSOLICITEDTIPS",
    "APPLICATION_SUBMITTED": "APPLICATIONSUBMITTED",
    "NO_ALLOTMENT": "NOALLOTMENT",
    "SUCCESSFUL_ALLOTMENT": "SUCCESSFULALLOTMENT",
    "ISSUE_BIDDING_OPEN": "ISSUEBIDDINGOPEN",
    # --- Angel One ---
    "MARGIN_UTILIZATION_WARNING": "MARGINUTILIZATIONWARNING",
    "MTM_LOSS_WARNING": "MTMLOSSWARNING",
    "TOTAL_MARGIN_SHORTFALL": "TOTALMARGINSHORTFALL",
    "TRADING_ACCOUNT_SHORTAGE": "TRADINGACCOUNTSHORTAGE",
    "PAY_TO_AVOID_SHARE_SALE": "PAYTOAVOIDSHARESALE",
    "SHARES_WILL_BE_SOLD_TODAY": "SHARESWILLBESOLDTODAY",
    "SHARES_SOLD_SHORTAGE": "SHARESSOLDSHORTAGE",
    "AUTO_PLEDGE_OUTSTANDING": "AUTOPLEDGEOUTSTANDING",
    "MARGIN_INCREASE_HAIRCUT": "MARGININCREASEHAIRCUT",
    "MTF_MARGIN_SHORTAGE": "MTFMARGINSHORTAGE",
    "DEMAT_ACCOUNT_DEBIT_WARNING": "DEMATACCOUNTDEBITWARNING",
    "FO_PHYSICAL_DELIVERY_WARNING": "FOPHYSICALDELIVERYWARNING",
    "FO_PHYSICAL_DELIVERY_V2": "FOPHYSICALDELIVERYV2",
    "OPTIONS_EXPIRY_ALERT": "OPTIONSEXPIRYALERT",
    "POSITION_MARGIN_ALERT": "POSITIONMARGINALERT",
    "FO_MARGIN_PENALTY": "FOMARGINPENALTY",
    "PAYOUT_PROCESSED": "PAYOUTPROCESSED",
    "FUNDS_PAYOUT_REQUEST": "PAYOUTREQUEST",
    "MANDATORY_SEBI_PAYOUT": "MANDATORYSEBIPAYOUT",
    "MANDATORY_SEBI_PAYOUT_GENERIC": "MANDATORYSEBIPAYOUTGENERIC",
    "WITHDRAWAL_REJECTED": "WITHDRAWALREJECTED",
    "MONTHLY_SETTLEMENT": "MONTHLYSETTLEMENT",
    "SIP_SETUP_COMPLETED": "SIPSETUPCOMPLETED",
    "SIP_DUE_REMINDER": "SIPDUEREMINDER",
    "SIP_UPCOMING_DEBIT": "SIPUPCOMINGDEBIT",
    "SIP_AUTOPAY_PAUSED": "SIPAUTOPAYPAUSED",
    "MF_SIP_MISSED_TXN": "MFSSIPMISSEDTXN",
    "BANK_UPDATE_REQUEST": "BANKUPDATEREQUEST",
    "BANK_MODIFICATION_APPROVED": "BANKMODIFICATIONAPPROVED",
    "CONTACT_DETAILS_MODIFICATION": "CONTACTDETAILSMODIFICATION",
    "INCOME_UPDATION_REQUEST": "INCOMEUPDATIONREQUEST",
    "NOMINEE_ADDED": "NOMINEEADDED",
    "NOMINEE_CHOSEN_LATER": "NOMINEECHOSENLATER",
    "NOMINEE_PREFERENCE_UPDATED": "NOMINEEPREFERENCEUPDATED",
    "SEGMENT_ACTIVATION_REQUEST": "SEGMENTACTIVATIONREQUEST",
    "DDPI_PROCESSING": "DDPIPROCESSING",
    "DOCUMENT_REVIEW": "DOCUMENTREVIEW",
    "ESIGN_PENDING": "ESIGNPENDING", "IPV_PENDING": "IPVPENDING",
    "CONTRACT_NOTE_UNDELIVERED": "CONTRACTNOTEUNDELIVERED",
    "IPO_APPLICATION_AP": "IPOAPPLICATIONAP",
    "IPO_ALLOTMENT_CONGRATS": "IPOALLOTMENTCONGRATS",
    "IPO_ALLOTMENT_STATUS": "IPOALLOTMENTSTATUS",
    "BUYBACK_BID_ACCEPTED": "BUYBACKBIDACCEPTED",
    "BUYBACK_PLACE_REQUEST": "BUYBACKPLACEREQUEST",
    "CLIENT_TRANSFER_REQUEST": "CLIENTTRANSFERREQUEST",
    "SHORT_DELIVERY_WARNING": "SHORTDELIVERYWARNING",
    "REFERRAL_THANK_YOU": "REFERRALTHANKYOU",
    "USERID_CREATED": "USERIDCREATED",
    "DEMAT_APPLICATION_INCOMPLETE": "DEMATAPPLICATIONINCOMPLETE",
    "ACCOUNT_DORMANCY_WARNING": "ACCOUNTDORMANCYWARNING",
    "ENABLE_NOTIFICATIONS": "ENABLENOTIFICATIONS",
    "ANGEL_PROMO": "ANGELPROMO",
    # --- Groww ---
    "QUARTERLY_SETTLEMENT_GROWW": "QUARTERLYSETTLEMENTGROWW",
    "GROWW_UPI_REGISTRATION": "GROWWUPIREGISTRATION",
    "MARGIN_SHORTFALL": "MARGINSHORTFALL",
    "UPI_CREDIT_RECEIVED": "UPICREDITRECEIVED",
    "SIP_PAYMENT_UNSUCCESSFUL": "SIPPAYMENTUNSUCCESSFUL",
    "NEGATIVE_BALANCE_WARNING": "NEGATIVEBALANCEWARNING",
    "PAYOUT_PROCESSED_GROWW": "PAYOUTPROCESSEDGROWW",
    "CONTRACT_NOTE_GROWW": "CONTRACTNOTEGROWW",
    "INTRADAY_ALERT": "INTRADAYALERT",
    "BROKER_FUND_BALANCE_GROWW": "BROKERFUNDBALANCEGROWW",
    "WELCOME_ACCOUNT_SETUP": "WELCOMEACCOUNTSETUP",
    "WEBINAR_INVITE": "WEBINARINVITE",
    "GROWW_CREDIT_EMI": "GROWWCREDITEMI",
    # --- MOFSL ---
    "MOFSL_TRD": "MOFSLTRD", "MOFSL_TRD_SEGMENT": "MOFSLTRDSEGMENT",
    "MOFSL_TRD_MCX": "MOFSLTRDMCX", "MOFSL_TRD_LINK": "MOFSLTRDLINK",
    "MOFSL_HIGH_VALUE_TXN": "MOFSLHIGHVALUETXN",
    "MOFSL_DERIVATIVE_PNL": "MOFSLDERIVATIVEPNL",
    "MOFSL_TRADE_CONFIRM_CALL": "MOFSLTRADECONFIRMCALL",
    "FUND_BALANCE": "FUNDBALANCE", "FUND_BALANCE_BSE": "FUNDBALANCEBSE",
    "PAYOUT_PROCESSED_MOFSL": "PAYOUTPROCESSEDMOFSL",
    "CALL_ALERT": "CALLALERT",
    # --- Zerodha ---
    "MARGIN_UTILISATION": "MARGINUTILISATION",
    "DEBIT_BALANCE_MTF": "DEBITBALANCEMTF",
    "SHORTFALL_WARNING": "SHORTFALLWARNING",
    "BUYBACK_ORDER_PLACED": "BUYBACKORDERPLACED",
    "ACCOUNT_LOCKED": "ACCOUNTLOCKED",
    "ORDER_EXECUTED": "ORDEREXECUTED",
    "GIFT_STOCKS": "GIFTSTOCKS",
    "SEBI_TRANSFER": "SEBITRANSFER",
    "PHYSICAL_DELIVERY_WARNING": "PHYSICALDELIVERYWARNING",
    "BROKER_FUND_BALANCE_ZERODHA": "BROKERFUNDBALANCEZERODHA",
    "PAYOUT_PROCESSED_ZERODHA": "PAYOUTPROCESSEDZERODHA",
    "QUARTERLY_SETTLEMENT_ZERODHA": "QUARTERLYSETTLEMENTZERODHA",
    # --- MCX ---
    "MCX_PRICE_ALERT": "MCXPRICEALERT",
    "MCX_OBLIGATION": "MCXOBLIGATION",
    "MCX_TRADE_EXECUTED": "MCXTRADEEXECUTED",
    "MCX_CLIENT_FUNDS": "MCXCLIENTFUNDS",
    "MCX_UCC_REGISTRATION": "MCXUCCREGISTRATION",
    # --- Upstox ---
    "BROKER_FUND_BALANCE_UPSTOX": "BROKERFUNDBALANCEUPSTOX",
    "PAYOUT_PROCESSED_UPSTOX": "PAYOUTPROCESSEDUPSTOX",
    # --- Dhan ---
    "DHAN_AUTH_CODE": "DHANAUTHCODE",
    "DHAN_ACCOUNT_BLOCKED": "DHANACCOUNTBLOCKED",
    "DHAN_RUNNING_SETTLEMENT": "DHANRUNNINGSETTLEMENT",
    "PAYOUT_DHAN": "PAYOUTDHAN",
    # --- Generic ---
    "PAYOUT_GENERIC": "PAYOUTGENERIC",
    # --- Account ---
    "ACCOUNT_ACTIVATED": "ACCOUNTACTIVATED",
    "ACCOUNT_OPENED": "ACCOUNTOPENED",
    "ACCOUNT_CLOSED": "ACCOUNTCLOSED",
    "ACCOUNT_CLOSURE_REQUEST": "ACCOUNTCLOSUREREQUEST",
    "ACCOUNT_SUSPENDED": "ACCOUNTSUSPENDED",
}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 7 — TABLE SCHEMA DEFINITIONS                                     ║
# ║                                                                            ║
# ║  Each key is the OUTPUT SHEET NAME.  The value is the ordered column list. ║
# ║  — change when required: rename sheets / add / remove columns.            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

TABLE_COLUMNS = {
    # Broker ↔ Client registration events
    "Broker_Associations": [
        "client_id", "broker_name", "dp_name", "exchange", "ucc",
        "segment", "registration_date", "account_activation_date",
        "account_closure_date",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # All transactions: share movement, trade summaries, payouts, corp-actions
    "Transactions": [
        "client_id", "transaction_category", "txn_date", "exchange",
        "broker_name", "depository", "txn_type", "segment",
        "security_name", "security_isin", "qty",
        "traded_value", "eq_value", "fno_value", "currency_value",
        "cm_value", "fo_value", "led_bal", "buy_qty", "buy_value",
        "sell_qty", "sell_value", "member_code",
        "amount", "bank_account_mask", "payout_type",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # Pledge / unpledge / invocation
    "Pledge_Activity": [
        "client_id", "depository", "pledgee_account",
        "security_name", "security_isin",
        "qty", "txn_type", "margin_broker_name",
        "masked_account", "date", "pledge_ref_no",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # End-of-day fund & securities balances
    "EOD_Balances": [
        "client_id", "broker_name", "exchange", "date", "fund_balance",
        "securities_balance", "currency", "includes_bank_pms",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # Mutual-fund SIP / lump-sum activity
    "Mutual_Funds": [
        "client_id", "broker_name", "fund_name", "folio_number", "sip_id",
        "amount", "units", "txn_type", "mf_or_etf", "due_date", "status",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # IPO application → allotment lifecycle
    "IPO_Lifecycle": [
        "client_id", "application_id", "company_name", "event_type",
        "qty_allotted", "issue_price", "exchange", "date",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # KYC / profile field changes
    "KYC_Changes": [
        "client_id", "account_mask", "field_changed",
        "old_value", "new_value",
        "date", "depository_or_broker",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # Account lifecycle: open, close, TPIN, voting, registrations, etc.
    "Account_Events": [
        "client_id", "broker_name", "security_name", "event_type", "segment",
        "request_id_or_ref_no", "status", "date", "account_mask",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # Margin / risk / MTM / shortfall alerts
    "Margin_Risk_Alerts": [
        "client_id", "broker_name", "alert_type", "type",
        "utilisation_pct", "mtm_loss_pct", "amount", "security_name",
        "date", "segment",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # Statement / document delivery (CAS, contract note, PnL)
    "Statements_Docs": [
        "client_id", "doc_type", "period", "email", "delivery_mode", "date",
        "depository",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # Point-in-time portfolio valuation
    "Portfolio_Valuations": [
        "client_id", "depository", "account_mask", "valuation_date", "value",
        "currency",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
    # Advisory / promo / uncategorised catch-all
    "Advisory_Promo": [
        "client_id", "msg_type", "broker_or_exchange", "topic", "date",
        "source_classifier", "source_sms_id", "event_timestamp", "full_sms",
    ],
}


# ── Alert labels that get routed to Advisory_Promo — change when required ──
_alert_labels = {
    "GROWW": {
        "INTRADAY_ALERT", "SIP_PAYMENT_UNSUCCESSFUL",
        "UPI_CREDIT_RECEIVED", "WELCOME_ACCOUNT_SETUP", "WEBINAR_INVITE",
    },
    "ANGELONE": {
        "MARGIN_UTILIZATION_WARNING", "MTM_LOSS_WARNING",
        "FO_PHYSICAL_DELIVERY_WARNING", "ACCOUNT_DORMANCY_WARNING",
        "ENABLE_NOTIFICATIONS", "DEMAT_APPLICATION_INCOMPLETE",
    },
    "ZERODHA": {"MARGIN_UTILISATION", "PHYSICAL_DELIVERY_WARNING"},
    "MOFSL": {"CALL_ALERT"},
    "MCX": set(), "CDSL": set(), "NSDL": set(),
    "BSE": {"UNSOLICITED_TIPS"},
    "NSE": {"INVESTOR_ADVISORY_NSE"},
}

# ── Column-name hints for auto-detecting columns in the input CSV ──
# — change when required (add alternate spellings)
BASE_HINTS = {
    "client_id": ["ClientCode", "clientcode", "client_id", "clientid"],
    "sms_id":    ["AppId", "appid", "app_id", "sms_id"],
    "message":   ["smsmessage", "sms_message", "message", "text", "body"],
    "event_time": [
        "smsdatetime", "sms_datetime", "TimeStamp", "timestamp",
        "smstimestamp",
    ],
}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 8 — CLASSIFIER ENGINE                                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


def build_rule_index(rules):
    """Flatten the nested RULES dict into a list of
    (FAMILY, label, canonical_key, compiled_pattern) tuples
    for fast iteration during classification.
    """
    out = []
    for family, items in rules.items():
        for label, pat in items:
            out.append((
                str(family).upper(),   # e.g. "NSDL"
                str(label),            # e.g. "PAY_IN_DEBIT"
                canon(label),          # e.g. "PAYINDEBIT"
                pat,                   # compiled regex
            ))
    return out


def classify(text, rule_index):
    """Run every rule against `text`.  Return:
      (source_classifier, family, parsed_fields_dict, [all_hit_labels])
    The FIRST match wins and provides the canonical label + parsed fields.
    We still record ALL hits for QA purposes.
    """
    winning = None
    winning_fields = None
    hit_labels = []

    for family, label, _key, pat in rule_index:
        m = pat.search(text)
        if m:
            hit_labels.append(f"{family}_{label}")
            if winning is None:  # first match = winner
                winning = (family, label)
                winning_fields = m.groupdict()

    if winning is None:
        return "UNCATEGORISED", "", {}, []

    fam, label = winning
    return f"{fam}_{label}", fam, norm_fields(winning_fields), hit_labels


# ── Parallel classification helpers (optional) ──

_worker_rule_index = None  # module-level cache for child processes


def _init_worker():
    """Initialise the rule index once per worker process."""
    global _worker_rule_index
    _worker_rule_index = build_rule_index(RULES)


def _classify_chunk(texts):
    """Classify a list of texts using the worker-local rule index."""
    global _worker_rule_index
    if _worker_rule_index is None:
        _worker_rule_index = build_rule_index(RULES)
    return [classify(t, _worker_rule_index) for t in texts]


def _chunked(seq, size):
    """Yield successive `size`-length slices from `seq`."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def classify_all(texts, rule_index, use_mp=True, chunk_size=CLASSIFY_CHUNK_SIZE):
    """Classify all texts, optionally using multiprocessing.
    Falls back to single-threaded if use_mp=False or data is small.
    """
    n = len(texts)
    workers = os.cpu_count() or 1

    # Single-threaded path
    if not use_mp or workers <= 1 or n < chunk_size * 2:
        return [classify(t, rule_index) for t in texts]

    # Multi-process path
    chunks = list(_chunked(texts, chunk_size))
    results = []
    with ProcessPoolExecutor(max_workers=workers,
                             initializer=_init_worker) as ex:
        for batch in ex.map(_classify_chunk, chunks):
            results.extend(batch)
    return results


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 9 — TABLE BUILDER  (the main routing logic)                      ║
# ║                                                                            ║
# ║  Iterates over every classified SMS and appends rows to the appropriate    ║
# ║  output table(s).  One SMS may produce rows in MULTIPLE tables            ║
# ║  (e.g. a pledge SMS creates rows in both Transactions and Pledge_Activity).║
# ║                                                                            ║
# ║  — change when required: add new routing blocks for new classifiers.      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


def add(rows, table, **kw):
    """Append a dict of column values to a table's row list."""
    rows[table].append(kw)


def core(r):
    """Extract the base columns that every output row needs."""
    return {
        "client_id": r.client_id,
        "source_sms_id": r.source_sms_id,
        "event_timestamp": r.event_timestamp,
        "source_classifier": r.source_classifier,
        "full_sms": r.sms_message,
    }


def build_tables(master):
    """Main routing function: for each classified SMS row, decide which
    output table(s) it belongs to and construct the normalised row(s).

    Returns a dict  {sheet_name: pd.DataFrame}.
    """
    # Initialise empty row-lists for every table
    rows = {k: [] for k in TABLE_COLUMNS}

    for r in master.itertuples(index=False):
        f = r.fields                       # parsed fields dict
        cls = r.source_classifier          # e.g. "NSDL_PAY_IN_DEBIT"
        c = canon(cls)                     # e.g. "PAYINDEBIT"
        fam = r.classifier_family          # e.g. "NSDL"
        text = r.sms_message               # cleaned SMS body
        b = r.broker_name                  # normalised broker name
        ex = exchange_from(cls, text)      # NSE / BSE / MCX / NaN
        base = core(r)                     # common columns for every row
        dep = family_depository(fam)       # "NSDL" or "CDSL" or NaN

        # Parse a date from fields or fall back to the SMS timestamp
        dt = parse_date(
            val(f, "date", "date2", "closedate"),
            (r.event_timestamp.normalize()
             if pd.notna(r.event_timestamp) else pd.NaT))

        # Track how many non-promo rows we've added BEFORE this SMS
        # so we can decide at the end whether to add an Advisory_Promo row
        _placed_before = sum(
            len(v) for k, v in rows.items() if k != "Advisory_Promo"
        )

        # ────────────────────────────────────────────────────────────
        # TABLE: Broker_Associations
        # Triggers: UCC registration, account open/close/activate,
        #           POA/DDPI, MCX UCC, DP closure
        # ────────────────────────────────────────────────────────────
        broker_triggers = [
            "UCCREGISTRATION", "UCCWELCOME", "BROKERFUNDBALANCE",
            "FUNDBALANCE", "ACCOUNTACTIVATED", "ACCOUNTOPENED",
            "ACCOUNTCLOSED", "ACCOUNTCLOSUREREQUEST", "ACCOUNTSUSPENDED",
            "INSTADEMATREGISTERED", "POAREGISTERED", "DDPIREGISTERED",
            "MCXUCCREGISTRATION", "DPCLOSURENOTICE",
        ]
        if any(x in c for x in broker_triggers):
            # Skip if UCC welcome but no broker could be determined
            if "UCCWELCOME" in c and pd.isna(b):
                pass
            else:
                is_closure = any(
                    x in c for x in ["CLOSURE", "ACCOUNTCLOSED", "DPCLOSURE"]
                )
                is_activation = any(
                    x in c for x in ["ACCOUNTACTIVAT", "ACCOUNTOPENED"]
                )
                seg = detect_trading_segment(text, f)
                add(rows, "Broker_Associations", **base,
                    broker_name=b, dp_name=val(f, "dp", "dpname"),
                    exchange=ex, ucc=val(f, "ucc"), segment=seg,
                    registration_date=(
                        dt if ("UCC" in c or "INSTA" in c) else pd.NaT),
                    account_activation_date=(
                        dt if is_activation else pd.NaT),
                    account_closure_date=(
                        dt if is_closure else pd.NaT))

        # ────────────────────────────────────────────────────────────
        # TABLE: Transactions — share movements (debit / credit)
        # ────────────────────────────────────────────────────────────
        share_type = None
        if any(x in c for x in [
            "PAYINDEBIT", "DEBITMISC", "DEBITSHORT", "DEBITBULK"
        ]):
            share_type = "debit"
        elif any(x in c for x in [
            "CREDITGENERAL", "CREDITGENERALCDSL", "CREDITIPOFPO",
            "CREDITPUBLICOFFER"
        ]):
            share_type = "credit"
        elif "BLOCKED" in c and "ACCOUNT" not in c:
            share_type = "early_payin" if "EARLY" in c else "blocked"

        if share_type:
            # Some CDSL messages pack multiple holdings in one string
            items = (
                holdings(val(f, "holdings"))
                if pd.notna(val(f, "holdings"))
                else [(to_num(val(f, "qty")), val(f, "security"))]
            )
            for q, sec in items:
                sec_clean = clean_security_name(sec)
                seg = detect_segment(sec_clean)
                add(rows, "Transactions", **base,
                    transaction_category="share_movement", txn_date=dt,
                    exchange=ex, broker_name=b, depository=dep,
                    txn_type=share_type, segment=seg,
                    security_name=sec_clean,
                    security_isin=val(f, "isin"),
                    qty=q, amount=np.nan,
                    bank_account_mask=np.nan, payout_type=np.nan)

        # ── NSDL MF units debit/credit → Transactions ──
        if any(x in c for x in ["MFUNITSDEBIT", "MFUNITSCREDIT"]):
            mf_txn_type = (
                "units_debit" if "MFUNITSDEBIT" in c else "units_credit"
            )
            sec_name = clean_security_name(val(f, "security"))
            seg = detect_segment(sec_name)
            add(rows, "Transactions", **base,
                transaction_category="mf_units_movement", txn_date=dt,
                exchange=ex, broker_name=b, depository=dep,
                txn_type=mf_txn_type, segment=seg,
                security_name=sec_name,
                security_isin=val(f, "isin"),
                qty=to_num(val(f, "qty")),
                amount=np.nan, bank_account_mask=np.nan,
                payout_type=np.nan)

        # ── NSDL debit invocation → Transactions ──
        if "DEBITINVOCATION" in c:
            sec_name = clean_security_name(val(f, "security"))
            add(rows, "Transactions", **base,
                transaction_category="pledge_invocation_debit", txn_date=dt,
                exchange=ex, broker_name=b, depository=dep,
                txn_type="invocation_debit",
                segment=detect_segment(sec_name),
                security_name=sec_name,
                security_isin=val(f, "isin"),
                qty=to_num(val(f, "qty")),
                amount=np.nan, bank_account_mask=np.nan,
                payout_type=np.nan)

        # ── Zerodha order executed → Transactions ──
        if "ORDEREXECUTED" in c:
            sec_name = clean_security_name(val(f, "security"))
            side = (clean_text(val(f, "side")).lower()
                    if pd.notna(val(f, "side")) else "trade")
            add(rows, "Transactions", **base,
                transaction_category="trade", txn_date=dt,
                exchange=ex, broker_name=b, depository=np.nan,
                txn_type=side, segment="EQ",
                security_name=sec_name, security_isin=np.nan,
                qty=to_num(val(f, "qty")),
                traded_value=to_num(val(f, "price")),
                amount=np.nan, bank_account_mask=np.nan,
                payout_type=np.nan)

        # ── Zerodha buyback order placed → Transactions ──
        if "BUYBACKORDERPLACED" in c:
            add(rows, "Transactions", **base,
                transaction_category="buyback", txn_date=dt,
                exchange=ex, broker_name=b, depository=np.nan,
                txn_type="buyback_order", segment="EQ",
                security_name=clean_security_name(val(f, "company")),
                security_isin=np.nan, qty=np.nan,
                amount=np.nan, bank_account_mask=np.nan,
                payout_type=np.nan)

        # ── Angel One buyback bid accepted → Transactions ──
        if "BUYBACKBIDACCEPTED" in c:
            add(rows, "Transactions", **base,
                transaction_category="buyback", txn_date=dt,
                exchange=ex, broker_name=b, depository=np.nan,
                txn_type="buyback_accepted", segment="EQ",
                security_name=clean_security_name(val(f, "company")),
                security_isin=np.nan,
                qty=to_num(val(f, "qty")),
                amount=np.nan, bank_account_mask=np.nan,
                payout_type=np.nan)

        # ── Zerodha gift stocks → Transactions ──
        if "GIFTSTOCKS" in c:
            add(rows, "Transactions", **base,
                transaction_category="gift", txn_date=dt,
                exchange=ex, broker_name=b, depository=np.nan,
                txn_type="gift_received", segment="EQ",
                security_name=np.nan, security_isin=np.nan, qty=np.nan,
                amount=np.nan, bank_account_mask=np.nan,
                payout_type=np.nan)

        # ── Corporate actions → Transactions ──
        corp_map = {
            "BONUSCREDIT": "bonus",
            "CREDITEDSCHEME": (
                clean_text(val(f, "reason")).lower()
                if pd.notna(val(f, "reason"))
                else "scheme_of_arrangement"),
            "DEBITEDEXTINGUISHMENT": (
                clean_text(val(f, "reason")).lower()
                if pd.notna(val(f, "reason"))
                else "extinguishment"),
            "CONSOLIDATION": "consolidation",
            "STOCKSPLIT": "split",
            "TEMPISINCONVERSION": "isin_conversion",
        }
        corp_txn = None
        for k, v in corp_map.items():
            if k in c:
                corp_txn = v
                break
        if corp_txn is None:
            for kw, label in [
                ("BONUS", "bonus"), ("RIGHTS", "rights"),
                ("SPLIT", "split"), ("MERGER", "merger"),
                ("SCHEME", "scheme_of_arrangement"),
                ("EXTINGUISHMENT", "extinguishment"),
                ("REDEMPTION", "redemption"), ("WARRANTS", "warrants"),
            ]:
                if kw in c:
                    corp_txn = label
                    break

        if corp_txn:
            corp_items = (
                [(to_num(val(f, "qty_out")), val(f, "security"))]
                if "qty_out" in f and pd.notna(val(f, "qty_out"))
                else (holdings(val(f, "holdings"))
                      if pd.notna(val(f, "holdings"))
                      else [(to_num(val(f, "qty")),
                             val(f, "security"))])
            )
            corp_dir = ("debit"
                        if corp_txn in ("extinguishment", "redemption")
                        else "credit")
            for q, sec in corp_items:
                sec_clean = clean_security_name(sec)
                seg = detect_segment(sec_clean)
                add(rows, "Transactions", **base,
                    transaction_category="corporate_action", txn_date=dt,
                    exchange=ex, broker_name=b, depository=dep,
                    txn_type=f"{corp_txn}_{corp_dir}", segment=seg,
                    security_name=sec_clean,
                    security_isin=val(f, "isin"),
                    qty=q, amount=np.nan,
                    bank_account_mask=np.nan, payout_type=np.nan)

        # ── NSDL redemption certificate → Transactions ──
        if "REDEMPTIONCERTIFICATE" in c:
            add(rows, "Transactions", **base,
                transaction_category="corporate_action", txn_date=dt,
                exchange=ex, broker_name=b, depository=dep,
                txn_type="redemption_certificate", segment="EQ",
                security_name=np.nan,
                security_isin=val(f, "isin"),
                qty=np.nan, amount=np.nan,
                bank_account_mask=np.nan, payout_type=np.nan)

        # ────────────────────────────────────────────────────────────
        # TABLE: Pledge_Activity
        # ────────────────────────────────────────────────────────────
        if any(x in c for x in [
            "PLEDGEACCEPTED", "PLEDGEBULKACCEPTED", "UNPLEDGEACCEPTED",
            "PLEDGEINITIATION", "AUTOPLEDGE", "MARGINPLEDGE",
            "PLEDGEINVOKED", "AUTOPLEDGEOUTSTANDING",
        ]):
            typ = ("unpledge" if "UNPLEDGE" in c
                   else "margin_pledge" if "MARGINPLEDGE" in c
                   else "pledge_invoked" if "PLEDGEINVOKED" in c
                   else "auto_pledge" if "AUTOPLEDGEOUTSTANDING" in c
                   else "pledge")
            items = (
                holdings(val(f, "holdings"))
                if pd.notna(val(f, "holdings"))
                else [(to_num(val(f, "qty")), val(f, "security"))]
            )
            for q, sec in items:
                add(rows, "Pledge_Activity", **base,
                    depository=dep,
                    pledgee_account=val(f, "pledgeeac"),
                    security_name=clean_security_name(sec),
                    security_isin=val(f, "isin"), qty=q,
                    txn_type=typ,
                    margin_broker_name=val(f, "dp", "broker"),
                    masked_account=val(f, "acmasked", "acno"),
                    date=dt,
                    pledge_ref_no=val(f, "ref", "urn"))

        # ────────────────────────────────────────────────────────────
        # TABLE: EOD_Balances
        # ────────────────────────────────────────────────────────────
        if any(x in c for x in [
            "FUNDBALANCE", "CLIENTFUNDS", "BROKERFUNDBALANCE"
        ]):
            fund_b = to_num(val(
                f, "fundbal", "fund_bal", "fund_balance", "fund",
                "mcxbal", "mcx_bal"))
            sec_b = to_num(val(
                f, "secbal", "sec_bal", "securities_balance", "sec"))
            add(rows, "EOD_Balances", **base,
                broker_name=b, exchange=ex, date=dt,
                fund_balance=fund_b, securities_balance=sec_b,
                currency="INR", includes_bank_pms=False)

        # ── Traded-value summary → Transactions ──
        if any(x in c for x in [
            "TRADEDVALUE", "TRADECONFIRMATION", "MOFSLTRD",
            "MOFSLTRDSEGMENT", "MCXTRADEEXECUTED",
            "MOFSLHIGHVALUETXN", "MCXOBLIGATION",
        ]):
            seg = val(f, "segment")
            seg = (seg if pd.notna(seg)
                   else ("MCX" if "MCX" in c
                         else "F&O" if "FO" in c
                         else np.nan))
            add(rows, "Transactions", **base,
                transaction_category="trade_value_summary", txn_date=dt,
                exchange=ex, broker_name=b, depository=dep,
                segment=seg, txn_type="trade_summary",
                traded_value=to_num(val(f, "value")),
                eq_value=to_num(val(f, "eqvalue", "eq_value")),
                fno_value=to_num(val(f, "fnovalue", "fno_value")),
                currency_value=to_num(val(f, "currencyvalue",
                                          "currency_value")),
                cm_value=to_num(val(f, "cm")),
                fo_value=to_num(val(f, "nsefo")),
                led_bal=to_num(val(f, "ledbal")),
                buy_qty=to_num(val(f, "buyqty")),
                buy_value=to_num(val(f, "buyval")),
                sell_qty=to_num(val(f, "sellqty")),
                sell_value=to_num(val(f, "sellval")),
                member_code=val(f, "membercode"),
                amount=np.nan, bank_account_mask=np.nan,
                payout_type=np.nan)

        # ── Payouts / settlements → Transactions ──
        if any(x in c for x in [
            "PAYOUT", "SETTLEMENT", "SEBITRANSFER",
            "QUARTERLYSETTLEMENTGROWW", "DHANRUNNING",
        ]):
            ptype = (
                "sebi_mandatory"
                if ("MANDATORY" in c or "SEBI" in c) else
                "quarterly_settlement"
                if "QUARTERLY" in c else
                "monthly_settlement"
                if "MONTHLY" in c else
                "running_settlement"
                if "RUNNING" in c else
                "withdrawal"
            )
            raw_amt = val(f, "amount")
            if pd.isna(to_num(raw_amt)):
                raw_amt = val(f, "amount2")
            add(rows, "Transactions", **base,
                transaction_category="payout", txn_date=dt,
                exchange=ex, broker_name=b, depository=np.nan,
                txn_type="payout", segment="Payout",
                amount=to_num(raw_amt),
                bank_account_mask=val(f, "acmasked", "bankmask"),
                payout_type=ptype)

        # ────────────────────────────────────────────────────────────
        # TABLE: Mutual_Funds
        # ────────────────────────────────────────────────────────────
        is_nse_invest_auth = "NSEINVESTAUTHORIZE" in c
        is_mf_units = any(x in c for x in ["MFUNITSDEBIT", "MFUNITSCREDIT"])
        is_nse_sip_cancelled = "NSEINVESTSIPCANCELLED" in c
        is_mf_trigger = any(x in c for x in [
            "SIP", "MFINVEST", "MFCONVERSION", "NSEMFINVEST",
            "SIPAUTHORIZE", "MFSSIPMISSEDTXN",
        ])

        if is_nse_invest_auth:
            # NSE Invest lump-sum authorisation
            add(rows, "Mutual_Funds", **base,
                broker_name="NSE Invest", fund_name=np.nan,
                folio_number=np.nan, sip_id=np.nan,
                amount=np.nan, units=np.nan, txn_type="lumpsum",
                mf_or_etf="MF", due_date=dt, status="successful")
        elif is_nse_sip_cancelled:
            # NSE Invest SIP cancelled
            add(rows, "Mutual_Funds", **base,
                broker_name="NSE Invest", fund_name=np.nan,
                folio_number=np.nan,
                sip_id=val(f, "sipid", "sip_id"),
                amount=np.nan, units=np.nan, txn_type="sip",
                mf_or_etf="MF", due_date=dt, status="cancelled")
        elif is_mf_trigger and not is_mf_units:
            # Generic MF / SIP event
            status = (
                "unsuccessful" if "UNSUCCESSFUL" in c
                else "due" if any(
                    x in c for x in ["DUE", "UPCOMING"]) else
                "submitted" if "SUBMITTED" in c else "completed"
            )
            sip_flag = is_sip_from_text(cls, text)
            mf_txn = ("conversion" if "MFCONVERSION" in c
                      else "sip" if sip_flag else "lumpsum")
            fund_nm = val(f, "fund", "symbol", "security")
            mf_etf = detect_mf_or_etf(fund_nm, text)
            add(rows, "Mutual_Funds", **base,
                broker_name=b, fund_name=fund_nm,
                folio_number=val(f, "folio"),
                sip_id=val(f, "sipid", "sip_id"),
                amount=to_num(val(f, "amount")),
                units=to_num(val(f, "qty")),
                txn_type=mf_txn, mf_or_etf=mf_etf,
                due_date=dt, status=status)

        # ────────────────────────────────────────────────────────────
        # TABLE: IPO_Lifecycle
        # ────────────────────────────────────────────────────────────
        if any(x in c for x in [
            "APPLICATIONSUBMITTED", "NOALLOTMENT",
            "SUCCESSFULALLOTMENT", "ISSUEBIDDINGOPEN",
            "IPOALLOTMENT", "CREDITIPOFPO",
            "IPOAPPLICATIONAP", "IPOALLOTMENTCONGRATS",
        ]):
            # Determine the event type
            if "NOALLOTMENT" in c or sms_says_not_allotted(text):
                et = "not_allotted"
            elif ("SUCCESSFULALLOTMENT" in c
                  or "IPOALLOTMENTCONGRATS" in c):
                et = "allotted"
            elif "IPOALLOTMENT" in c:
                et = ("not_allotted"
                      if sms_says_not_allotted(text) else "allotted")
            elif "CREDITIPOFPO" in c:
                et = "credit_received"
            elif "BIDDING" in c:
                et = "bidding_open"
            elif "IPOAPPLICATIONAP" in c:
                et = "application_submitted_via_ap"
            else:
                et = "application_submitted"

            app_id = val(
                f, "applicationno", "application_no", "appid", "app_id"
            )
            add(rows, "IPO_Lifecycle", **base,
                application_id=app_id,
                company_name=val(
                    f, "company", "issuename", "issue_name", "security"),
                event_type=et,
                qty_allotted=to_num(val(f, "qty")),
                issue_price=to_num(val(f, "price")),
                exchange=ex, date=dt)

        # ────────────────────────────────────────────────────────────
        # TABLE: KYC_Changes
        # ────────────────────────────────────────────────────────────
        field = None
        if "NOMINEEUPDATE" in c:
            field = "nominee"
        elif "MOBILEUPDATED" in c:
            field = "mobile"
        elif "SIGNATUREUPDATED" in c:
            field = "signature"
        elif "ADDRESSCHANGED" in c:
            field = "address"
        elif "ACCOUNTFROZEN" in c:
            field = "account_freeze"
        elif any(x in c for x in [
            "BANKUPDATEREQUEST", "BANKMODIFICATIONAPPROVED"
        ]):
            field = "bank_details"
        elif "CONTACTDETAILSMODIFICATION" in c:
            field = "contact_details"
        elif "INCOMEUPDATIONREQUEST" in c:
            field = "income"
        elif any(x in c for x in [
            "NOMINEEADDED", "NOMINEECHOSENLATER",
            "NOMINEEPREFERENCEUPDATED",
        ]):
            field = "nominee"
        elif "BANKACCOUNTCHANGED" in c:
            field = "bank_account_number"
        elif "BANKNAMECHANGED" in c:
            field = "bank_name"

        if field:
            old_v = val(f, "oldvalue", "oldbankac", "oldbank")
            new_v = val(
                f, "newvalue", "newmobile", "freezetype", "ref",
                "newbankac", "newbank"
            )
            add(rows, "KYC_Changes", **base,
                account_mask=val(
                    f, "acmasked", "acno", "ac", "aclast4"),
                field_changed=field,
                old_value=old_v, new_value=new_v,
                date=dt,
                depository_or_broker=(
                    dep if pd.notna(dep) else b))

        # ────────────────────────────────────────────────────────────
        # TABLE: Account_Events
        # ────────────────────────────────────────────────────────────

        # (A) Account open / close / suspend / activate
        account_life = None
        life_status = "completed"
        for key, evt, stat in [
            ("ACCOUNTCLOSED",         "account_closure",    "completed"),
            ("ACCOUNTCLOSUREREQUEST", "closure_request",    "submitted"),
            ("ACCOUNTSUSPENDED",      "account_suspension", "completed"),
            ("ACCOUNTACTIVATED",      "account_activation", "completed"),
            ("ACCOUNTOPENED",         "account_opening",    "completed"),
        ]:
            if key in c:
                account_life = evt
                life_status = stat
                break

        # (B) Other lifecycle events
        if account_life is None:
            for kw, evt, stat in [
                ("DDPIREGISTERED",           "ddpi_registered",       "completed"),
                ("POAREGISTERED",            "poa_registered",        "completed"),
                ("INSTADEMATREGISTERED",     "insta_demat_registered","completed"),
                ("DDPIPROCESSING",           "ddpi_processing",       "processing"),
                ("SEGMENTACTIVATIONREQUEST", "segment_activation",    "submitted"),
                ("ACCOUNTLOCKED",            "account_locked",        "locked"),
                ("DHANACCOUNTBLOCKED",       "account_blocked",       "completed"),
                ("DOCUMENTREVIEW",           "document_review",       "in_progress"),
                ("ESIGNPENDING",             "esign_pending",         "pending"),
                ("IPVPENDING",               "ipv_pending",           "pending"),
                ("DEMATACCOUNTOPENEDCDSL",   "demat_opened",          "completed"),
                ("UCCLINKED",                "ucc_linked",            "completed"),
                ("DISISSUED",                "dis_issued",            "completed"),
                ("SMSDEREGISTERED",          "sms_deregistered",      "completed"),
                ("POSITIVECONSENT",          "positive_consent",      "active"),
                ("BENEFICIARYADDITION",      "beneficiary_addition",  "initiated"),
                ("MFCONVERSIONLINK",         "mf_conversion_info",    "informational"),
                ("CONTRACTNOTEUNDELIVERED",  "contract_note_undelivered", "action_required"),
                ("CLIENTTRANSFERREQUEST",    "client_transfer",       "initiated"),
                ("GROWWUPIREGISTRATION",     "upi_registration",      "initiated"),
                ("DHANAUTHCODE",             "auth_code_generated",   "completed"),
                ("WITHDRAWALREJECTED",       "withdrawal_rejected",   "rejected"),
                ("REFERRALTHANKYOU",         "referral",              "completed"),
                ("BUYBACKPLACEREQUEST",      "buyback_request",       "initiated"),
                ("SHORTDELIVERYWARNING",     "short_delivery_warning","warning"),
            ]:
                if kw in c:
                    account_life = evt
                    life_status = stat
                    break

        # (C) UCC registration also counts as segment activation
        if account_life is None and (
            "UCCREGISTRATION" in c or "MCXUCCREGISTRATION" in c
        ):
            account_life = "segment_activation"
            life_status = "completed"

        if account_life:
            seg = detect_trading_segment(text, f)
            sec_name = extract_security_from_sms(text, f)
            add(rows, "Account_Events", **base,
                broker_name=b, security_name=sec_name,
                event_type=account_life, segment=seg,
                request_id_or_ref_no=val(f, "ref", "urn", "crn"),
                status=life_status, date=dt,
                account_mask=val(
                    f, "acmasked", "acno", "ac", "aclast4", "account"))

        # (D) TPIN events → Account_Events
        if "TPIN" in c:
            tpin_evt = (
                "tpin_changed" if "CHANGED" in c
                else "tpin_edis_assigned" if "EDIS" in c
                else "tpin_generated"
            )
            tpin_broker = val(f, "dp")
            tpin_broker_norm = normalize_broker_name(tpin_broker)
            if pd.isna(tpin_broker_norm):
                tpin_broker_norm = dep if pd.notna(dep) else b
            add(rows, "Account_Events", **base,
                broker_name=tpin_broker_norm,
                security_name=np.nan,
                event_type=tpin_evt, segment=np.nan,
                request_id_or_ref_no=val(f, "tpin"),
                status="completed", date=dt,
                account_mask=val(f, "boid", "bolast4", "bo_id"))

        # ────────────────────────────────────────────────────────────
        # TABLE: Margin_Risk_Alerts
        # ────────────────────────────────────────────────────────────
        risk_map = {
            "MARGINUTILIZATIONWARNING": "margin_utilization",
            "MTMLOSSWARNING":           "mtm_loss",
            "TRADINGACCOUNTSHORTAGE":   "trading_account_shortage",
            "PAYTOAVOIDSHARESALE":      "pay_to_avoid_share_sale",
            "SHARESWILLBESOLDTODAY":    "shares_will_be_sold",
            "SHARESSOLDSHORTAGE":       "shares_sold_shortage",
            "MARGINSHORTFALL":          "margin_shortfall",
            "FOMARGINPENALTY":          "fo_margin_penalty",
            "FOPHYSICALDELIVERYWARNING":"physical_delivery",
            "FOPHYSICALDELIVERYV2":     "physical_delivery",
            "PHYSICALDELIVERYWARNING":  "physical_delivery",
            "MARGINUTILISATION":        "margin_utilisation",
            "DEBITBALANCEMTF":          "debit_balance_mtf",
            "SHORTFALLWARNING":         "shortfall_warning",
            "TOTALMARGINSHORTFALL":     "total_margin_shortfall",
            "MARGININCREASEHAIRCUT":    "margin_increase_haircut",
            "MTFMARGINSHORTAGE":        "mtf_margin_shortage",
            "DEMATACCOUNTDEBITWARNING": "demat_account_debit",
            "OPTIONSEXPIRYALERT":       "options_expiry_alert",
            "POSITIONMARGINALERT":      "position_margin_alert",
            "NEGATIVEBALANCEWARNING":   "negative_balance_warning",
            "INTRADAYALERT":            "intraday_alert",
        }
        risk = None
        for k, v in risk_map.items():
            if k in c:
                risk = v
                break

        if risk:
            util_pct = to_num(val(f, "util"))
            mtm_pct = to_num(val(f, "losspct"))
            # Determine the human-readable alert bucket
            if risk == "mtm_loss" or pd.notna(mtm_pct):
                alert_bucket = "MTM loss"
            elif (risk in ("margin_utilization", "margin_utilisation")
                  or pd.notna(util_pct)):
                alert_bucket = (
                    "Margin already breached"
                    if pd.notna(util_pct) and util_pct >= 100
                    else "Margin utilisation warning"
                )
            elif "shortfall" in risk or "shortage" in risk or "debit" in risk:
                alert_bucket = "Margin shortfall"
            elif "haircut" in risk:
                alert_bucket = "Margin update"
            elif "physical" in risk or "expiry" in risk:
                alert_bucket = "Delivery/Expiry warning"
            else:
                alert_bucket = np.nan

            add(rows, "Margin_Risk_Alerts", **base,
                broker_name=b, alert_type=risk, type=alert_bucket,
                utilisation_pct=util_pct, mtm_loss_pct=mtm_pct,
                amount=to_num(val(f, "amount", "shortfall")),
                security_name=val(f, "security"),
                date=dt,
                segment=(
                    "F&O" if any(
                        x in c for x in ["FO", "FNO", "OPTION", "FUTURES"])
                    else np.nan))

        # ── Trade-confirmation call → Margin_Risk_Alerts ──
        if any(x in c for x in ["CALLALERT", "MOFSLTRADECONFIRMCALL"]):
            add(rows, "Margin_Risk_Alerts", **base,
                broker_name=b, alert_type="trade_confirmation_call",
                type="Operational Alert",
                utilisation_pct=np.nan, mtm_loss_pct=np.nan,
                amount=np.nan, security_name=np.nan,
                date=dt, segment=np.nan)

        # ────────────────────────────────────────────────────────────
        # TABLE: Statements_Docs
        # ────────────────────────────────────────────────────────────
        if (any(x in c for x in ["CAS", "HALFYEARLYCAS"])
                and "TPIN" not in c):
            doc = ("half_yearly_CAS" if "HALF" in c else "monthly_CAS")
            add(rows, "Statements_Docs", **base,
                doc_type=doc, period=val(f, "period", "months"),
                email=val(f, "email"), delivery_mode="email",
                date=dt, depository=dep)

        # MOFSL derivative PnL → Statements_Docs
        if "MOFSLDERIVATIVEPNL" in c:
            add(rows, "Statements_Docs", **base,
                doc_type="derivative_pnl_report", period=np.nan,
                email=np.nan, delivery_mode="link",
                date=dt, depository=np.nan)

        # Groww contract note → Statements_Docs
        if "CONTRACTNOTEGROWW" in c:
            add(rows, "Statements_Docs", **base,
                doc_type="contract_note", period=np.nan,
                email=np.nan, delivery_mode="link",
                date=dt, depository=np.nan)

        # ── MCX price alert → Margin_Risk_Alerts (market data) ──
        if "MCXPRICEALERT" in c:
            add(rows, "Margin_Risk_Alerts", **base,
                broker_name="MCX", alert_type="commodity_price_alert",
                type="Market Data",
                utilisation_pct=np.nan, mtm_loss_pct=np.nan,
                amount=np.nan, security_name=np.nan,
                date=dt, segment="Commodity Derivatives")

        # ────────────────────────────────────────────────────────────
        # TABLE: Portfolio_Valuations
        # ────────────────────────────────────────────────────────────
        if "PORTFOLIOVALUATION" in c:
            add(rows, "Portfolio_Valuations", **base,
                depository="CDSL",
                account_mask=val(f, "acno", "acmasked"),
                valuation_date=dt,
                value=to_num(val(f, "value")),
                currency="INR")

        # ────────────────────────────────────────────────────────────
        # Voting → Account_Events (if not already placed)
        # ────────────────────────────────────────────────────────────
        _placed_elsewhere = (
            sum(len(v) for k, v in rows.items()
                if k != "Advisory_Promo") > _placed_before
        )
        if (any(x in c for x in ["VOTING", "EVOTING", "VOTECAST"])
                and not _placed_elsewhere):
            evt = "vote_cast" if "VOTECAST" in c else "evoting_notice"
            vote_broker = dep if pd.notna(dep) else b
            sec_name = extract_security_from_sms(text, f)
            add(rows, "Account_Events", **base,
                broker_name=vote_broker, security_name=sec_name,
                event_type=evt, segment=np.nan,
                request_id_or_ref_no=np.nan,
                status=(
                    "completed" if "VOTECAST" in c else "notice_sent"),
                date=dt, account_mask=np.nan)
            _placed_elsewhere = True

        # ── Welcome / ISIN / TWCP / UserID → Account_Events ──
        if (any(x in c for x in [
            "WELCOMESMSALERT", "ISINREQUESTGENERATED",
            "DESIGNATEDPERSON", "USERIDCREATED",
        ]) and not _placed_elsewhere):
            evt = ("welcome_sms" if "WELCOME" in c
                   else "isin_request" if "ISIN" in c
                   else "userid_created" if "USERID" in c
                   else "twcp_restriction")
            misc_broker = dep if pd.notna(dep) else b
            add(rows, "Account_Events", **base,
                broker_name=misc_broker,
                security_name=extract_security_from_sms(text, f),
                event_type=evt, segment=np.nan,
                request_id_or_ref_no=val(f, "urn", "isin", "uid"),
                status="completed", date=dt,
                account_mask=val(f, "acmasked", "acno"))
            _placed_elsewhere = True

        # ── NSE defaulter notice → Advisory_Promo ──
        if "NSEDEFAULTERNOTICE" in c and not _placed_elsewhere:
            add(rows, "Advisory_Promo", **base,
                msg_type="regulatory_notice",
                broker_or_exchange="NSE",
                topic="defaulter_notice", date=dt)
            _placed_elsewhere = True

        # ── Groww credit / EMI → Advisory_Promo ──
        if "GROWWCREDITEMI" in c and not _placed_elsewhere:
            add(rows, "Advisory_Promo", **base,
                msg_type="credit_alert",
                broker_or_exchange="Groww",
                topic="emi_reminder", date=dt)
            _placed_elsewhere = True

        # ────────────────────────────────────────────────────────────
        # TABLE: Advisory_Promo  (final catch-all)
        # If the SMS has not been routed to any data table above,
        # it goes here (advisory / promo / uncategorised).
        # ────────────────────────────────────────────────────────────
        _placed_elsewhere = (
            sum(len(v) for k, v in rows.items()
                if k != "Advisory_Promo") > _placed_before
        )
        is_alert_label = (
            str(r.rule_label) in set(_alert_labels.get(fam, set()))
        )
        is_advisory = (
            "ADVISORY" in c or "PROMO" in c or "UNSOLICITED" in c
        )

        if not _placed_elsewhere and (
            is_advisory or is_alert_label
            or r.source_classifier == "UNCATEGORISED"
        ):
            # Sub-classify the catch-all bucket
            is_security_sms = _TPIN_BODY_RE.search(str(text))
            if is_security_sms:
                typ = "security"
                topic = "tpin_credential"
            elif "ADVISORY" in c or "UNSOLICITED" in c:
                typ = "regulatory_advisory"
                topic = "tips_warning"
            elif "PROMO" in c:
                typ = "promo"
                topic = "app_promo"
            elif is_alert_label:
                typ = "alert"
                topic = np.nan
            else:
                typ = "uncategorised"
                topic = np.nan

            promo_broker = (
                b if pd.notna(b)
                else (dep if pd.notna(dep)
                      else (fam if fam else ex))
            )
            add(rows, "Advisory_Promo", **base,
                msg_type=typ, broker_or_exchange=promo_broker,
                topic=topic, date=dt)

    # ── Convert row-lists into DataFrames ──
    return {
        name: pd.DataFrame(data, columns=TABLE_COLUMNS[name])
        for name, data in rows.items()
    }


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 10 — ENSURE ALL CLIENTS IN BROKER TABLE                          ║
# ║                                                                            ║
# ║  Scan all tables: if a (client_id, broker_name) pair appears anywhere but  ║
# ║  not in Broker_Associations, add an inferred row there.                    ║
# ║  Also remove rows where both broker_name AND full_sms are empty.           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


def ensure_all_clients_in_broker_table(tables):
    """Make sure every (client_id, broker_name) pair that appears in any
    output table is represented at least once in Broker_Associations.
    — change when required (adjust the dedup logic)
    """
    target = "Broker_Associations"
    t01b = tables[target]
    cols = TABLE_COLUMNS[target]

    # Collect existing (client, broker) pairs
    existing_pairs = set()
    existing_clients = set()
    if not t01b.empty:
        for _, row in t01b[["client_id", "broker_name"]].iterrows():
            cid = row["client_id"]
            if pd.notna(cid):
                existing_clients.add(str(cid))
                if pd.notna(row["broker_name"]):
                    existing_pairs.add((str(cid), str(row["broker_name"])))

    new_rows = []
    seen_new_pairs = set()
    seen_new_clients = set()

    for name, df in tables.items():
        if name == target or df.empty or "client_id" not in df.columns:
            continue
        has_broker = "broker_name" in df.columns
        has_sms = "full_sms" in df.columns

        for _, row in df.iterrows():
            cid = row.get("client_id")
            if pd.isna(cid):
                continue
            cid = str(cid)
            bn = (row.get("broker_name", np.nan)
                  if has_broker else np.nan)

            # Add missing (client, broker) pair
            if pd.notna(bn):
                key = (cid, str(bn))
                if key not in existing_pairs and key not in seen_new_pairs:
                    sms_val = (row.get("full_sms", np.nan)
                               if has_sms else np.nan)
                    new_row = {c: np.nan for c in cols}
                    new_row["client_id"] = cid
                    new_row["broker_name"] = bn
                    new_row["exchange"] = row.get("exchange", np.nan)
                    new_row["source_classifier"] = row.get(
                        "source_classifier", "inferred")
                    new_row["source_sms_id"] = row.get(
                        "source_sms_id", np.nan)
                    new_row["event_timestamp"] = row.get(
                        "event_timestamp", pd.NaT)
                    new_row["full_sms"] = sms_val
                    new_rows.append(new_row)
                    seen_new_pairs.add(key)
                    seen_new_clients.add(cid)

            # Add missing client (no broker yet)
            if (cid not in existing_clients
                    and cid not in seen_new_clients):
                sms_val = (row.get("full_sms", np.nan)
                           if has_sms else np.nan)
                bn_val = (row.get("broker_name", np.nan)
                          if has_broker else np.nan)
                # Skip if both broker and SMS are empty (no useful data)
                if pd.isna(bn_val) and (
                    pd.isna(sms_val)
                    or not str(sms_val).strip()
                ):
                    seen_new_clients.add(cid)
                    continue
                new_row = {c: np.nan for c in cols}
                new_row["client_id"] = cid
                new_row["broker_name"] = bn_val
                new_row["source_classifier"] = row.get(
                    "source_classifier", "inferred")
                new_row["source_sms_id"] = row.get(
                    "source_sms_id", np.nan)
                new_row["event_timestamp"] = row.get(
                    "event_timestamp", pd.NaT)
                new_row["full_sms"] = sms_val
                new_rows.append(new_row)
                seen_new_clients.add(cid)

    # Append inferred rows
    if new_rows:
        tables[target] = pd.concat(
            [t01b, pd.DataFrame(new_rows, columns=cols)],
            ignore_index=True)

    # Remove rows where both broker_name AND full_sms are empty
    df01 = tables[target]
    mask_broker = (df01["broker_name"].isna()
                   | (df01["broker_name"].astype(str).str.strip() == ""))
    mask_sms = (df01["full_sms"].isna()
                | (df01["full_sms"].astype(str).str.strip() == ""))
    tables[target] = df01[~(mask_broker & mask_sms)].reset_index(drop=True)

    return tables


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 11 — MAIN RUNNER                                                 ║
# ║                                                                            ║
# ║  Reads the input CSV, classifies every SMS, builds all tables,             ║
# ║  and writes ONE Excel workbook containing every sheet.                     ║
# ║                                                                            ║
# ║  — change when required: swap file paths, add/remove QA sheets.           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝


def run(input_csv, output_dir):
    """End-to-end pipeline: read → classify → build tables → write Excel.

    Parameters
    ----------
    input_csv  : str or Path — path to the raw SMS CSV file.
    output_dir : str or Path — folder where the output workbook is saved.

    Returns
    -------
    tables : dict[str, pd.DataFrame]
    master : pd.DataFrame  (the enriched per-SMS master table)
    """
    if not RULES:
        raise ValueError("RULES dict is empty — nothing to classify.")

    t_start = time.time()
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # ── 1. Read CSV header to detect column names ──
    header = pd.read_csv(input_csv, nrows=0)
    stripped_to_raw = {str(c).strip(): c for c in header.columns}
    header.columns = list(stripped_to_raw.keys())

    # Identify the client-ID column (needed for dtype hint)
    client_raw_col = stripped_to_raw[
        pick_column(header, BASE_HINTS["client_id"])
    ]

    # ── 2. Read full CSV ──
    raw = pd.read_csv(
        input_csv, low_memory=False,
        dtype={client_raw_col: "string"},  # keep client codes as strings
    )
    raw.columns = [str(c).strip() for c in raw.columns]

    # Resolve column names
    client = pick_column(raw, BASE_HINTS["client_id"])
    msg    = pick_column(raw, BASE_HINTS["message"])
    sms_id = pick_column(raw, BASE_HINTS["sms_id"], False)
    ts     = pick_column(raw, BASE_HINTS["event_time"], False)

    # ── 3. Classify every SMS ──
    rule_index = build_rule_index(RULES)
    print(f"Classifying {len(raw):,} SMS "
          f"({os.cpu_count() or 1} cores available) ...")

    t0 = time.time()
    texts = [clean_text(v) for v in raw[msg].tolist()]
    classified = classify_all(
        texts, rule_index, use_mp=USE_MULTIPROCESSING
    )
    print(f"  classification done in {time.time() - t0:.1f}s")

    # ── 4. Build the enriched master DataFrame ──

    def _clean_cid(v):
        """Normalise client IDs: strip whitespace, convert float → int."""
        if pd.isna(v):
            return np.nan
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        s = str(v).strip()
        return s if s else np.nan

    client_ids = [_clean_cid(v) for v in raw[client].tolist()]
    sms_ids = (raw[sms_id].astype(str).tolist()
               if sms_id else [np.nan] * len(raw))
    timestamps = (
        pd.to_datetime(raw[ts], errors="coerce", utc=True).tolist()
        if ts else [pd.NaT] * len(raw)
    )

    rec = []
    for i in range(len(raw)):
        cls, fam, fields, hits = classified[i]
        rec.append({
            "client_id":           client_ids[i],
            "source_sms_id":       sms_ids[i],
            "event_timestamp":     timestamps[i],
            "sms_message":         texts[i],
            "source_classifier":   cls,
            "classifier_family":   fam,
            "rule_label":          cls.split("_", 1)[1] if "_" in cls else "",
            "fields":              fields,       # kept for build_tables()
            "all_classifier_hits": " | ".join(hits),
            "classifier_hit_count": len(hits),
        })
    master = pd.DataFrame(rec)

    # Strip timezone from timestamps so Excel can handle them
    if "event_timestamp" in master.columns:
        master["event_timestamp"] = (
            pd.to_datetime(master["event_timestamp"]).dt.tz_localize(None)
        )

    # Determine the broker for every SMS
    master["broker_name"] = [
        broker_from_sms(t, f)
        for t, f in zip(master["sms_message"], master["fields"])
    ]

    # ── 5. Build all output tables ──
    print("Building normalised tables ...")
    t1 = time.time()
    tables = build_tables(master)
    print(f"  table build done in {time.time() - t1:.1f}s")

    # ── 6. Ensure every client+broker appears in Broker_Associations ──
    print("Cross-checking Broker_Associations for completeness ...")
    tables = ensure_all_clients_in_broker_table(tables)

    # Sort Broker_Associations for readability
    if not tables["Broker_Associations"].empty:
        tables["Broker_Associations"] = (
            tables["Broker_Associations"]
            .sort_values(
                ["client_id", "broker_name", "event_timestamp"],
                na_position="last")
            .reset_index(drop=True)
        )

    # ── 7. Prepare QA helper DataFrames ──
    # Row-count summary
    row_summary = pd.DataFrame([
        {"table": k, "rows": len(v)} for k, v in tables.items()
    ])

    # SMS that matched multiple rules (investigate overlaps)
    qa_multi = master.loc[
        master.classifier_hit_count > 1,
        ["client_id", "source_sms_id", "sms_message",
         "source_classifier", "all_classifier_hits"],
    ].copy()

    # SMS that matched NO rule (candidates for new rules)
    qa_uncat = master.loc[
        master.source_classifier.eq("UNCATEGORISED"),
        ["client_id", "source_sms_id", "sms_message"],
    ].copy()

    # Enriched master without the heavyweight 'fields' dict column
    master_export = master.drop(columns="fields")

    # ── 8. Write SINGLE Excel workbook with all sheets ──
    #       — change when required (rename the file, add/remove sheets)
    workbook_path = out / "sms_warehouse.xlsx"
    print(f"Writing workbook → {workbook_path} ...")

    with pd.ExcelWriter(
        workbook_path,
        engine="openpyxl",
        datetime_format="yyyy-mm-dd hh:mm:ss",
    ) as writer:

        # (a) Enriched master (every SMS + its classifier + parsed values)
        master_export.to_excel(
            writer, sheet_name="Enriched_Master", index=False
        )

        # (b) All normalised data tables
        for name, df in tables.items():
            # Excel sheet names are limited to 31 characters
            df.to_excel(writer, sheet_name=name[:31], index=False)

        # (c) QA / summary helper sheets
        row_summary.to_excel(
            writer, sheet_name="Row_Summary", index=False
        )
        qa_multi.to_excel(
            writer, sheet_name="Multi_Rule_Hits", index=False
        )
        qa_uncat.to_excel(
            writer, sheet_name="Uncategorised", index=False
        )

    print(f"Done — {len(tables)} data sheets + 4 helper sheets")
    print(f"Total elapsed: {time.time() - t_start:.1f}s")

    return tables, master


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SECTION 12 — ENTRY POINT                                                 ║
# ║                                                                            ║
# ║  Set INPUT_CSV and OUTPUT_DIR below, then run this file directly.          ║
# ║  — change when required: update file paths for your environment.           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# Path to the raw SMS CSV file
INPUT_CSV = (
    r"C:\Users\Nimisha_Jain\Downloads"
    r"\part-00000-tid-6777259446022346870-93b63f41-8ab4-4ce1-b5c4-"
    r"066888431dd2-5994-1-c000.csv"
)

# Folder where the output workbook will be saved
OUTPUT_DIR = r"C:\Users\Nimisha_Jain\Downloads\sms_warehouse_output"

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    run(INPUT_CSV, OUTPUT_DIR)

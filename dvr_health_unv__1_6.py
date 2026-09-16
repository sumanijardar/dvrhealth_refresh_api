# -*- coding: utf-8 -*-
"""
UNV / Uniview DVR Health Check & Refresh API  —  Production Version
---------------------------------------------------------------------
FEATURES:
 1. Dual Mode: Main 2-Hour Loop + Background FastAPI Refresh Server (Port 8003).
 2. Single Command: Run 'python script.py' to launch both simultaneously.
 3. Strict Port 8003: Fixed port for Frontend integration (no auto-port hopping).
 4. Resilient Execution: FastAPI stays active even if the main loop crashes/sleeps.
 5. High Performance: Parallel snapshots (~8s), early returns on ping failure, DB connection pool.
 6. Single DB Transaction: Atomically updates all_dvr_live + site_dvr_data + dvr_history.
"""

import os
import re
import sys
import json
import time
import platform
import threading
import subprocess
import concurrent.futures
from datetime import datetime, timedelta, timezone

import requests
from requests.auth import HTTPDigestAuth

import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "dvrhealth"))

try:
    from ping3 import ping
except ImportError:
    ping = None

try:
    import cv2
    import numpy as np
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False

from dvr_api_logger import (
    init_api_logging,
    APIResponseLogger,
    get_human_readable_status,
)

init_api_logging()

# ==================================================================== #
#                          CONFIGURATION                               #
# ==================================================================== #

DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "esurv"

IMG_DIRS = [
    os.path.join("C:", os.sep, "xampp", "htdocs", "unified", "dvrimages"),
    os.path.join("D:", os.sep, "dvrimages"),
]
IMG_PATH_DB_PREFIX = "/dvrimages"      # Prefix stored in the DB

MAX_WORKERS        = 40
DB_MAX_CONNECTIONS = 80
DEFAULT_PORT       = 81
DEFAULT_CAMS       = 4
CYCLE_SLEEP_SEC    = 7200

# Timeouts
T_PING      = 3
T_LOGIN     = (4, 6)
T_API       = (4, 8)
T_SNAPSHOT  = (4, 8)
T_RECORD    = (5, 12)

REC_MAX_MONTHS = 6

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

CHANNEL_NAMES = {1: "Lobby", 2: "Backroom", 3: "Outdoor", 4: "Pinhole"}

# ==================================================================== #
#                       POOL / LOCKS / GLOBALS                         #
# ==================================================================== #

pool = PooledDB(
    creator=pymysql,
    maxconnections=DB_MAX_CONNECTIONS,
    mincached=5,
    maxcached=10,
    maxshared=0,
    blocking=True,
    maxusage=None,
    ping=1,
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    charset="utf8mb4",
    cursorclass=DictCursor,
    connect_timeout=10,
    read_timeout=30,
    write_timeout=30,
)

print_lock = threading.Lock()
_lfd_lock = threading.Lock()
last_fetched_dates = {}


def safe_print(msg):
    with print_lock:
        print(msg, flush=True)


def db_execute(sql, params=None, fetch=False, many=False):
    """Single statement; connection always returned to the pool."""
    conn = None
    try:
        conn = pool.connection()
        with conn.cursor() as cur:
            if many:
                cur.executemany(sql, params or [])
            else:
                cur.execute(sql, params or ())
            if fetch:
                return cur.fetchall()
            conn.commit()
            return cur.rowcount
    except Exception as e:
        safe_print(f"[DB ERROR] {e} | {sql[:100]}")
        return [] if fetch else 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ==================================================================== #
#                          SITE FETCHING                               #
# ==================================================================== #

def get_dvr_sites():
    sql = """
        SELECT SN AS sn, ATMID AS atm_id, IPAddress AS ip, port,
               UserName AS username, Password AS password, dvrname AS dvr_name,
               no_of_cam, cam_type, capacity, freespace,
               recording_from, recording_to, cdate, last_communication
        FROM all_dvr_live
        WHERE UPPER(dvrname) IN ('UNV', 'UNIVIEW') AND live = 'Y'
    """
    rows = db_execute(sql, fetch=True)
    sites = []
    for r in rows:
        try:
            port = int(r.get("port") or DEFAULT_PORT)
        except Exception:
            port = DEFAULT_PORT
        try:
            no_of_cam = int(r.get("no_of_cam") or DEFAULT_CAMS)
        except Exception:
            no_of_cam = DEFAULT_CAMS
        no_of_cam = max(1, min(no_of_cam, 32))

        sites.append({
            "sn": r.get("sn"),
            "atm_id": str(r.get("atm_id") or "").strip(),
            "ip": str(r.get("ip") or "").strip(),
            "port": port,
            "username": str(r.get("username") or "admin").strip(),
            "password": str(r.get("password") or "").strip(),
            "dvr_name": str(r.get("dvr_name") or "UNV").strip(),
            "no_of_cam": no_of_cam,
            "cam_type": str(r.get("cam_type") or "").strip(),
            "capacity": str(r.get("capacity") or "").strip(),
            "freespace": str(r.get("freespace") or "").strip(),
            "recording_from": str(r.get("recording_from") or "").strip(),
            "recording_to": str(r.get("recording_to") or "").strip(),
            "cdate": str(r.get("cdate") or "").strip(),
            "last_communication": r.get("last_communication"),
        })
    safe_print(f"📋 Loaded {len(sites)} UNV sites from esurv.all_dvr_live.")
    return sites


# ==================================================================== #
#                          PING                                        #
# ==================================================================== #

def ping_host(ip):
    if ping is not None:
        try:
            return bool(ping(ip, timeout=T_PING))
        except Exception:
            return False
    try:
        if platform.system().lower() == "windows":
            cmd = ["ping", "-n", "1", "-w", str(T_PING * 1000), ip]
        else:
            cmd = ["ping", "-c", "1", "-W", str(T_PING), ip]
        return subprocess.run(cmd, capture_output=True, timeout=T_PING + 2).returncode == 0
    except Exception:
        return False


# ==================================================================== #
#                      IMAGE VALIDATION                                #
# ==================================================================== #

def is_valid_snapshot(content):
    """Size check + optional black-frame check."""
    if not content or len(content) < 500:
        return False
    if not HAS_CV2:
        return True
    try:
        arr = np.frombuffer(content, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return False
        black_ratio = float(np.sum(img < 30)) / img.size
        return black_ratio <= 0.85
    except Exception:
        return True


# ==================================================================== #
#                          UNV CLIENT                                  #
# ==================================================================== #

class UNVClient:
    def __init__(self, ip, port, username, password, atm_id, api_logger):
        self.ip = ip
        self.port = port
        self.base = f"http://{ip}:{port}/LAPI/V1.0"
        self.api_logger = api_logger
        self.session = requests.Session()
        self.session.auth = HTTPDigestAuth(username, password)
        self.session.headers.update(HEADERS)

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def _log(self, ep, code, body="", err=""):
        try:
            self.api_logger.add_api_call(f"/LAPI/V1.0{ep}", "GET", code, body, err)
        except Exception:
            pass

    def _get(self, ep, timeout=T_API, log=True):
        try:
            r = self.session.get(f"{self.base}{ep}", timeout=timeout)
            if log:
                self._log(ep, r.status_code, (r.text or "")[:200])
            return r
        except requests.exceptions.RequestException as e:
            if log:
                self._log(ep, 0, "", str(e))
            return None

    # -------- Login --------
    def login(self):
        r = self._get("/System/DeviceInfo", timeout=T_LOGIN)
        code = r.status_code if r is not None else 0
        return code == 200, code

    # -------- Time --------
    def get_time(self):
        r = self._get("/System/Time")
        if r is None or r.status_code != 200:
            return None
        try:
            d = r.json()["Response"]["Data"]
            ts = int(d["DeviceTime"])
            tz_str = str(d.get("TimeZone", "GMT+05:30"))
            m = re.search(r"([+-])(\d{1,2}):?(\d{2})", tz_str)
            if m:
                sign = 1 if m.group(1) == "+" else -1
                off = timedelta(hours=int(m.group(2)), minutes=int(m.group(3))) * sign
                return datetime.fromtimestamp(ts, tz=timezone(off))
            return datetime.fromtimestamp(ts)
        except Exception as e:
            safe_print(f"[time parse] {self.ip}: {e}")
            return None

    # -------- HDD --------
    def get_hdd(self):
        """returns (ok, total_mb, used_mb, free_mb, details)"""
        r = self._get("/Storage/Containers/DetailInfos")
        if r is None or r.status_code != 200:
            return False, 0, 0, 0, []
        try:
            hdds = r.json()["Response"]["Data"].get("LocalHDDList", []) or []
            total = used = 0
            ok = False
            details = []
            for h in hdds:
                cap = int(h.get("TotalCapacity", 0) or 0)
                use = int(h.get("UsedCapacity", 0) or 0)
                good = (h.get("Status") == 3 and cap > 0)
                ok = ok or good
                total += cap
                used += use
                details.append(f"HDD {h.get('ID')}: {'Working' if good else 'Not Working'}")
            return ok, total, used, max(0, total - used), details
        except Exception:
            return False, 0, 0, 0, []

    # -------- Camera status --------
    def get_camera_status(self, no_of_cam):
        cams = {i: "Not Working" for i in range(1, no_of_cam + 1)}
        for ep in ("/Channels/System/ChannelDetailInfos",
                   "/Channels/System/ChannelSummaryInfos",
                   "/Video/Inputs/Channels"):
            r = self._get(ep)
            if r is None or r.status_code != 200:
                continue
            try:
                data = r.json().get("Response", {}).get("Data", {})
                lst = (data.get("DetailInfos") or data.get("SummaryInfos")
                       or data.get("ChannelList") or data.get("Channel") or [])
                if not isinstance(lst, list) or not lst:
                    continue
                for idx, c in enumerate(lst, start=1):
                    if not isinstance(c, dict):
                        continue
                    try:
                        cid = int(c.get("ID") or c.get("ChannelID") or c.get("id") or idx)
                        if 100 < cid <= 132:
                            cid -= 100
                    except Exception:
                        cid = idx
                    raw = c.get("Status", c.get("OnlineStatus"))
                    st = "Working" if (raw in (1, "1") or str(raw).lower() in ("online", "working", "ok")) else "Not Working"
                    desc = str(c.get("ResDesc") or c.get("Name") or c.get("ChannelName") or "").upper()
                    if "NO VIDEO" in desc or "OFFLINE" in desc:
                        st = "Not Working"
                    if cid in cams:
                        cams[cid] = st
                return cams
            except Exception:
                continue
        return cams

    # -------- Snapshots (parallel) --------
    def capture_snapshots(self, no_of_cam):
        """returns {ch: True/False}"""
        dirs = [os.path.join(d, self.ip) for d in IMG_DIRS]
        for d in dirs:
            try:
                os.makedirs(d, exist_ok=True)
            except Exception:
                pass

        def grab(ch):
            ep = f"/Channels/{ch}/Media/Video/Streams/0/Snapshot"
            s = requests.Session()
            s.auth = self.session.auth
            s.headers.update(HEADERS)
            try:
                r = s.get(f"{self.base}{ep}", timeout=T_SNAPSHOT)
                if r.status_code == 200 and is_valid_snapshot(r.content):
                    for d in dirs:
                        try:
                            with open(os.path.join(d, f"{ch}.jpg"), "wb") as f:
                                f.write(r.content)
                        except Exception:
                            pass
                    self._log(ep, 200, f"Image Bytes: {len(r.content)}")
                    return ch, True
                self._log(ep, r.status_code, "", "Empty/black image" if r.status_code == 200 else "")
                return ch, False
            except Exception as e:
                self._log(ep, 0, "", str(e))
                return ch, False
            finally:
                s.close()

        out = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, no_of_cam)) as ex:
            for ch, ok in ex.map(grab, range(1, no_of_cam + 1)):
                out[ch] = ok
        return out

    # -------- Recording --------
    @staticmethod
    def _parse_times(text):
        pairs = []
        try:
            recs = json.loads(text).get("Response", {}).get("Data", {}).get("RecordList", []) or []
            for rec in recs:
                b = rec.get("BeginTime") or rec.get("Begin")
                e = rec.get("EndTime") or rec.get("End") or b
                if b:
                    pairs.append((int(b), int(e)))
            if pairs:
                return pairs
        except Exception:
            pass
        bs = [int(x) for x in re.findall(r'"?Begin(?:Time)?"?\s*:\s*(\d+)', text)]
        es = [int(x) for x in re.findall(r'"?End(?:Time)?"?\s*:\s*(\d+)', text)]
        for i, b in enumerate(bs):
            pairs.append((b, es[i] if i < len(es) else b))
        return pairs

    def _records(self, begin, end, channel=1, stream=0):
        ep = f"/Channels/{channel}/Media/Video/Streams/{stream}/Records?Begin={begin}&End={end}"
        r = self._get(ep, timeout=T_RECORD, log=False)
        if r is None or r.status_code != 200:
            return []
        return self._parse_times(r.text or "")

    def recording_start(self):
        now = int(time.time())
        step = 30 * 86400
        for i in range(REC_MAX_MONTHS - 1, -1, -1):
            pairs = self._records(now - step * (i + 1), now - step * i)
            if pairs:
                return min(b for b, _ in pairs)
        return None

    def recording_end(self):
        now = int(time.time())
        for span in (3600, 6 * 3600, 86400, 3 * 86400, 7 * 86400):
            pairs = self._records(now - span, now)
            if pairs:
                return max(e for _, e in pairs)
        return None


# ==================================================================== #
#                     site_dvr_data SYNC                               #
# ==================================================================== #

def sync_site_dvr_data(cur, site, results, now_str):
    ip = site["ip"]
    no_of_cam = site["no_of_cam"]
    login_ok = results["login_ok"]
    cams = results.get("cam_statuses", {}) or {}
    snaps = results.get("snapshots", {}) or {}
    img_path = f"{IMG_PATH_DB_PREFIX}/{ip}"

    cur.execute("SELECT channel FROM site_dvr_data WHERE IPAddress=%s", (ip,))
    existing = set()
    for r in cur.fetchall():
        try:
            existing.add(int(r["channel"]))
        except Exception:
            pass

    # Insert missing channels
    missing = [ch for ch in range(1, no_of_cam + 1) if ch not in existing]
    if missing:
        ins = """
            INSERT INTO site_dvr_data
                (SN, IPAddress, dvrname, channel, channel_name, cam_type,
                 UserName, Password, port, is_working, img_path, last_communication)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        vals = []
        for ch in missing:
            working = login_ok and (snaps.get(ch) or cams.get(ch) == "Working")
            vals.append((
                site["sn"], ip, site["dvr_name"], ch,
                CHANNEL_NAMES.get(ch, f"Channel {ch}"), site["cam_type"],
                site["username"], site["password"], site["port"],
                "1" if working else "0",
                img_path if (working and snaps.get(ch)) else None,
                now_str if working else None,
            ))
        cur.executemany(ins, vals)

    # Login failed -> is_working=0
    if not login_ok:
        cur.execute("UPDATE site_dvr_data SET is_working='0' WHERE IPAddress=%s", (ip,))
        return

    # Update existing channels
    working_with_img = [ch for ch in existing if snaps.get(ch)]
    working_no_img = [ch for ch in existing if not snaps.get(ch) and cams.get(ch) == "Working"]
    not_working = [ch for ch in existing if ch not in working_with_img and ch not in working_no_img]

    if working_with_img:
        ph = ",".join(["%s"] * len(working_with_img))
        cur.execute(
            f"UPDATE site_dvr_data SET is_working='1', img_path=%s, last_communication=%s "
            f"WHERE IPAddress=%s AND channel IN ({ph})",
            [img_path, now_str, ip] + working_with_img,
        )
    if working_no_img:
        ph = ",".join(["%s"] * len(working_no_img))
        cur.execute(
            f"UPDATE site_dvr_data SET is_working='1', last_communication=%s "
            f"WHERE IPAddress=%s AND channel IN ({ph})",
            [now_str, ip] + working_no_img,
        )
    if not_working:
        ph = ",".join(["%s"] * len(not_working))
        cur.execute(
            f"UPDATE site_dvr_data SET is_working='0' "
            f"WHERE IPAddress=%s AND channel IN ({ph})",
            [ip] + not_working,
        )


# ==================================================================== #
#                       DB UPDATE (Single Transaction)                 #
# ==================================================================== #

def update_database(results, site):
    ip = site["ip"]
    conn = None
    try:
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        ping_ok = results["ping_ok"]
        login_ok = results["login_ok"]
        hdd_ok = results["hdd_ok"]
        cams = results.get("cam_statuses", {}) or {}

        dvr_dt = results.get("dvr_time")
        dvr_time_str = dvr_dt.strftime("%Y-%m-%d %H:%M:%S") if dvr_dt else None
        time_diff = ""
        if dvr_time_str:
            try:
                c = now.replace(microsecond=0)
                s = datetime.strptime(dvr_time_str, "%Y-%m-%d %H:%M:%S")
                time_diff = str(c - s) if c >= s else f"+{s - c}"
            except Exception:
                time_diff = ""

        if login_ok:
            last_comm = now_str
        else:
            lc = site.get("last_communication")
            last_comm = lc.strftime("%Y-%m-%d %H:%M:%S") if isinstance(lc, datetime) else (str(lc) if lc else None)

        def cam(i):
            if not login_ok:
                return "N/A"
            return cams.get(i, "Not Working")

        data = {
            "atmid": site["atm_id"],
            "ip": ip,
            "status": "1" if ping_ok else "0",
            "login_status": "0" if login_ok else "1",
            "network_status": "working" if ping_ok else "not working",
            "cam1": cam(1), "cam2": cam(2), "cam3": cam(3), "cam4": cam(4),
            "hdd": "Working" if hdd_ok else ("N/A" if not login_ok else "Not Working"),
            "capacity": str(results["hdd_total_mb"] if login_ok else 0),
            "freespace": str(results["hdd_free_mb"] if login_ok else 0),
            "recording_from": results["rec_start_str"] if login_ok else "",
            "recording_to": results["rec_end_str"] if login_ok else "",
            "dvr_time": dvr_time_str or "",
            "time_diff": time_diff,
            "last_communication": last_comm,
            "cdate": now_str,
            "latency": "",
        }

        conn = pool.connection()
        with conn.cursor() as cur:
            # 1. all_dvr_live
            cur.execute("""
                UPDATE all_dvr_live SET
                    status=%(status)s, login_status=%(login_status)s,
                    network_status=%(network_status)s, network_status_dt=%(cdate)s,
                    cam1=%(cam1)s, cam2=%(cam2)s, cam3=%(cam3)s, cam4=%(cam4)s,
                    hdd=%(hdd)s, capacity=%(capacity)s, freespace=%(freespace)s,
                    recording_from=%(recording_from)s, recording_to=%(recording_to)s,
                    dvr_time=%(dvr_time)s, time_diff=%(time_diff)s,
                    cdate=%(cdate)s, cdate1=%(cdate)s,
                    last_communication=%(last_communication)s
                WHERE IPAddress=%(ip)s
            """, data)

            # 2. site_dvr_data
            try:
                sync_site_dvr_data(cur, site, results, now_str)
            except Exception as e:
                safe_print(f"[site_dvr_data] {ip}: {e}")

            # 3. dvr_history
            cur.execute("""
                INSERT INTO dvr_history
                    (ip, status, cam1, cam2, cam3, cam4, hdd, latency, cdate, login_status,
                     last_communication, atmid, capacity, freespace, recording_from, recording_to)
                VALUES
                    (%(ip)s, %(status)s, %(cam1)s, %(cam2)s, %(cam3)s, %(cam4)s, %(hdd)s,
                     %(latency)s, %(cdate)s, %(login_status)s, %(last_communication)s,
                     %(atmid)s, %(capacity)s, %(freespace)s, %(recording_from)s, %(recording_to)s)
            """, data)

        conn.commit()
    except Exception as e:
        safe_print(f"❌ Database Update Error [{ip}]: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ==================================================================== #
#                        PROCESS SINGLE SITE                           #
# ==================================================================== #

def _to_int(s):
    try:
        return int(re.sub(r"[^\d]", "", str(s))) if s else 0
    except Exception:
        return 0


def build_card(site, results, skip_api):
    ip, atm = site["ip"], site["atm_id"]
    c = ["═" * 70,
         f"🖥️  UNV HEALTH CHECK | IP: {ip:<15} | ATM ID: {atm or 'N/A'}",
         "─" * 70,
         f"  📶 Ping       : {'✅ Working' if results['ping_ok'] else '❌ Not Working'}",
         f"  🔑 Login      : {'✅ Working (0)' if results['login_ok'] else '❌ Not Working (1)'}"]
    if results["login_ok"]:
        sk = " [Cached from DB]" if skip_api else ""
        dt = results["dvr_time"].strftime("%Y-%m-%d %H:%M:%S") if results["dvr_time"] else "N/A"
        c.append(f"  ⏰ DVR Time   : {dt}")
        c.append(f"  💾 HDD        : {'✅ Working' if results['hdd_ok'] else '❌ Not Working'} "
                 f"(Total: {results['hdd_total_mb']:,} MB | Free: {results['hdd_free_mb']:,} MB){sk}")
        c.append("  📹 Cameras    :")
        cams = results["cam_statuses"]
        snaps = results.get("snapshots", {})
        keys = sorted(cams)
        for i in keys:
            icon = "✅" if cams[i] == "Working" else "❌"
            img = " 📸" if snaps.get(i) else ""
            c.append(f"     {'└─' if i == keys[-1] else '├─'} Cam {i}   : {icon} {cams[i]}{img}")
        if results["rec_ok"]:
            c.append(f"  🎥 Recording  : ✅ Working ({results['rec_start_str']} to {results['rec_end_str']}){sk}")
        else:
            c.append(f"  🎥 Recording  : ❌ Not Working{sk}")
    elif not results["ping_ok"]:
        c.append("  ⏭️  APIs       : Skipped (ping failed)")
    c += ["─" * 70, "  💾 Database   : ✅ Updated", "═" * 70]
    return "\n".join(c)


def process_unv_site(site):
    ip = site["ip"]
    atm_id = site["atm_id"]
    no_of_cam = site["no_of_cam"]
    today = datetime.now().strftime("%Y-%m-%d")

    already_today = (bool(site["recording_from"]) and bool(site["recording_to"])
                     and bool(site["capacity"]) and site["cdate"].startswith(today))
    with _lfd_lock:
        skip_api = (last_fetched_dates.get(ip) == today) or already_today

    api_logger = APIResponseLogger(ip, site["port"], "UNV", atm_id)

    results = {
        "ping_ok": False, "login_ok": False, "http_status": 0,
        "dvr_time": None,
        "hdd_ok": False, "hdd_total_mb": 0, "hdd_used_mb": 0, "hdd_free_mb": 0,
        "cam_statuses": {i: "Not Working" for i in range(1, no_of_cam + 1)},
        "snapshots": {},
        "rec_ok": False, "rec_start_str": "", "rec_end_str": "",
        "skip_api": skip_api,
    }

    client = None
    try:
        # 1. Ping
        results["ping_ok"] = ping_host(ip)
        if not results["ping_ok"]:
            api_logger.add_api_call("/ping", "ICMP", 0, "", "Ping failed")
            return results

        client = UNVClient(ip, site["port"], site["username"], site["password"], atm_id, api_logger)

        # 2. Login
        results["login_ok"], results["http_status"] = client.login()
        if not results["login_ok"]:
            return results

        # 3. Time
        results["dvr_time"] = client.get_time()

        # 4. HDD
        if skip_api:
            total = _to_int(site["capacity"])
            free = _to_int(site["freespace"])
            results.update(hdd_ok=total > 0, hdd_total_mb=total,
                           hdd_used_mb=max(0, total - free), hdd_free_mb=free)
            api_logger.add_api_call("/HDD/Storage", "CACHE", 200, "", "HDD API skipped (DB values)")
        else:
            ok, total, used, free, _ = client.get_hdd()
            results.update(hdd_ok=ok, hdd_total_mb=total, hdd_used_mb=used, hdd_free_mb=free)

        # 5. Snapshots
        snaps = client.capture_snapshots(no_of_cam)
        results["snapshots"] = snaps
        for ch, ok in snaps.items():
            if ok:
                results["cam_statuses"][ch] = "Working"

        # 6. Camera Status API Fallback
        if not all(snaps.values()):
            api_cams = client.get_camera_status(no_of_cam)
            for ch, st in api_cams.items():
                if not snaps.get(ch):
                    results["cam_statuses"][ch] = st

        # 7. Recording
        if skip_api:
            results["rec_start_str"] = site["recording_from"]
            results["rec_end_str"] = site["recording_to"]
            results["rec_ok"] = bool(results["rec_start_str"] and results["rec_end_str"])
            api_logger.add_api_call("/Media/Records", "CACHE", 200, "", "Recording API skipped (DB values)")
        else:
            rs = client.recording_start()
            re_ = client.recording_end()
            api_logger.add_api_call("/LAPI/V1.0/Channels/1/Media/Video/Streams/0/Records", "GET",
                                    200 if (rs and re_) else 0, "",
                                    "Success" if (rs and re_) else "No recordings")
            if rs and re_:
                results["rec_ok"] = True
                results["rec_start_str"] = datetime.fromtimestamp(rs).strftime("%Y-%m-%d")
                results["rec_end_str"] = datetime.fromtimestamp(re_).strftime("%Y-%m-%d")

        if not skip_api:
            with _lfd_lock:
                last_fetched_dates[ip] = today

        return results

    except Exception as e:
        safe_print(f"❌ Process error {ip}: {e}")
        return results
    finally:
        if client:
            client.close()
        try:
            api_logger.save_to_database()
        except Exception as e:
            safe_print(f"[api_logger] {ip}: {e}")
        update_database(results, site)
        safe_print(build_card(site, results, skip_api))


# ==================================================================== #
#                             MAIN LOOP                                #
# ==================================================================== #

def main():
    while True:
        start = datetime.now()
        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        safe_print("\n" + "█" * 70)
        safe_print(f"🚀 SCHEDULED UNV DVR HEALTH CHECK STARTED AT {start_str}")
        safe_print("█" * 70 + "\n")

        sites = get_dvr_sites()
        if not sites:
            safe_print("⚠️ No active UNV sites found.")
        else:
            safe_print(f"📋 {len(sites)} sites | {MAX_WORKERS} threads\n")
            succ = fail = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = {ex.submit(process_unv_site, s): s for s in sites}
                for f in concurrent.futures.as_completed(futs):
                    try:
                        r = f.result()
                        if r and r.get("login_ok"):
                            succ += 1
                        else:
                            fail += 1
                    except Exception as e:
                        fail += 1
                        safe_print(f"❌ Error: {e}")

            end = datetime.now()
            dur = round((end - start).total_seconds(), 1)
            safe_print("\n".join([
                "\n" + "═" * 70,
                "📊 RUN CYCLE SUMMARY — UNV DVR HEALTH CHECK",
                "═" * 70,
                f"  ⏱️  Started At     : {start_str}",
                f"  ⏱️  Finished At    : {end.strftime('%Y-%m-%d %H:%M:%S')} "
                f"(Duration: {dur}s / {round(dur / 60, 1)} min)",
                f"  📍 Total Sites    : {len(sites)}",
                f"  🟢 Login Success  : {succ}",
                f"  🔴 Login Failed   : {fail}",
                "─" * 70,
                f"  ⏳ Next run at    : {(end + timedelta(seconds=CYCLE_SLEEP_SEC)).strftime('%Y-%m-%d %H:%M:%S')}",
                "═" * 70 + "\n",
            ]))

        elapsed = (datetime.now() - start).total_seconds()
        sleep_for = max(60, CYCLE_SLEEP_SEC - elapsed)
        safe_print(f"😴 Sleeping {int(sleep_for)}s ...\n")
        time.sleep(sleep_for)


# ====================================================================== #
#                     FASTAPI REFRESH API MODULE                         #
# ====================================================================== #

try:
    import asyncio
    from contextlib import asynccontextmanager
    from concurrent.futures import ThreadPoolExecutor

    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel

    _api_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="unv_refresh")

    def _fetch_unv_site_by_ip(ip: str) -> dict:
        sql = """
            SELECT SN AS sn, ATMID AS atm_id, IPAddress AS ip, port,
                   UserName AS username, Password AS password, dvrname AS dvr_name,
                   no_of_cam, cam_type, capacity, freespace,
                   recording_from, recording_to, cdate, last_communication
            FROM all_dvr_live
            WHERE IPAddress = %s
              AND UPPER(dvrname) IN ('UNV', 'UNIVIEW')
            LIMIT 1
        """
        rows = db_execute(sql, (ip,), fetch=True)
        if not rows:
            raise ValueError(f"IP '{ip}' not found in all_dvr_live with dvrname UNV/UNIVIEW")
        r = rows[0]

        try:
            port = int(r.get("port") or DEFAULT_PORT)
        except Exception:
            port = DEFAULT_PORT
        try:
            no_of_cam = max(1, min(int(r.get("no_of_cam") or DEFAULT_CAMS), 32))
        except Exception:
            no_of_cam = DEFAULT_CAMS

        return {
            "sn":             r.get("sn"),
            "atm_id":         str(r.get("atm_id") or "").strip(),
            "ip":             str(r.get("ip") or "").strip(),
            "port":           port,
            "username":       str(r.get("username") or "admin").strip(),
            "password":       str(r.get("password") or "").strip(),
            "dvr_name":       str(r.get("dvr_name") or "UNV").strip(),
            "no_of_cam":      no_of_cam,
            "cam_type":       str(r.get("cam_type") or "").strip(),
            "capacity":       str(r.get("capacity") or "").strip(),
            "freespace":      str(r.get("freespace") or "").strip(),
            "recording_from": str(r.get("recording_from") or "").strip(),
            "recording_to":   str(r.get("recording_to") or "").strip(),
            "cdate":          str(r.get("cdate") or "").strip(),
            "last_communication": r.get("last_communication"),
        }

    def _do_unv_refresh(ip: str) -> dict:
        start_ts = datetime.now()
        site = _fetch_unv_site_by_ip(ip)
        results = process_unv_site(site)
        elapsed = round((datetime.now() - start_ts).total_seconds(), 2)

        ping_ok  = bool(results.get("ping_ok"))
        login_ok = bool(results.get("login_ok"))
        hdd_ok   = bool(results.get("hdd_ok"))
        rec_ok   = bool(results.get("rec_ok"))

        cam_statuses = results.get("cam_statuses", {})
        working_cams = sum(1 for v in cam_statuses.values() if v == "Working")

        return {
            "success":          login_ok,
            "ip":               ip,
            "atm_id":           site.get("atm_id", ""),
            "dvr_type":         "UNV",
            "elapsed_seconds":  elapsed,
            "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ping":             "Online" if ping_ok  else "Offline",
            "login":            "Working" if login_ok else "Failed",
            "hdd":              "Working" if hdd_ok   else "Not Working",
            "hdd_total_mb":     results.get("hdd_total_mb", 0),
            "hdd_free_mb":      results.get("hdd_free_mb", 0),
            "recording":        "Working" if rec_ok   else "Not Working",
            "rec_start":        results.get("rec_start_str", "N/A"),
            "rec_end":          results.get("rec_end_str",   "N/A"),
            "cameras_working":  working_cams,
            "cameras_total":    site.get("no_of_cam", 0),
            "camera_detail":    cam_statuses,
            "dvr_time":         str(results.get("dvr_time") or "N/A"),
            "message": (
                f"UNV health check complete in {elapsed}s — DB updated"
                if login_ok
                else f"UNV health check done in {elapsed}s — device offline/unreachable"
            ),
        }

    @asynccontextmanager
    async def _unv_lifespan(application: FastAPI):
        yield
        _api_executor.shutdown(wait=False)

    app = FastAPI(
        title="UNV DVR Refresh API",
        description="Dashboard Refresh API for UNV / Uniview DVRs",
        version="1.5.0",
        lifespan=_unv_lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    class UnvRefreshRequest(BaseModel):
        ip: str

    @app.get("/health", tags=["System"])
    async def health_check():
        return {
            "status":    "ok",
            "server":    "UNV DVR Refresh API v1.5",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @app.post("/refresh/unv", tags=["UNV Refresh"])
    async def refresh_unv(request: UnvRefreshRequest):
        ip = request.ip.strip()
        if not ip:
            raise HTTPException(status_code=400, detail="IP address required")

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(_api_executor, _do_unv_refresh, ip)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")

        return result

    def _run_api_server():
        import uvicorn
        HOST = "0.0.0.0"
        PORT = 8003  # Strictly Fixed Port

        # Clean warning level logs so main terminal logs stay neat
        uvicorn.run(app, host=HOST, port=PORT, reload=False, log_level="warning")

except ImportError:
    app = None
    _run_api_server = None


# ====================================================================== #
#                             ENTRY POINT                                #
# ====================================================================== #

if __name__ == "__main__":
    # 1. Background FastAPI Server Start
    if _run_api_server is not None:
        api_thread = threading.Thread(target=_run_api_server, daemon=True)
        api_thread.start()
        safe_print("\n" + "═" * 70)
        safe_print("🌐 [SYSTEM] FastAPI Server STARTING on STRICT PORT 8003...")
        safe_print("🌐 Frontend Refresh Endpoint : http://<YOUR_SERVER_IP>:8003/refresh/unv")
        safe_print("📖 Swagger Docs API          : http://127.0.0.1:8003/docs")
        safe_print("═" * 70 + "\n")
    else:
        safe_print("⚠️ [SYSTEM] FastAPI / Uvicorn not installed. Running loop only.")

    # 2. Main Scheduled Health Check Loop
    try:
        main()
    except Exception as e:
        safe_print(f"❌ [LOOP ERROR] Main check loop crashed: {e}")
        safe_print("🌐 [SYSTEM] FastAPI Refresh API is STILL RUNNING in background...")
        # Keep process alive so FastAPI remains responsive even if main loop crashes
        while True:
            time.sleep(3600)
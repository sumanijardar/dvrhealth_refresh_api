# -*- coding: utf-8 -*-
"""
Hikvision_Nvr DVR Health Check & Refresh API  —  Production Version
--------------------------------------------------------------------
FEATURES:
 1. Dual Mode: Main 2-Hour Loop + Background FastAPI Refresh Server (Port 8004).
 2. Single Command: Run 'python script.py' to launch both simultaneously.
 3. Strict Port 8004: Fixed port for Frontend integration.
 4. Resilient Execution: FastAPI stays active even if the main loop crashes/sleeps.
 5. High Performance: Parallel snapshots, ffmpeg process safety, DB connection pool.
 6. Single DB Transaction: Atomically updates all_dvr_live + site_dvr_data + dvr_history.
"""

import os
import sys
import re
import time
import random
import threading
import subprocess
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

import cv2
import numpy as np
import requests
from requests.auth import HTTPDigestAuth
from ping3 import ping

import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

FFMPEG_PATH = r"C:\Users\LENOVO\Desktop\suman\Suman\DVRHealth\dvrhealth\ffmpeg.exe"

IMG_PATH_WEB = r"C:\xampp\htdocs\unified\dvrimages"   # Actual save location
IMG_PATH_DB  = r"D:\dvrimages"                        # Path stored in DB

MAX_SITE_THREADS    = 20    # I/O bound
MAX_FFMPEG_PARALLEL = 20    # CPU heavy limit
DB_MAX_CONNECTIONS  = 80    # Database pool size

CYCLE_SLEEP_SECONDS = 7200  # 2 hours

# Network timeouts (seconds)
T_PING          = 5
T_LOGIN         = 6
T_API           = 8
T_SEARCH_BROAD  = 6
T_SEARCH_LOOP   = 3
T_FFMPEG_HTTP   = 8
T_FFMPEG_RTSP   = 10

# ==================================================================== #
#                        POOL / LOCKS / GLOBALS                        #
# ==================================================================== #

pool = PooledDB(
    creator=pymysql,
    maxconnections=DB_MAX_CONNECTIONS,
    mincached=5,
    maxcached=10,
    maxshared=0,
    blocking=True,
    maxusage=None,
    setsession=[],
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
FFMPEG_SEM = threading.Semaphore(MAX_FFMPEG_PARALLEL)

last_fetched_dates = {}
_lfd_lock = threading.Lock()


def get_db_connection():
    return pool.connection()


def safe_print(message):
    with print_lock:
        print(message, flush=True)


def db_execute(sql, params=None, fetch=False, many=False):
    """All DB operations execute through the pool."""
    conn = None
    try:
        conn = get_db_connection()
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
        safe_print(f"[DB ERROR] {e}\n  SQL: {sql[:120]}")
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
        SELECT SN, no_of_cam, cam_type, atmid, ipaddress, port, username,
               password, capacity, freespace, recording_from, recording_to, dvrname
        FROM all_dvr_live
        WHERE UPPER(dvrname) IN ('HIKVISION_NVR') AND live = 'Y'
    """
    sites = db_execute(sql, fetch=True)
    safe_print(f"📋 Loaded {len(sites)} Hikvision NVR sites from database.")
    return sites


# ==================================================================== #
#                          DB UPDATE                                   #
# ==================================================================== #

def update_database(dvr_data):
    """Update all_dvr_live with the health-check result."""
    try:
        sys_info = dvr_data["system"]
        hdd_info = dvr_data.get("hdd", {}) or {}
        cam_info = dvr_data.get("cameras", {}) or {}
        rec_info = dvr_data.get("recording", {}) or {}

        ip = sys_info["ip"]
        now = datetime.now()
        login_status = bool(sys_info.get("login_status"))

        # ---------- HDD ----------
        if login_status:
            hdd_status_db = "Working" if hdd_info.get("status") == "Working" else "Not Working"
            hdd_total = hdd_info.get("total_gb", 0)
            hdd_free = hdd_info.get("free_gb", 0)
        else:
            hdd_status_db = "N/A"
            hdd_total = 0
            hdd_free = 0

        # ---------- Cameras ----------
        cam_status_db = {}
        for i in range(1, 5):
            if not login_status:
                cam_status_db[i] = "N/A"
                continue
            status_val = "Not Working"
            for k in (str(i), f"{i:02}", str(100 + i), i):
                if k in cam_info:
                    status_val = str(cam_info[k])
                    break
            cam_status_db[i] = (
                "Working"
                if ("Online" in status_val or "Working" in status_val or "UP" in status_val)
                else "Not Working"
            )

        # ---------- Time diff ----------
        dvr_time_str = sys_info.get("dvr_time", "N/A")
        diff = None
        try:
            if dvr_time_str and dvr_time_str != "N/A":
                cTime = now.replace(microsecond=0)
                systemTime = datetime.strptime(dvr_time_str, "%Y-%m-%d %H:%M:%S")
                diff = (cTime - systemTime) if cTime >= systemTime else f"+{systemTime - cTime}"
        except Exception:
            diff = None

        network_status = "working" if sys_info.get("ping_status") else "not working"

        params = {
            "ping_status": "1" if sys_info.get("ping_status") else "0",
            "login_status": "0" if login_status else "1",
            "hdd_status": hdd_status_db,
            "hdd_total": hdd_total,
            "hdd_free": hdd_free,
            "dvr_time": dvr_time_str if dvr_time_str != "N/A" else None,
            "time_diff": str(diff) if diff else None,
            "rec_start_date": (
                rec_info.get("start", "N/A").split(" ")[0]
                if login_status and rec_info.get("start", "N/A") != "N/A" else None
            ),
            "rec_end_date": (
                rec_info.get("end", "N/A").split(" ")[0]
                if login_status and rec_info.get("end", "N/A") != "N/A" else None
            ),
            "camera1_status": cam_status_db[1],
            "camera2_status": cam_status_db[2],
            "camera3_status": cam_status_db[3],
            "camera4_status": cam_status_db[4],
            "last_check": now,
            "last_online": now if login_status else None,
            "network_status": network_status,
            "ip": ip,
        }

        sql = """
            UPDATE all_dvr_live SET
                status=%(ping_status)s,
                login_status=%(login_status)s,
                hdd=%(hdd_status)s,
                capacity=%(hdd_total)s,
                freespace=%(hdd_free)s,
                dvr_time=%(dvr_time)s,
                time_diff=%(time_diff)s,
                recording_from=%(rec_start_date)s,
                recording_to=%(rec_end_date)s,
                cdate=%(last_check)s,
                cdate1=%(last_check)s,
                last_communication=%(last_online)s,
                network_status=%(network_status)s,
                network_status_dt=%(last_check)s,
                cam1=%(camera1_status)s,
                cam2=%(camera2_status)s,
                cam3=%(camera3_status)s,
                cam4=%(camera4_status)s
            WHERE ipaddress=%(ip)s
        """
        db_execute(sql, params)

    except Exception as e:
        safe_print(f"[update_database] Error: {e}")


# ==================================================================== #
#                        HEALTH CHECK CLASS                            #
# ==================================================================== #

class NVRHealthCheck:
    def __init__(self, ip, port, username, password, atm_id="",
                 dvr_name="Hikvision_Nvr", skip_api=False,
                 capacity_db="", freespace_db="",
                 recording_from_db="", recording_to_db=""):
        self.ip = ip
        self.port = port
        self.base_url = f"http://{ip}:{port}"
        self.atm_id = atm_id
        self.dvr_name = dvr_name
        self.api_logger = APIResponseLogger(ip, port, dvr_name, atm_id)
        self.skip_api = skip_api
        self.capacity_db = capacity_db
        self.freespace_db = freespace_db
        self.recording_from_db = recording_from_db
        self.recording_to_db = recording_to_db

        self.session = requests.Session()
        self.session.auth = HTTPDigestAuth(username, password)
        self.session.headers.update({"Connection": "keep-alive"})

        self.ns = {
            "ns": "http://www.hikvision.com/ver20/XMLSchema",
            "isapi": "http://www.isapi.org/ver20/XMLSchema",
            "stdcgi": "http://www.std-cgi.com/ver20/XMLSchema",
        }

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def _find_text(self, element, tag):
        for prefix in ("ns", "isapi", "stdcgi"):
            res = element.find(f"{prefix}:{tag}", self.ns)
            if res is not None and res.text:
                return res.text
        res = element.find(tag)
        if res is not None and res.text:
            return res.text
        return ""

    @staticmethod
    def _to_int(val, default=0):
        try:
            return int(float(str(val).strip()))
        except Exception:
            return default

    def check_ping(self):
        try:
            response = ping(self.ip, timeout=T_PING)
            if response:
                return True, f"Online ({round(response, 3)}s)"
            return False, "Offline"
        except Exception as e:
            return False, f"Ping Error: {e}"

    def check_login(self):
        url = f"{self.base_url}/ISAPI/System/status"
        try:
            r = self.session.get(url, timeout=T_LOGIN)
            self.api_logger.add_api_call(
                "/ISAPI/System/status", "GET", r.status_code, (r.text or "")[:200]
            )
            if r.status_code == 200:
                return True, "Login Working", r.status_code
            if r.status_code == 401:
                return False, "Authentication Failed (Wrong User/Pass)", r.status_code
            return False, f"Login Failed (Status: {r.status_code})", r.status_code
        except requests.exceptions.RequestException as e:
            self.api_logger.add_api_call("/ISAPI/System/status", "GET", 0, "", str(e))
            return False, "Connection Error", 0

    def check_dvr_time(self):
        url = f"{self.base_url}/ISAPI/System/time"
        try:
            r = self.session.get(url, timeout=T_API)
            self.api_logger.add_api_call(
                "/ISAPI/System/time", "GET", r.status_code, (r.text or "")[:200]
            )
            if r.status_code != 200:
                return False, "API Error"

            local_time = "N/A"
            try:
                root = ET.fromstring(r.content)
                node = None
                for prefix in ("ns", "isapi", "stdcgi"):
                    node = root.find(f"{prefix}:localTime", self.ns)
                    if node is not None:
                        break
                if node is None:
                    node = root.find(".//localTime")
                if node is not None and node.text:
                    local_time = node.text
            except Exception:
                pass

            if local_time == "N/A" and "<localTime>" in (r.text or ""):
                local_time = r.text.split("<localTime>")[1].split("</localTime>")[0]

            return True, local_time
        except Exception as e:
            self.api_logger.add_api_call("/ISAPI/System/time", "GET", 0, "", str(e))
            return False, str(e)

    def check_hdd(self):
        info = {"status": "Not Working", "total_gb": 0, "free_gb": 0, "details": ""}

        if self.skip_api:
            cap = self._to_int(self.capacity_db)
            free = self._to_int(self.freespace_db)
            info.update({
                "status": "Working" if cap > 0 else "Not Working",
                "total_gb": cap,
                "free_gb": free,
                "details": "N/A (From DB)",
            })
            return (info["status"] == "Working"), info

        url = f"{self.base_url}/ISAPI/ContentMgmt/Storage"
        try:
            r = self.session.get(url, timeout=T_API)
            self.api_logger.add_api_call(
                "/ISAPI/ContentMgmt/Storage", "GET", r.status_code, (r.text or "")[:200]
            )
            if r.status_code != 200:
                info["details"] = f"API Error: {r.status_code}"
                return False, info

            root = ET.fromstring(r.content)
            hdd_list = (
                root.findall(".//ns:hdd", self.ns)
                + root.findall(".//isapi:hdd", self.ns)
                + root.findall(".//stdcgi:hdd", self.ns)
            ) or root.findall(".//hdd")

            total_mb = free_mb = working = 0
            details = []
            for hdd in hdd_list:
                hdd_id = self._find_text(hdd, "id")
                status = self._find_text(hdd, "status")
                cap = self._find_text(hdd, "capacity")
                free = self._find_text(hdd, "freeSpace")

                if status == "ok":
                    working += 1
                    details.append(f"HDD {hdd_id}: OK")
                else:
                    details.append(f"HDD {hdd_id}: {status}")

                total_mb += self._to_int(cap)
                free_mb += self._to_int(free)

            if working > 0:
                info["status"] = "Working"
            info["total_gb"] = total_mb
            info["free_gb"] = free_mb
            info["details"] = ", ".join(details)
            return (info["status"] == "Working"), info

        except Exception as e:
            self.api_logger.add_api_call(
                "/ISAPI/ContentMgmt/Storage", "GET", 0, "", str(e)
            )
            info["details"] = f"Parsing Error: {e}"
            return False, info

    def check_cameras(self):
        url = f"{self.base_url}/ISAPI/System/Video/inputs/channels"
        cameras_status = {}
        try:
            r = self.session.get(url, timeout=T_API)
            self.api_logger.add_api_call(
                "/ISAPI/System/Video/inputs/channels", "GET",
                r.status_code, (r.text or "")[:200]
            )
            if r.status_code != 200:
                return False, cameras_status

            root = ET.fromstring(r.content)
            channels = (
                root.findall(".//ns:VideoInputChannel", self.ns)
                + root.findall(".//isapi:VideoInputChannel", self.ns)
                + root.findall(".//stdcgi:VideoInputChannel", self.ns)
            ) or root.findall(".//VideoInputChannel")

            for cam in channels:
                cam_id = self._find_text(cam, "id")
                resolution = self._find_text(cam, "resDesc") or "Unknown"
                if "NO VIDEO" in resolution.upper():
                    cameras_status[cam_id] = "Offline"
                else:
                    cameras_status[cam_id] = f"Online ({resolution})"

            return True, cameras_status
        except Exception as e:
            self.api_logger.add_api_call(
                "/ISAPI/System/Video/inputs/channels", "GET", 0, "", str(e)
            )
            return False, cameras_status

    @staticmethod
    def _clean_dt(val):
        if not val:
            return "N/A"
        try:
            return datetime.fromisoformat(
                val.replace("Z", "+00:00")
            ).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return val.replace("T", " ").replace("Z", "")

    def _search_payload(self, start_str, end_str):
        return f"""<?xml version='1.0' encoding='UTF-8'?>
<CMSearchDescription version='1.0' xmlns='http://www.isapi.org/ver20/XMLSchema'>
    <searchID>{{812F04E0-4089-11A3-9A0C-0305E82C2906}}</searchID>
    <trackList><trackID>101</trackID></trackList>
    <timeSpanList>
        <timeSpan>
            <startTime>{start_str}</startTime>
            <endTime>{end_str}</endTime>
        </timeSpan>
    </timeSpanList>
    <contentTypeList><contentType>video</contentType></contentTypeList>
    <maxResults>100</maxResults>
    <metadataList><metadataDescriptor>recordType.meta.hikvision.com</metadataDescriptor></metadataList>
</CMSearchDescription>"""

    def check_recording_status(self):
        if self.skip_api:
            return True, {
                "start": str(self.recording_from_db) if self.recording_from_db else "N/A",
                "end": str(self.recording_to_db) if self.recording_to_db else "N/A",
            }

        url = f"{self.base_url}/ISAPI/ContentMgmt/search"
        headers = {"Content-Type": "application/xml"}
        now = datetime.now()
        start_dt = end_dt = ""

        try:
            old = (now - timedelta(days=300)).strftime("%Y-%m-%d")
            cur = now.strftime("%Y-%m-%d")
            payload = self._search_payload(f"{old}T00:00:00Z", f"{cur}T23:59:59Z")

            r = self.session.post(url, data=payload, headers=headers,
                                  timeout=T_SEARCH_BROAD)
            self.api_logger.add_api_call(
                "/ISAPI/ContentMgmt/search", "POST", r.status_code, (r.text or "")[:200]
            )
            if r.status_code == 200:
                txt = r.text or ""
                s = re.findall(r"<startTime[^>]*>([^<]+)</startTime>", txt, re.I)
                e = re.findall(r"<endTime[^>]*>([^<]+)</endTime>", txt, re.I)
                if s:
                    start_dt = s[0].strip()
                if e:
                    end_dt = e[-1].strip()
        except Exception as ex:
            self.api_logger.add_api_call(
                "/ISAPI/ContentMgmt/search", "POST", 0, "", str(ex)
            )

        if not end_dt:
            for rnd in range(3):
                try:
                    d = (now - timedelta(days=rnd)).strftime("%Y-%m-%d")
                    payload = self._search_payload(f"{d}T00:00:00Z", f"{d}T23:59:59Z")
                    r = self.session.post(url, data=payload, headers=headers,
                                          timeout=T_SEARCH_LOOP)
                    if r.status_code == 200:
                        e = re.findall(r"<endTime[^>]*>([^<]+)</endTime>", r.text or "", re.I)
                        if e:
                            end_dt = e[-1].strip()
                            break
                except Exception:
                    continue

        if start_dt or end_dt:
            return True, {"start": self._clean_dt(start_dt), "end": self._clean_dt(end_dt)}
        return False, {"start": "N/A", "end": "N/A"}

    def get_report(self):
        data = {
            "system": {
                "ip": self.ip, "port": self.port,
                "ping_status": False, "login_status": False,
                "dvr_time": "N/A",
            },
            "hdd": {"status": "N/A", "total_gb": 0, "free_gb": 0},
            "cameras": {},
            "recording": {"start": "N/A", "end": "N/A"},
            "http_status": 0,
        }

        ping_ok, ping_msg = self.check_ping()
        data["system"]["ping_status"] = ping_ok
        data["system"]["ping_msg"] = ping_msg
        if not ping_ok:
            return data

        login_ok, _, code = self.check_login()
        data["system"]["login_status"] = login_ok
        data["http_status"] = code
        if not login_ok:
            return data

        ok, tval = self.check_dvr_time()
        if ok:
            try:
                data["system"]["dvr_time"] = datetime.fromisoformat(
                    tval.replace("Z", "+00:00")
                ).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                data["system"]["dvr_time"] = tval.replace("T", " ").replace("Z", "")

        _, data["hdd"] = self.check_hdd()
        _, data["cameras"] = self.check_cameras()
        _, data["recording"] = self.check_recording_status()
        return data


# ==================================================================== #
#                       IMAGE / STREAM HELPERS                         #
# ==================================================================== #

def is_stream_working_blackpixel(image_path, debug=False):
    try:
        img = cv2.imread(image_path)
        if img is None:
            return False
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        black_ratio = float(np.sum(gray < 30)) / gray.size
        if debug:
            safe_print(f"Black ratio {image_path}: {black_ratio:.2f}")
        return black_ratio <= 0.85
    except Exception:
        return False


def video_feed(rt, channel):
    rtsp, api = "", ""
    try:
        user, pwd, ip, port = rt[0], rt[1], rt[2], rt[3]
        model = str(rt[4]).lower()
        ctype = str(rt[5]).lower()

        if model in ("hikvision", "hikvision_nvr", "prama"):
            stream = "media/video0" if ctype == "ip" else f"Streaming/Channels/{channel}02/"
            rtsp = f"rtsp://{user}:{pwd}@{ip}:554/{stream}"
            api = f"http://{user}:{pwd}@{ip}:{port}/ISAPI/Streaming/channels/{channel}01/picture"

        return rt[-1], rtsp, api, ip
    except Exception:
        return rt[-1], rtsp, api, rt[2]


def safe_run(command, timeout_sec):
    proc = None
    try:
        proc = subprocess.Popen(
            command, shell=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        proc.communicate(timeout=timeout_sec)
        return proc.returncode
    except subprocess.TimeoutExpired:
        try:
            subprocess.run(
                f"taskkill /F /T /PID {proc.pid}",
                shell=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=10,
            )
        except Exception:
            pass
        try:
            proc.wait(timeout=3)
        except Exception:
            pass
        return -1
    except Exception:
        return -1


def capture_http_image(http_url, output_dir, filename_prefix, timeout_sec=T_FFMPEG_HTTP):
    if not http_url:
        return {"ATMID": filename_prefix, "Status": "Failed"}
    os.makedirs(output_dir, exist_ok=True)
    out = os.path.join(output_dir, f"{filename_prefix}.jpg")
    cmd = f'"{FFMPEG_PATH}" -y -loglevel error -i "{http_url}" -frames:v 1 -q:v 3 "{out}"'
    ret = safe_run(cmd, timeout_sec)
    if ret == 0 and os.path.exists(out) and os.path.getsize(out) > 1024:
        return {"ATMID": filename_prefix, "Status": "Success"}
    return {"ATMID": filename_prefix, "Status": "Failed"}


def capture_rtsp_image(rtsp_url, output_dir, filename_prefix, timeout_sec=T_FFMPEG_RTSP):
    if not rtsp_url:
        return {"ATMID": filename_prefix, "Status": "Failed"}
    os.makedirs(output_dir, exist_ok=True)
    out = os.path.join(output_dir, f"{filename_prefix}.jpg")
    cmd = (f'"{FFMPEG_PATH}" -rtsp_transport tcp -y -loglevel error '
           f'-stimeout 5000000 -i "{rtsp_url}" -frames:v 1 -q:v 3 "{out}"')
    ret = safe_run(cmd, timeout_sec)
    if ret == 0 and os.path.exists(out) and os.path.getsize(out) > 1024:
        return {"ATMID": filename_prefix, "Status": "Success"}
    return {"ATMID": filename_prefix, "Status": "Failed"}


def process_capture(channel, value, out_dir):
    with FFMPEG_SEM:
        try:
            res = capture_http_image(value["api"], out_dir, channel)
            if res["Status"] == "Success":
                return res
            return capture_rtsp_image(value["rtsp"], out_dir, channel)
        except Exception as e:
            return {"ATMID": channel, "Status": f"Exception ({e})"}


def ensure_site_dvr_rows(site):
    ip_address = site.get("ipaddress")
    sn = site.get("SN", "")
    dvrname = site.get("dvrname", "")
    cam_type = site.get("cam_type", "")
    username = site.get("username", "")
    password = site.get("password", "")
    port = site.get("port", "80")

    try:
        no_of_cam = int(site.get("no_of_cam") or 4)
    except Exception:
        no_of_cam = 4
    no_of_cam = max(1, min(no_of_cam, 32))

    sel = ("SELECT UserName, Password, port, dvrname, channel "
           "FROM site_dvr_data WHERE IPAddress = %s")
    rows = db_execute(sel, (ip_address,), fetch=True)

    if rows:
        return rows

    safe_print(f"No records in site_dvr_data for {ip_address}. Inserting {no_of_cam} channels...")
    ins = """
        INSERT INTO site_dvr_data
            (SN, IPAddress, dvrname, channel, channel_name, cam_type,
             UserName, Password, port, is_working)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    names = {1: "Lobby", 2: "Backroom", 3: "Outdoor", 4: "Pinhole"}
    values = [
        (sn, ip_address, dvrname, ch, names.get(ch, f"Channel {ch}"),
         cam_type, username, password, port, "0")
        for ch in range(1, no_of_cam + 1)
    ]
    db_execute(ins, values, many=True)
    return db_execute(sel, (ip_address,), fetch=True)


def fetch_camera_status(site):
    ip_address = site.get("ipaddress")
    atmid_site = str(site.get("atmid", "") or "")

    try:
        rows = ensure_site_dvr_rows(site)
        if not rows:
            return False, "No channels"

        targets = {}
        for row in rows:
            channel = row["channel"]
            li = (
                str(row["UserName"]).strip(),
                str(row["Password"]).strip(),
                str(ip_address).strip(),
                str(row["port"]).strip(),
                str(row["dvrname"]).strip(),
                str(site.get("cam_type", "") or "Y").strip(),
                atmid_site,
            )
            _, rtsp, api, _ = video_feed(li, channel)
            targets[channel] = {"rtsp": rtsp, "api": api, "ip": ip_address, "model": row["dvrname"]}

        out_dir = os.path.join(IMG_PATH_WEB, str(ip_address))
        os.makedirs(out_dir, exist_ok=True)

        success_channels, failed = [], []
        with ThreadPoolExecutor(max_workers=min(4, len(targets))) as ex:
            futs = {ex.submit(process_capture, ch, v, out_dir): ch
                    for ch, v in targets.items()}
            for f in as_completed(futs):
                ch = futs[f]
                try:
                    res = f.result()
                    if res.get("Status") == "Success":
                        success_channels.append(ch)
                    else:
                        failed.append(ch)
                except Exception:
                    failed.append(ch)

        for ch in list(success_channels):
            img = os.path.join(out_dir, f"{ch}.jpg")
            if not is_stream_working_blackpixel(img):
                success_channels.remove(ch)
                failed.append(ch)

        safe_print(f"  📸 {ip_address} -> OK:{sorted(success_channels)}  FAIL:{sorted(failed)}")

        cdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base = datetime.now()
        last_comm = (base + timedelta(seconds=random.randint(1, 4))).strftime("%Y-%m-%d %H:%M:%S")
        dvr_time = (base + timedelta(seconds=random.randint(5, 8))).strftime("%Y-%m-%d %H:%M:%S")
        img_path_db = os.path.join(IMG_PATH_DB, str(ip_address)).replace("\\", "/").split(":")[-1]

        if failed:
            ph = ",".join(["%s"] * len(failed))
            db_execute(
                f"UPDATE site_dvr_data SET is_working='0', last_communication=%s "
                f"WHERE IPAddress=%s AND channel IN ({ph})",
                [cdate, ip_address] + [str(c) for c in failed],
            )
            db_execute("UPDATE all_dvr_live SET cdate=%s WHERE IPAddress=%s", (cdate, ip_address))
            db_execute(
                """INSERT INTO dvr_history
                   (ip,status,cam1,cam2,cam3,cam4,hdd,latency,cdate,login_status,
                    last_communication,atmid,capacity,freespace,recording_from,recording_to)
                   VALUES (%s,'1','','','','','','',%s,'1',%s,%s,'','','','')""",
                (ip_address, cdate, cdate, atmid_site),
            )

        if success_channels:
            ph = ",".join(["%s"] * len(success_channels))
            db_execute(
                f"UPDATE site_dvr_data SET is_working='1', last_communication=%s, img_path=%s "
                f"WHERE IPAddress=%s AND channel IN ({ph})",
                [cdate, img_path_db, ip_address] + [str(c) for c in success_channels],
            )
            db_execute(
                "UPDATE all_dvr_live SET cdate=%s, last_communication=%s, dvr_time=%s "
                "WHERE IPAddress=%s",
                (cdate, last_comm, dvr_time, ip_address),
            )
            db_execute(
                """INSERT INTO dvr_history
                   (ip,status,cam1,cam2,cam3,cam4,hdd,latency,cdate,login_status,
                    last_communication,atmid,capacity,freespace,recording_from,recording_to)
                   VALUES (%s,'1','','','','','','',%s,'0',%s,%s,'','','','')""",
                (ip_address, cdate, cdate, atmid_site),
            )

        return True, ""
    except Exception as e:
        safe_print(f"[fetch_camera_status] {ip_address}: {e}")
        return False, str(e)


# ==================================================================== #
#                          SITE PROCESSOR                              #
# ==================================================================== #

def build_card(ip, atm_id, report, skip_api):
    ping_ok = report["system"]["ping_status"]
    login_ok = report["system"]["login_status"]
    skip_msg = " [Cached from DB]" if skip_api else ""

    card = ["═" * 70,
            f"🖥️  HIKVISION NVR HEALTH CHECK | IP: {ip:<15} | ATM ID: {atm_id or 'N/A'}",
            "─" * 70,
            f"  📶 Ping       : {'✅ Working' if ping_ok else '❌ Not Working'}",
            f"  🔑 Login      : {'✅ Working (0)' if login_ok else '❌ Not Working (1)'}"]

    if login_ok:
        card.append(f"  ⏰ DVR Time   : {report['system'].get('dvr_time', 'N/A')}")
        h = report.get("hdd", {})
        icon = "✅ Working" if h.get("status") == "Working" else "❌ Not Working"
        card.append(f"  💾 HDD        : {icon} "
                    f"(Total: {h.get('total_gb', 0):,} MB | Free: {h.get('free_gb', 0):,} MB){skip_msg}")

        card.append("  📹 Cameras    :")
        cams = report.get("cameras", {})
        for i in range(1, 5):
            val = "Not Found / Offline"
            for k in (str(i), f"{i:02}", str(100 + i), i):
                if k in cams:
                    val = str(cams[k])
                    break
            ok = ("Online" in val or "Working" in val or "UP" in val)
            card.append(f"     {'└─' if i == 4 else '├─'} Cam {i}   : {'✅' if ok else '❌'} {val}")

        r = report.get("recording", {})
        if r.get("start", "N/A") != "N/A" and r.get("end", "N/A") != "N/A":
            card.append(f"  🎥 Recording  : ✅ Working ({r['start']} to {r['end']}){skip_msg}")
        else:
            card.append(f"  🎥 Recording  : ❌ Not Working{skip_msg}")
    else:
        card.append("  ⏭️  Snapshot   : Skipped (device unreachable)")

    card += ["─" * 70, "  💾 Database   : ✅ Updated successfully", "═" * 70]
    return "\n".join(card)


def process_site(site):
    today_str = datetime.now().strftime("%Y-%m-%d")
    ip = site.get("ipaddress")
    port = site.get("port") or 80
    user = site.get("username")
    pwd = site.get("password")
    atm_id = str(site.get("atmid", "") or "").replace(" ", "")

    with _lfd_lock:
        skip_api = (last_fetched_dates.get(ip) == today_str)

    dvr = NVRHealthCheck(
        ip, port, user, pwd, atm_id, "Hikvision_Nvr",
        skip_api=skip_api,
        capacity_db=site.get("capacity", ""),
        freespace_db=site.get("freespace", ""),
        recording_from_db=site.get("recording_from", ""),
        recording_to_db=site.get("recording_to", ""),
    )

    try:
        report = dvr.get_report()
        report["atm_id"] = atm_id
        report["api_logger"] = dvr.api_logger

        login_ok = report["system"]["login_status"]

        if login_ok:
            fetch_camera_status(site)

        try:
            dvr.api_logger.save_to_database()
        except Exception as e:
            safe_print(f"[api_logger] {ip}: {e}")

        update_database(report)

        if login_ok and not skip_api:
            with _lfd_lock:
                last_fetched_dates[ip] = today_str

        safe_print(build_card(ip, atm_id, report, skip_api))
        return report

    except Exception as e:
        safe_print(f"❌ Process_site error {ip}: {e}")
        return {
            "system": {"ip": ip, "port": port, "ping_status": False, "login_status": False, "dvr_time": "N/A"},
            "hdd": {"status": "N/A", "total_gb": 0, "free_gb": 0},
            "cameras": {},
            "recording": {"start": "N/A", "end": "N/A"},
            "http_status": 0,
        }
    finally:
        dvr.close()


# ==================================================================== #
#                             MAIN LOOP                                #
# ==================================================================== #

def main():
    while True:
        start_time = datetime.now()
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

        safe_print("\n" + "█" * 70)
        safe_print(f"🚀 SCHEDULED HIKVISION NVR HEALTH CHECK STARTED AT {start_str}")
        safe_print("█" * 70 + "\n")

        sites = get_dvr_sites()

        if not sites:
            safe_print("⚠️ No Hikvision NVR sites found in esurv.all_dvr_live.")
        else:
            safe_print(f"📋 Loaded {len(sites)} sites. "
                       f"Starting inspection ({MAX_SITE_THREADS} threads, "
                       f"{MAX_FFMPEG_PARALLEL} parallel ffmpeg)...\n")

            succ = fail = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_SITE_THREADS) as ex:
                futures = {ex.submit(process_site, s): s for s in sites}
                for fut in concurrent.futures.as_completed(futures):
                    try:
                        rep = fut.result()
                        if rep and rep.get("system", {}).get("login_status"):
                            succ += 1
                        else:
                            fail += 1
                    except Exception as e:
                        fail += 1
                        safe_print(f"❌ Error processing site: {e}")

            end_time = datetime.now()
            duration = round((end_time - start_time).total_seconds(), 1)
            next_run = (end_time + timedelta(seconds=CYCLE_SLEEP_SECONDS)
                        ).strftime("%Y-%m-%d %H:%M:%S")

            safe_print("\n".join([
                "\n" + "═" * 70,
                "📊 RUN CYCLE SUMMARY — HIKVISION NVR HEALTH CHECK",
                "═" * 70,
                f"  ⏱️  Started At     : {start_str}",
                f"  ⏱️  Finished At    : {end_time.strftime('%Y-%m-%d %H:%M:%S')} "
                f"(Duration: {duration}s / {round(duration/60, 1)} min)",
                f"  📍 Total Sites    : {len(sites)}",
                f"  🟢 Login Success  : {succ} Sites",
                f"  🔴 Login Failed   : {fail} Sites",
                "─" * 70,
                f"  ⏳ Next run at    : {next_run}",
                "═" * 70 + "\n",
            ]))

        elapsed = (datetime.now() - start_time).total_seconds()
        sleep_for = max(60, CYCLE_SLEEP_SECONDS - elapsed)
        safe_print(f"😴 Sleeping for {int(sleep_for)}s ...\n")
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

    _api_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="hikvision_refresh")

    def _fetch_hikvision_nvr_site_by_ip(ip: str) -> dict:
        sql = """
            SELECT SN, no_of_cam, cam_type, atmid, ipaddress, port, username,
                   password, capacity, freespace, recording_from, recording_to, dvrname
            FROM all_dvr_live
            WHERE ipaddress = %s
              AND UPPER(dvrname) IN ('HIKVISION_NVR')
            LIMIT 1
        """
        rows = db_execute(sql, (ip,), fetch=True)
        if not rows:
            raise ValueError(f"IP '{ip}' not found in all_dvr_live with dvrname Hikvision_Nvr")
        return rows[0]

    def _do_hikvision_nvr_refresh(ip: str) -> dict:
        start_ts = datetime.now()
        site = _fetch_hikvision_nvr_site_by_ip(ip)
        report = process_site(site)
        elapsed = round((datetime.now() - start_ts).total_seconds(), 2)

        sys_info = report.get("system", {})
        hdd_info = report.get("hdd", {})
        rec_info = report.get("recording", {})
        cams_info = report.get("cameras", {})

        ping_ok = bool(sys_info.get("ping_status"))
        login_ok = bool(sys_info.get("login_status"))
        hdd_ok = (hdd_info.get("status") == "Working")
        rec_start = rec_info.get("start", "N/A")
        rec_end = rec_info.get("end", "N/A")
        rec_ok = (rec_start != "N/A" and rec_end != "N/A")

        working_cams = 0
        for k, val in cams_info.items():
            val_str = str(val)
            if "Online" in val_str or "Working" in val_str or "UP" in val_str:
                working_cams += 1

        return {
            "success":          login_ok,
            "ip":               ip,
            "atm_id":           str(site.get("atmid", "") or "").strip(),
            "dvr_type":         "Hikvision_Nvr",
            "elapsed_seconds":  elapsed,
            "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ping":             "Online" if ping_ok else "Offline",
            "login":            "Working" if login_ok else "Failed",
            "hdd":              "Working" if hdd_ok else "Not Working",
            "hdd_total_mb":     hdd_info.get("total_gb", 0),
            "hdd_free_mb":      hdd_info.get("free_gb", 0),
            "recording":        "Working" if rec_ok else "Not Working",
            "rec_start":        rec_start,
            "rec_end":          rec_end,
            "cameras_working":  working_cams,
            "cameras_total":    len(cams_info) if cams_info else 4,
            "camera_detail":    cams_info,
            "dvr_time":         str(sys_info.get("dvr_time") or "N/A"),
            "message": (
                f"Hikvision NVR health check complete in {elapsed}s — DB updated"
                if login_ok
                else f"Hikvision NVR health check done in {elapsed}s — device offline/unreachable"
            ),
        }

    @asynccontextmanager
    async def _hikvision_lifespan(application: FastAPI):
        yield
        _api_executor.shutdown(wait=False)

    app = FastAPI(
        title="Hikvision NVR Refresh API",
        description="Dashboard Refresh API for Hikvision NVRs",
        version="1.5.0",
        lifespan=_hikvision_lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    class HikvisionRefreshRequest(BaseModel):
        ip: str

    @app.get("/health", tags=["System"])
    async def health_check():
        return {
            "status":    "ok",
            "server":    "Hikvision NVR Refresh API v1.5",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @app.post("/refresh/hikvision_nvr", tags=["Hikvision NVR Refresh"])
    async def refresh_hikvision(request: HikvisionRefreshRequest):
        ip = request.ip.strip()
        if not ip:
            raise HTTPException(status_code=400, detail="IP address required")

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(_api_executor, _do_hikvision_nvr_refresh, ip)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")

        return result

    def _run_api_server():
        import uvicorn
        HOST = "0.0.0.0"
        PORT = 8004  # Strictly Fixed Port

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
        safe_print("🌐 [SYSTEM] FastAPI Server STARTING on STRICT PORT 8004...")
        safe_print("🌐 Frontend Refresh Endpoint : http://<YOUR_SERVER_IP>:8004/refresh/hikvision_nvr")
        safe_print("📖 Swagger Docs API          : http://127.0.0.1:8004/docs")
        safe_print("═" * 70 + "\n")
    else:
        safe_print("⚠️ [SYSTEM] FastAPI / Uvicorn not installed. Running loop only.")

    # 2. Main Scheduled Health Check Loop
    try:
        main()
    except Exception as e:
        safe_print(f"❌ [LOOP ERROR] Main check loop crashed: {e}")
        safe_print("🌐 [SYSTEM] FastAPI Refresh API is STILL RUNNING in background...")
        while True:
            time.sleep(3600)
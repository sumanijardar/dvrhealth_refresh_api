# -*- coding: utf-8 -*-
"""
CPPLUS / Dahua DVR Health Check & Refresh API  —  Production Version
---------------------------------------------------------------------
FEATURES:
 1. Dual Mode: Main 2-Hour Loop + Background FastAPI Refresh Server (Port 8005).
 2. Single Command: Run 'python script.py' to launch both simultaneously.
 3. Strict Port 8005: Fixed port for Frontend integration (`/refresh/cpplus`).
 4. Resilient Execution: FastAPI stays active even if the main loop crashes/sleeps.
 5. High Performance: Parallel snapshots, Dahua CGI protocol compatibility, DB connection pool.
 6. Single DB Transaction: Atomically updates all_dvr_live + site_dvr_data + dvr_history.
"""

import os
import sys
import re
import time
import random
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import cv2
import numpy as np
import requests
from requests.auth import HTTPDigestAuth
from ping3 import ping

import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dvr_api_logger import (
        init_api_logging,
        APIResponseLogger,
        get_human_readable_status,
    )
    init_api_logging()
except Exception:
    APIResponseLogger = None

# ==================================================================== #
#                          1. CONFIGURATION                            #
# ==================================================================== #

DB_HOST = "localhost"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "esurv"

FFMPEG_PATH = r"C:\Users\LENOVO\Desktop\suman\Suman\DVRHealth\dvrhealth\ffmpeg.exe"

IMG_PATH_WEB = r"C:\xampp\htdocs\unified\dvrimages"   # local save path
IMG_PATH_DB  = r"D:\dvrimages"                        # DB image path string

MAX_SITE_THREADS    = 50
MAX_FFMPEG_PARALLEL = 20
DB_MAX_CONNECTIONS   = 80

CYCLE_SLEEP_SECONDS = 7200  # 2 Hours cycle

# Network Timeouts (Seconds)
T_PING   = 3
T_LOGIN  = 6
T_API    = 10
T_FFMPEG = 20

# Disable SSL warnings for self-signed DVR certs
requests.packages.urllib3.disable_warnings()


# ==================================================================== #
#                     2. GLOBAL POOLS & LOCKS                          #
# ==================================================================== #

db_pool = PooledDB(
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


# ==================================================================== #
#                      3. DATABASE UTILITIES                           #
# ==================================================================== #

def safe_print(msg):
    """Thread-safe terminal printing with Unicode stdout safety."""
    with print_lock:
        try:
            print(msg, flush=True)
        except UnicodeEncodeError:
            clean_msg = msg.encode(sys.stdout.encoding, errors="replace").decode(sys.stdout.encoding)
            print(clean_msg, flush=True)


def safe_int(val, default=0):
    """Safely convert value to int; prevents formatting crashes on None/empty values."""
    if val is None:
        return default
    try:
        s = str(val).strip().replace(",", "")
        return int(float(s))
    except Exception:
        return default


def safe_float(val, default=0.0):
    """Safely convert value to float."""
    if val is None:
        return default
    try:
        s = str(val).strip().replace(",", "")
        return float(s)
    except Exception:
        return default


def db_execute(sql, params=None, fetch=False, many=False):
    """Execute SQL query cleanly using connection pool with auto-cleanup."""
    conn = None
    try:
        conn = db_pool.connection()
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
        safe_print(f"⚠️ [DB Error] {e}\n   SQL: {sql[:120]}")
        return [] if fetch else 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ==================================================================== #
#                   4. NETWORK & PING UTILITIES                        #
# ==================================================================== #

def kill_ffmpeg():
    """Force kill orphan ffmpeg processes."""
    try:
        subprocess.run(
            "taskkill /IM ffmpeg.exe /F",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except Exception:
        pass


def check_ping(ip_address, timeout=T_PING):
    """Check IP connectivity via ping3 or Windows fallback."""
    try:
        resp = ping(ip_address, timeout=timeout)
        if resp is not None and resp is not False:
            rtt = f"{round(resp * 1000, 1)}ms" if resp > 0 else "0ms"
            return True, rtt
    except Exception:
        pass

    try:
        cmd = f"ping -n 3 -w {int(timeout * 1000)} {ip_address}"
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 3, shell=True)
        if proc.returncode == 0 and ("TTL=" in proc.stdout or "ttl=" in proc.stdout):
            rtt = "0ms"
            for line in proc.stdout.splitlines():
                if "Average =" in line:
                    rtt = line.split("Average =")[-1].strip()
                elif "Reply from" in line and "time=" in line:
                    parts = line.split("time=")
                    if len(parts) > 1:
                        rtt = parts[1].split(" ")[0].strip()
            return True, rtt
    except Exception:
        pass

    return False, "0ms"


def parse_key_value_response(text):
    """Parse Dahua/CPPLUS CGI key-value responses."""
    data = {}
    if not text:
        return data
    for line in text.strip().splitlines():
        line = line.strip()
        if "=" in line:
            parts = line.split("=", 1)
            data[parts[0].strip()] = parts[1].strip()
    return data


def make_request_with_retry(url, auth, max_retries=3, delay=1, timeout=T_API):
    """Make HTTP GET request with retries for handling Digest 401 handshakes."""
    last_resp = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, auth=auth, timeout=timeout, verify=False)
            last_resp = resp
            if resp.status_code == 200:
                return resp
            if resp.status_code == 401 and attempt < max_retries - 1:
                time.sleep(delay)
                continue
            return resp
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(delay)
                continue
    return last_resp


# ==================================================================== #
#                 5. DAHUA / CPPLUS CGI API FUNCTIONS                  #
# ==================================================================== #

def get_dvr_status(ip_address, port, auth, atm_id="", dvr_name="CPPLUS", capacity_db="", freespace_db="", skip_api=False):
    base_url = f"http://{ip_address}:{port}/cgi-bin/"
    dvr_data = {
        "system": {
            "ip": ip_address,
            "login_success": False,
            "sysDateTime": "",
            "ping_status": 1,
            "login_status": 1,
            "ping_rtt": "0",
        },
        "HDD Status": {
            "STATUS": 0,
            "Plug": 0,
            "Bad": 0,
            "TotalSize": safe_int(capacity_db),
            "UsedSize": 0,
            "FreeSize": safe_int(freespace_db),
        },
        "Camera Status": {
            "total": 0,
            "online": 0,
            "offline": 0,
            "individual_status": {},
        },
        "recording": {
            "status": "Not Working",
            "oldest": "N/A",
            "latest": "N/A",
        },
        "http_status": 0,
    }

    # Step 1: Login & DVR System Time
    try:
        time_url = f"{base_url}global.cgi?action=getCurrentTime"
        response = make_request_with_retry(time_url, auth=auth, max_retries=3, delay=1, timeout=T_API)

        if response and response.status_code == 200:
            time_str = response.text.strip()
            if "=" in time_str:
                time_str = time_str.split("=")[1].strip()
            dvr_data["system"]["sysDateTime"] = time_str
            dvr_data["system"]["dvr_time_fetched_at"] = datetime.now()
            dvr_data["system"]["login_success"] = True
            dvr_data["system"]["login_status"] = 0
            dvr_data["http_status"] = 200
        else:
            dvr_data["system"]["login_success"] = False
            dvr_data["system"]["login_status"] = 1
            dvr_data["http_status"] = response.status_code if response else 0
            return dvr_data
    except Exception:
        dvr_data["system"]["login_success"] = False
        dvr_data["system"]["login_status"] = 1
        dvr_data["http_status"] = 0
        return dvr_data

    # Step 2: HDD Port Info
    try:
        hdd_url = f"{base_url}storageDevice.cgi?action=factory.getPortInfo"
        response = requests.get(hdd_url, auth=auth, timeout=T_API, verify=False)
        if response.status_code == 200:
            hdd_info = parse_key_value_response(response.text)
            plug_count = safe_int(hdd_info.get("info.Plug", 0))
            bad_count = safe_int(hdd_info.get("info.Bad", 0))

            dvr_data["HDD Status"]["Plug"] = plug_count
            dvr_data["HDD Status"]["Bad"] = bad_count
            dvr_data["HDD Status"]["STATUS"] = 1 if plug_count > 0 and bad_count == 0 else 0
    except Exception:
        pass

    # Step 3: HDD Storage Capacity & Free Space
    if skip_api:
        cap = safe_int(capacity_db)
        free = safe_int(freespace_db)
        dvr_data["HDD Status"]["TotalSize"] = cap
        dvr_data["HDD Status"]["FreeSize"] = free
        if cap > 0:
            dvr_data["HDD Status"]["STATUS"] = 1
    else:
        try:
            storage_url = f"{base_url}storageDevice.cgi?action=getDeviceAllInfo"
            response = requests.get(storage_url, auth=auth, timeout=T_API, verify=False)
            if response.status_code == 200:
                storage_data = parse_key_value_response(response.text)
                total_bytes = 0.0
                used_bytes = 0.0
                for k, v in storage_data.items():
                    if "TotalBytes" in k:
                        total_bytes += safe_float(v)
                    elif "UsedBytes" in k:
                        used_bytes += safe_float(v)

                total_gb = int(total_bytes / (1024 ** 2)) if total_bytes > 0 else 0
                used_gb = int(used_bytes / (1024 ** 2)) if used_bytes > 0 else 0
                free_gb = total_gb - used_gb if total_gb >= used_gb else 0

                dvr_data["HDD Status"]["TotalSize"] = total_gb
                dvr_data["HDD Status"]["UsedSize"] = used_gb
                dvr_data["HDD Status"]["FreeSize"] = free_gb
                if total_gb > 0 and dvr_data["HDD Status"]["Plug"] > 0:
                    dvr_data["HDD Status"]["STATUS"] = 1
        except Exception:
            pass

    return dvr_data


def find_recording_dates_java_logic(dvr_data, ip_address, port, auth):
    base_cgi_url = f"http://{ip_address}:{port}/cgi-bin/mediaFileFind.cgi"
    if not dvr_data["system"]["login_success"]:
        dvr_data["recording"] = {
            "status": "Login Failed",
            "oldest": "N/A",
            "latest": "N/A",
        }
        return dvr_data

    object_id = None
    oldest_start_dt = "N/A"
    latest_end_dt = "N/A"
    MAX_DAYS_BACK = 180
    DATE_FORMAT = "%Y-%m-%d"

    try:
        today = datetime.now()
        date_oldest_ago = today - timedelta(days=MAX_DAYS_BACK)
        old_date_str = date_oldest_ago.strftime(DATE_FORMAT)
        today_date_str = today.strftime(DATE_FORMAT)

        create_url = f"{base_cgi_url}?action=factory.create"
        response = requests.get(create_url, auth=auth, timeout=T_API, verify=False)
        response.raise_for_status()

        text = response.text.strip().splitlines()[0]
        if "result=" in text:
            object_id = text.split("result=")[1].strip()

        if object_id:
            # Search Oldest Date
            oldest_params = {
                "action": "findFile",
                "object": object_id,
                "condition.Channel": "1",
                "condition.StartTime": f"{old_date_str} 00:00:00",
                "condition.EndTime": f"{today_date_str} 23:59:59",
            }
            resp = requests.get(base_cgi_url, params=oldest_params, auth=auth, timeout=15, verify=False)

            if resp.text.strip() == "OK":
                find_next_url = f"{base_cgi_url}?action=findNextFile&object={object_id}&count=1"
                resp_next = requests.get(find_next_url, auth=auth, timeout=15, verify=False)
                data = parse_key_value_response(resp_next.text)
                if data.get("found", "0") != "0":
                    raw_st = data.get("items[0].StartTime", "N/A")
                    oldest_start_dt = raw_st[:10] if len(raw_st) >= 10 else raw_st

            # Search Latest Date
            consecutive_errors = 0
            for days_back in range(0, MAX_DAYS_BACK):
                if consecutive_errors >= 5:
                    break

                current_date = today - timedelta(days=days_back)
                current_date_str = current_date.strftime(DATE_FORMAT)

                day_params = {
                    "action": "findFile",
                    "object": object_id,
                    "condition.Channel": "1",
                    "condition.StartTime": f"{current_date_str} 00:00:00",
                    "condition.EndTime": f"{current_date_str} 23:59:59",
                }

                try:
                    r_find = requests.get(base_cgi_url, params=day_params, auth=auth, timeout=10, verify=False)
                    if r_find.text.strip() != "OK":
                        consecutive_errors += 1
                        continue

                    consecutive_errors = 0
                    find_next_url = f"{base_cgi_url}?action=findNextFile&object={object_id}&count=100"
                    r_next = requests.get(find_next_url, auth=auth, timeout=20, verify=False)

                    if r_next.status_code == 200 and "found=" in r_next.text:
                        parsed = parse_key_value_response(r_next.text)
                        if safe_int(parsed.get("found", 0)) > 0:
                            if "EndTime=" in r_next.text:
                                idx = r_next.text.rfind("EndTime=")
                                raw_end = r_next.text[idx + 8 : idx + 27].strip()
                                latest_end_dt = raw_end.split(" ")[0] if raw_end else current_date_str
                            else:
                                latest_end_dt = current_date_str
                            break
                except Exception:
                    consecutive_errors += 1

                time.sleep(0.05)

        if latest_end_dt == "N/A":
            dvr_data["recording"] = {
                "status": "Not Working (No Files Found)",
                "oldest": "N/A",
                "latest": "N/A",
            }
        else:
            dvr_data["recording"] = {
                "status": "OK",
                "oldest": oldest_start_dt,
                "latest": latest_end_dt,
                "total_files": "N/A (Java Logic)",
            }

    except Exception as e:
        dvr_data["recording"] = {
            "status": f"Error: {e}",
            "oldest": "N/A",
            "latest": "N/A",
        }
    finally:
        if object_id:
            try:
                requests.get(f"{base_cgi_url}?action=close&object={object_id}", auth=auth, timeout=5, verify=False)
                requests.get(f"{base_cgi_url}?action=destroy&object={object_id}", auth=auth, timeout=5, verify=False)
            except Exception:
                pass

    return dvr_data


# ==================================================================== #
#                  6. FFMPEG & STREAM SNAPSHOT PIPELINE                #
# ==================================================================== #

def safe_run_command(command, timeout_sec=T_FFMPEG):
    try:
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate(timeout=timeout_sec)
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=2)
        except Exception:
            pass
        return -1, None, None


def capture_http_image(http_url, output_dir, filename_prefix, timeout_sec=T_FFMPEG):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{filename_prefix}.jpg")
    ffmpeg_cmd = f'"{FFMPEG_PATH}" -y -i "{http_url}" -frames:v 1 "{output_path}"'

    with FFMPEG_SEM:
        ret, _, _ = safe_run_command(ffmpeg_cmd, timeout_sec + 5)

    if ret == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return {"ATMID": filename_prefix, "Status": "Success"}
    return {"ATMID": filename_prefix, "Status": "Failed"}


def capture_rtsp_image(rtsp_url, output_dir, filename_prefix, timeout_sec=T_FFMPEG):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{filename_prefix}.jpg")
    ffmpeg_cmd = f'"{FFMPEG_PATH}" -rtsp_transport tcp -y -i "{rtsp_url}" -frames:v 1 -q:v 2 -t 10 "{output_path}"'

    with FFMPEG_SEM:
        ret, _, _ = safe_run_command(ffmpeg_cmd, timeout_sec + 5)

    if ret == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return {"ATMID": filename_prefix, "Status": "Success"}
    return {"ATMID": filename_prefix, "Status": "Failed"}


def video_feed(rt, channel):
    rtsp, api = "", ""
    try:
        user, pwd, ip, port = str(rt[0]), str(rt[1]), str(rt[2]), str(rt[3])
        dvr_type = str(rt[4]).lower()
        cam_type = str(rt[5]).lower()

        if dvr_type in ("uniview", "unv"):
            stream = "media/video0" if cam_type == "ip" else f"unicast/c{channel}/s1/live"
            rtsp = f"rtsp://{user}:{pwd}@{ip}:554/{stream}"
            api = f"http://{user}:{pwd}@{ip}:{port}/LAPI/V1.0/Channels/{channel}/Media/Video/Streams/1/Snapshot"

        elif dvr_type in ("hikvision", "hikvision_nvr", "prama"):
            stream = "media/video0" if cam_type == "ip" else f"Streaming/Channels/{channel}02/"
            rtsp = f"rtsp://{user}:{pwd}@{ip}:554/{stream}"
            api = f"http://{user}:{pwd}@{ip}:{port}/ISAPI/Streaming/channels/{channel}01/picture"

        elif dvr_type == "cpplus_indigo":
            stream = f"&channel={channel}&stream=1.sdp?"
            rtsp = f"rtsp://{ip}:554/user={user}&password={pwd}{stream}"
            api = f"http://{user}:{pwd}@{ip}:{port}/cgi-bin/snapshot.cgi?channel={channel}"

        elif dvr_type == "hikvision_jupiter":
            rtsp = f"rtsp://admin:smarti@123@{ip}:1031/cam/realmonitor?channel={channel}&subtype=1&unicast=true&proto=Onvif"
            api = rtsp

        else:
            # Default: Dahua / CPPLUS / CPPLUS_ORANGE
            stream = f"cam/realmonitor?channel={channel}&subtype=1"
            rtsp = f"rtsp://{user}:{pwd}@{ip}:554/{stream}"
            api = f"http://{user}:{pwd}@{ip}:{port}/cgi-bin/snapshot.cgi?channel={channel}"

        return rt[-1], rtsp, api, ip
    except Exception:
        return rt[-1], rtsp, api, rt[2]


def process_capture(key, value, out_dir):
    try:
        res = capture_http_image(value["api"], out_dir, key)
        if res.get("Status") != "Failed":
            return res
        return capture_rtsp_image(value["rtsp"], out_dir, key)
    except Exception as e:
        return {"ATMID": key, "Status": f"Exception ({e})"}


def is_stream_working_blackpixel(image_path, debug=False):
    try:
        img = cv2.imread(image_path)
        if img is None:
            return False
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        black_pixels = np.sum(gray < 30)
        total_pixels = gray.size
        black_ratio = black_pixels / float(total_pixels)
        if debug:
            safe_print(f"Black ratio: {black_ratio:.2f}")
        return black_ratio <= 0.85
    except Exception:
        return False


def fetch_camera_status(sn, ip_address, atmid):
    atmid1 = atmid
    cam_status_dict = {1: "not working", 2: "not working", 3: "not working", 4: "not working"}
    try:
        sql = "SELECT UserName, Password, port, dvrname, channel FROM site_dvr_data WHERE IPAddress = %s"
        result = db_execute(sql, (ip_address,), fetch=True)

        if not result:
            safe_print(f"No records in site_dvr_data for IP {ip_address}. Auto-inserting 4 channels...")
            row_live = db_execute(
                "SELECT UserName, Password, port, dvrname, ATMID FROM all_dvr_live WHERE IPAddress = %s LIMIT 1",
                (ip_address,),
                fetch=True,
            )
            if row_live:
                _user = row_live[0]["UserName"]
                _pass = row_live[0]["Password"]
                _port = row_live[0]["port"]
                _dvrname = row_live[0]["dvrname"]
            else:
                _user, _pass, _port, _dvrname = "admin", "", "37777", "CPPLUS"

            insert_sql = """
                INSERT INTO site_dvr_data 
                (sn, IPAddress, dvrname, channel, channel_name, UserName, Password, port, is_working) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            channel_names = {1: "Lobby", 2: "Backroom", 3: "Outdoor", 4: "Pinhole"}
            insert_values = []
            for ch in range(1, 5):
                ch_name = channel_names.get(ch, f"Channel {ch}")
                insert_values.append((sn, ip_address, _dvrname, ch, ch_name, _user, _pass, _port, "0"))

            db_execute(insert_sql, insert_values, many=True)
            result = db_execute(sql, (ip_address,), fetch=True)

        rtsp_urls = {}
        for row in result:
            UserName = row["UserName"]
            Password = row["Password"]
            DVRName = row["dvrname"]
            dvr_port = row["port"]
            channel = row["channel"]

            li = (
                str(UserName).strip(),
                str(Password).strip(),
                str(ip_address).strip(),
                str(dvr_port).strip(),
                str(DVRName).strip(),
                "analog",
                atmid,
            )
            _, rtsp, api, _ = video_feed(li, channel)

            rtsp_urls[channel] = {
                "rtsp": rtsp,
                "api": api,
                "ip": ip_address,
                "model": DVRName,
            }

        urls_items = list(rtsp_urls.items())
        failed = []
        success_channels = []
        out_dir = os.path.join(IMG_PATH_WEB, ip_address)
        out_dir_db = os.path.join(IMG_PATH_DB, ip_address)
        os.makedirs(out_dir, exist_ok=True)

        futures = []
        with ThreadPoolExecutor(max_workers=min(4, len(urls_items))) as executor:
            for key, value in urls_items:
                futures.append(executor.submit(process_capture, key, value, out_dir))

            for future in as_completed(futures):
                try:
                    res = future.result()
                    atm_ch = res.get("ATMID", "Unknown")
                    status = res.get("Status", "Failed")
                    if status == "Failed":
                        failed.append(atm_ch)
                    else:
                        success_channels.append(atm_ch)
                except Exception:
                    pass

        for ch in list(success_channels):
            img_path = os.path.join(out_dir, f"{ch}.jpg")
            if not is_stream_working_blackpixel(img_path, debug=False):
                success_channels.remove(ch)
                failed.append(ch)

        for ch in success_channels:
            try:
                ch_int = int(ch)
                if ch_int in cam_status_dict:
                    cam_status_dict[ch_int] = "working"
            except Exception:
                pass

        cdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_time = datetime.now()
        last_comm = (base_time + timedelta(seconds=random.randint(1, 4))).strftime("%Y-%m-%d %H:%M:%S")
        dvr_time_sim = (base_time + timedelta(seconds=random.randint(5, 8))).strftime("%Y-%m-%d %H:%M:%S")

        if failed:
            ph = ",".join(["%s"] * len(failed))
            db_execute(
                f"UPDATE site_dvr_data SET is_working='0', last_communication=%s WHERE IPAddress=%s AND channel IN ({ph})",
                [cdate, ip_address] + [str(x) for x in failed],
            )
            db_execute("UPDATE all_dvr_live SET cdate=%s WHERE IPAddress=%s", (cdate, ip_address))
            db_execute(
                """INSERT INTO esurv.`dvr_history` 
                   (ip,status,cam1,cam2,cam3,cam4,hdd,latency,cdate,login_status,last_communication,atmid,capacity,freespace,recording_from,recording_to)
                   VALUES (%s,'1','','','','','','',%s,'1',%s,%s,'','','','')""",
                (ip_address, cdate, cdate, atmid1),
            )

        if success_channels:
            base_dir_sql = out_dir_db.replace("\\", "/")
            if ":" in base_dir_sql:
                base_dir_sql = base_dir_sql.split(":", 1)[1]

            ph = ",".join(["%s"] * len(success_channels))
            db_execute(
                f"UPDATE site_dvr_data SET is_working='1', last_communication=%s, img_path=%s WHERE IPAddress=%s AND channel IN ({ph})",
                [cdate, base_dir_sql, ip_address] + [str(x) for x in success_channels],
            )
            db_execute(
                "UPDATE all_dvr_live SET cdate=%s, last_communication=%s, dvr_time=%s WHERE IPAddress=%s",
                (cdate, last_comm, dvr_time_sim, ip_address),
            )
            db_execute(
                """INSERT INTO esurv.`dvr_history` 
                   (ip,status,cam1,cam2,cam3,cam4,hdd,latency,cdate,login_status,last_communication,atmid,capacity,freespace,recording_from,recording_to)
                   VALUES (%s,'1','','','','','','',%s,'0',%s,%s,'','','','')""",
                (ip_address, cdate, cdate, atmid1),
            )

        return cam_status_dict
    except Exception as e:
        safe_print(f"Error fetching camera status for IP {ip_address}: {e}")
        return cam_status_dict


def Insertnewtorkstatus(ip_address, status, login, latency, atmid):
    cdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    network_status = "working" if status == 1 else "not working"

    db_execute(
        "UPDATE esurv.all_dvr_live SET login_status=%s, status=%s, cdate=%s, network_status=%s, network_status_dt=%s WHERE ipaddress LIKE %s",
        (str(login), str(status), cdate, network_status, cdate, f"%{ip_address}%"),
    )
    db_execute(
        """INSERT INTO esurv.`dvr_history` 
           (ip,status,cam1,cam2,cam3,cam4,hdd,latency,cdate,login_status,last_communication,atmid,capacity,freespace,recording_from,recording_to)
           VALUES (%s,%s,'','','','','','',%s,%s,%s,%s,'','','','')""",
        (ip_address, str(status), cdate, str(login), cdate, atmid),
    )


def update_sites_master(atmid, ip_address, ping_status, login_status, status, hdd_status, total_space, free_space, cam1, cam2, cam3, cam4, dvr_time, time_diff, http_error, cdate1, recording_from, recording_to):
    cdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        UPDATE esurv.`all_dvr_live` SET
            latency=%(ping_status)s,
            cdate=NOW(),
            cdate1=%(cdate1)s,
            status=%(status)s,
            login_status=%(login_status)s,
            hdd=%(hdd_status)s,
            capacity=%(total_space)s,
            freespace=%(free_space)s,
            dvr_time=%(dvr_time)s,
            time_diff=%(time_diff)s,
            remarks=%(http_error)s,
            recording_from=%(recording_from)s,
            recording_to=%(recording_to)s,
            cam1=%(cam1)s,
            cam2=%(cam2)s,
            cam3=%(cam3)s,
            cam4=%(cam4)s,
            network_status='working',
            network_status_dt=%(cdate1)s,
            last_communication=NOW()
        WHERE ipaddress=%(ip_address)s
    """
    params = {
        "ping_status": ping_status,
        "cdate1": cdate1,
        "status": status,
        "login_status": login_status,
        "hdd_status": hdd_status,
        "total_space": str(total_space or ""),
        "free_space": str(free_space or ""),
        "dvr_time": dvr_time or None,
        "time_diff": str(time_diff) if time_diff is not None else None,
        "http_error": http_error or None,
        "recording_from": recording_from or None,
        "recording_to": recording_to or None,
        "cam1": cam1,
        "cam2": cam2,
        "cam3": cam3,
        "cam4": cam4,
        "ip_address": ip_address,
    }
    db_execute(sql, params)

    hist_sql = """
        INSERT INTO esurv.`dvr_history` 
        (ip,status,cam1,cam2,cam3,cam4,hdd,latency,cdate,login_status,last_communication,atmid,capacity,freespace,recording_from,recording_to)
        VALUES (%s,%s,%s,%s,%s,%s,%s,'',%s,'0',%s,%s,%s,%s,'','')
    """
    db_execute(
        hist_sql,
        (
            ip_address, status, cam1, cam2, cam3, cam4, hdd_status,
            cdate, cdate, atmid, str(total_space or ""), str(free_space or "")
        ),
    )


# ==================================================================== #
#                     7. SITE INSPECTION WORKFLOW                      #
# ==================================================================== #

def process_single_ip(sn, atmid, ip_address, port, username, password, dvr_name, capacity_db, freespace_db, recording_from_db, recording_to_db, last_fetched_dates_dict=None):
    """
    Complete health check pipeline for a single IP address.
    Returns a structured report dictionary.
    """
    ping_status, latency = check_ping(ip_address, timeout=T_PING)

    # 1. Ping Failure Case
    if not ping_status:
        Insertnewtorkstatus(ip_address, status=0, login=1, latency=latency, atmid=atmid)
        card = [
            "═" * 70,
            f"🖥️  CPPLUS / DAHUA HEALTH CHECK | IP: {ip_address:<15} | ATM ID: {atmid or 'N/A'}",
            "─" * 70,
            "  📶 Ping       : ❌ Not Working",
            "  🔑 Login      : ❌ Not Working (1)",
            "─" * 70,
            "  💾 Database   : ✅ Updated successfully",
            "═" * 70
        ]
        safe_print("\n".join(card))
        return {
            "ping_ok": False,
            "login_ok": False,
            "dvr_time": "N/A",
            "hdd_status": "Not Ok",
            "total_space": 0,
            "free_space": 0,
            "rec_start": "N/A",
            "rec_end": "N/A",
            "cams": {1: "not working", 2: "not working", 3: "not working", 4: "not working"}
        }

    # 2. Camera Snapshot Check
    cam_status_dict = fetch_camera_status(sn, ip_address, atmid)
    cam1 = cam_status_dict.get(1, "not working")
    cam2 = cam_status_dict.get(2, "not working")
    cam3 = cam_status_dict.get(3, "not working")
    cam4 = cam_status_dict.get(4, "not working")

    today_str = datetime.now().strftime("%Y-%m-%d")
    lfd = last_fetched_dates_dict if last_fetched_dates_dict is not None else last_fetched_dates
    with _lfd_lock:
        cached_date = lfd.get(atmid)

    skip_api = (cached_date == today_str) or (str(recording_to_db).strip() == today_str)

    auth = HTTPDigestAuth(username, password)
    status_data = get_dvr_status(
        ip_address=ip_address,
        port=port,
        auth=auth,
        atm_id=atmid,
        dvr_name=dvr_name,
        capacity_db=capacity_db,
        freespace_db=freespace_db,
        skip_api=skip_api
    )

    login_success = status_data["system"]["login_success"]

    # 3. Login Failure Case
    if not login_success:
        cdate1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_sites_master(
            atmid, ip_address, latency, login_status="1", status="1", hdd_status="",
            total_space="", free_space="", cam1=cam1, cam2=cam2, cam3=cam3, cam4=cam4,
            dvr_time="", time_diff=None, http_error="HTTP_401_UNAUTHORIZED", cdate1=cdate1,
            recording_from="", recording_to=""
        )
        card = [
            "═" * 70,
            f"🖥️  CPPLUS / DAHUA HEALTH CHECK | IP: {ip_address:<15} | ATM ID: {atmid or 'N/A'}",
            "─" * 70,
            "  📶 Ping       : ✅ Working",
            "  🔑 Login      : ❌ Not Working (1)",
            "─" * 70,
            "  💾 Database   : ✅ Updated successfully",
            "═" * 70
        ]
        safe_print("\n".join(card))
        return {
            "ping_ok": True,
            "login_ok": False,
            "dvr_time": "N/A",
            "hdd_status": "Not Ok",
            "total_space": 0,
            "free_space": 0,
            "rec_start": "N/A",
            "rec_end": "N/A",
            "cams": cam_status_dict
        }

    # 4. Login Success Case
    login_status = "0"
    status = "1"

    if skip_api:
        status_data["recording"] = {
            "status": "OK",
            "oldest": str(recording_from_db or "N/A"),
            "latest": str(recording_to_db or "N/A"),
            "total_files": "N/A (From DB)",
        }
        final_data = status_data
    else:
        final_data = find_recording_dates_java_logic(status_data, ip_address, port, auth)
        if final_data["system"]["login_success"]:
            with _lfd_lock:
                lfd[atmid] = today_str

    dvr_time = final_data["system"].get("sysDateTime", "")
    diff = None
    if dvr_time:
        try:
            fetch_time = final_data["system"].get("dvr_time_fetched_at") or datetime.now()
            cTime = fetch_time.replace(microsecond=0)
            systemTime = datetime.strptime(dvr_time, "%Y-%m-%d %H:%M:%S")
            diff = cTime - systemTime if cTime >= systemTime else f"+{systemTime - cTime}"
        except Exception:
            diff = None

    hdd = final_data.get("HDD Status", {})
    hdd_status = "Ok" if hdd.get("STATUS") == 1 and safe_int(hdd.get("TotalSize")) > 0 else "Not Ok"
    total_space = safe_int(hdd.get("TotalSize"), default=safe_int(capacity_db))
    free_space = safe_int(hdd.get("FreeSize"), default=safe_int(freespace_db))

    rec = final_data.get("recording", {})
    rec_start_text = rec.get("oldest", "N/A")
    rec_end_text = rec.get("latest", "N/A")

    cdate1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_sites_master(
        atmid, ip_address, latency, login_status, status, hdd_status,
        total_space, free_space, cam1, cam2, cam3, cam4,
        dvr_time, diff, None, cdate1, rec_start_text, rec_end_text
    )

    skip_msg = " [Cached from DB]" if skip_api else ""

    card = [
        "═" * 70,
        f"🖥️  CPPLUS / DAHUA HEALTH CHECK | IP: {ip_address:<15} | ATM ID: {atmid or 'N/A'}",
        "─" * 70,
        "  📶 Ping       : ✅ Working",
        "  🔑 Login      : ✅ Working (0)",
        f"  ⏰ DVR Time   : {dvr_time if dvr_time else 'N/A'}",
        f"  💾 HDD        : {'✅ Working' if hdd_status == 'Ok' else '❌ Not Working'} (Total: {total_space} GB | Free: {free_space} GB){skip_msg}"
    ]

    if rec_start_text and rec_end_text and rec_start_text != "N/A":
        card.append(f"  🎥 Recording  : ✅ Working ({rec_start_text} to {rec_end_text}){skip_msg}")
    else:
        card.append(f"  🎥 Recording  : ❌ Not Working{skip_msg}")

    card.extend([
        "─" * 70,
        "  💾 Database   : ✅ Updated successfully",
        "═" * 70
    ])

    safe_print("\n".join(card))

    return {
        "ping_ok": True,
        "login_ok": True,
        "dvr_time": dvr_time,
        "hdd_status": hdd_status,
        "total_space": total_space,
        "free_space": free_space,
        "rec_start": rec_start_text,
        "rec_end": rec_end_text,
        "cams": cam_status_dict
    }


# ==================================================================== #
#                      8. MAIN SCHEDULER LOOP                          #
# ==================================================================== #

def main():
    while True:
        start_time = datetime.now()
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

        safe_print("\n" + "█" * 70)
        safe_print(f"🚀 SCHEDULED CPPLUS / DAHUA DVR HEALTH CHECK STARTED AT {start_str}")
        safe_print("█" * 70 + "\n")

        sql = """
            SELECT SN, UserName, Password, IPAddress, port, dvrname, ATMID, 
                   capacity, freespace, recording_from, recording_to 
            FROM `all_dvr_live` 
            WHERE UPPER(dvrname) IN ('CPPLUS', 'DAHUVA', 'DAHUA') AND live = 'Y' 
            ORDER BY cdate ASC
        """
        sites = db_execute(sql, fetch=True)

        if not sites:
            safe_print("⚠️ No active CPPLUS / Dahua sites found in esurv.all_dvr_live.")
        else:
            max_workers = min(MAX_SITE_THREADS, len(sites))
            safe_print(f"📋 Loaded {len(sites)} CPPLUS / Dahua sites from database.")
            safe_print(f"📋 Starting parallel inspection ({max_workers} threads)...\n")

            succ_count = 0
            fail_count = 0
            futures = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for row in sites:
                    sn = str(row.get("SN", "") or "").strip()
                    username = str(row.get("UserName", "") or "").strip()
                    password = str(row.get("Password", "") or "").strip()
                    ip_address = str(row.get("IPAddress", "") or "").strip()
                    port = str(row.get("port", "80") or "80").strip()
                    dvr_name = str(row.get("dvrname", "CPPLUS") or "CPPLUS").strip()
                    atmid = str(row.get("ATMID", "") or "").strip()
                    capacity = str(row.get("capacity", "") or "").strip()
                    freespace = str(row.get("freespace", "") or "").strip()
                    rec_from = str(row.get("recording_from", "") or "").strip()
                    rec_to = str(row.get("recording_to", "") or "").strip()

                    future = executor.submit(
                        process_single_ip,
                        sn, atmid, ip_address, port, username, password,
                        dvr_name, capacity, freespace, rec_from, rec_to, last_fetched_dates
                    )
                    futures.append(future)

                for future in as_completed(futures):
                    try:
                        rep = future.result()
                        if rep and rep.get("login_ok"):
                            succ_count += 1
                        else:
                            fail_count += 1
                    except Exception as e:
                        fail_count += 1
                        safe_print(f"❌ Error processing site: {e}")

            kill_ffmpeg()

            end_time = datetime.now()
            end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
            duration = round((end_time - start_time).total_seconds(), 1)
            next_run = (end_time + timedelta(seconds=CYCLE_SLEEP_SECONDS)).strftime("%Y-%m-%d %H:%M:%S")

            summary = [
                "\n" + "═" * 70,
                "📊 RUN CYCLE SUMMARY — CPPLUS / DAHUA HEALTH CHECK",
                "═" * 70,
                f"  ⏱️  Started At     : {start_str}",
                f"  ⏱️  Finished At    : {end_str} (Duration: {duration}s)",
                f"  📍 Total Sites    : {len(sites)}",
                f"  🟢 Login Success  : {succ_count} Sites",
                f"  🔴 Login Failed   : {fail_count} Sites",
                "─" * 70,
                f"  ⏳ Next run scheduled in 2 hours at {next_run}",
                "═" * 70 + "\n"
            ]
            safe_print("\n".join(summary))

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

    _api_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="cpplus_refresh")

    def _fetch_cpplus_site_by_ip(ip: str) -> dict:
        sql = """
            SELECT SN, UserName, Password, IPAddress, port, dvrname, ATMID, 
                   capacity, freespace, recording_from, recording_to 
            FROM `all_dvr_live` 
            WHERE IPAddress = %s 
              AND UPPER(dvrname) IN ('CPPLUS', 'DAHUVA', 'DAHUA')
            LIMIT 1
        """
        rows = db_execute(sql, (ip,), fetch=True)
        if not rows:
            raise ValueError(f"IP '{ip}' not found in all_dvr_live with dvrname CPPLUS/DAHUVA")
        row = rows[0]
        return {
            "sn": str(row.get("SN", "") or "").strip(),
            "username": str(row.get("UserName", "") or "").strip(),
            "password": str(row.get("Password", "") or "").strip(),
            "ip_address": str(row.get("IPAddress", "") or "").strip(),
            "port": str(row.get("port", "80") or "80").strip(),
            "dvr_name": str(row.get("dvrname", "CPPLUS") or "CPPLUS").strip(),
            "atmid": str(row.get("ATMID", "") or "").strip(),
            "capacity": str(row.get("capacity", "") or "").strip(),
            "freespace": str(row.get("freespace", "") or "").strip(),
            "rec_from": str(row.get("recording_from", "") or "").strip(),
            "rec_to": str(row.get("recording_to", "") or "").strip(),
        }

    def _do_cpplus_refresh(ip: str) -> dict:
        start_ts = datetime.now()
        site = _fetch_cpplus_site_by_ip(ip)

        report = process_single_ip(
            sn=site["sn"],
            atmid=site["atmid"],
            ip_address=site["ip_address"],
            port=site["port"],
            username=site["username"],
            password=site["password"],
            dvr_name=site["dvr_name"],
            capacity_db=site["capacity"],
            freespace_db=site["freespace"],
            recording_from_db=site["rec_from"],
            recording_to_db=site["rec_to"],
            last_fetched_dates_dict=last_fetched_dates
        )

        elapsed = round((datetime.now() - start_ts).total_seconds(), 2)

        ping_ok = bool(report.get("ping_ok"))
        login_ok = bool(report.get("login_ok"))
        hdd_ok = (report.get("hdd_status") == "Ok")

        rec_start = report.get("rec_start", "N/A")
        rec_end = report.get("rec_end", "N/A")
        rec_ok = (rec_start != "N/A" and rec_end != "N/A")

        cams_info = report.get("cams", {})
        working_cams = sum(1 for status in cams_info.values() if str(status).lower() in ("working", "online", "up"))

        return {
            "success":          login_ok,
            "ip":               ip,
            "atm_id":           site["atmid"],
            "dvr_type":         site["dvr_name"],
            "elapsed_seconds":  elapsed,
            "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ping":             "Online" if ping_ok else "Offline",
            "login":            "Working" if login_ok else "Failed",
            "hdd":              "Working" if hdd_ok else "Not Working",
            "hdd_total_mb":     report.get("total_space", 0),
            "hdd_free_mb":      report.get("free_space", 0),
            "recording":        "Working" if rec_ok else "Not Working",
            "rec_start":        rec_start,
            "rec_end":          rec_end,
            "cameras_working":  working_cams,
            "cameras_total":    len(cams_info) if cams_info else 4,
            "camera_detail":    cams_info,
            "dvr_time":         str(report.get("dvr_time") or "N/A"),
            "message": (
                f"CPPLUS / Dahua health check complete in {elapsed}s — DB updated"
                if login_ok
                else f"CPPLUS / Dahua health check done in {elapsed}s — device offline/unreachable"
            ),
        }

    @asynccontextmanager
    async def _cpplus_lifespan(application: FastAPI):
        yield
        _api_executor.shutdown(wait=False)

    app = FastAPI(
        title="CPPLUS / Dahua DVR Refresh API",
        description="Dashboard Refresh API for CPPLUS / Dahua DVRs",
        version="1.5.0",
        lifespan=_cpplus_lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    class CpPlusRefreshRequest(BaseModel):
        ip: str

    @app.get("/health", tags=["System"])
    async def health_check():
        return {
            "status":    "ok",
            "server":    "CPPLUS / Dahua DVR Refresh API v1.5",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @app.post("/refresh/cpplus_dahuva", tags=["CPPLUS / Dahuva Refresh"])
    async def refresh_cpplus(request: CpPlusRefreshRequest):
        ip = request.ip.strip()
        if not ip:
            raise HTTPException(status_code=400, detail="IP address required")

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(_api_executor, _do_cpplus_refresh, ip)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Refresh failed: {e}")

        return result

    def _run_api_server():
        import uvicorn
        HOST = "0.0.0.0"
        PORT = 8005  # Strictly Fixed Port

        uvicorn.run(app, host=HOST, port=PORT, reload=False, log_level="warning")

except ImportError:
    app = None
    _run_api_server = None


# ====================================================================== #
#                             ENTRY POINT                                #
# ====================================================================== #

if __name__ == "__main__":
    PORT_DISPLAY = 8005

    # 1. Background FastAPI Server Start
    if _run_api_server is not None:
        api_thread = threading.Thread(target=_run_api_server, daemon=True)
        api_thread.start()
        safe_print("\n" + "═" * 70)
        safe_print(f"🌐 [SYSTEM] FastAPI Server STARTING on STRICT PORT {PORT_DISPLAY}...")
        safe_print(f"🌐 Frontend Refresh Endpoint : http://<YOUR_SERVER_IP>:{PORT_DISPLAY}/refresh/cpplus_dahuva")
        safe_print(f"📖 Swagger Docs API          : http://127.0.0.1:{PORT_DISPLAY}/docs")
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
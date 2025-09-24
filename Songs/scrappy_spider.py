# scap_min_fix.py
import csv, time, tempfile, shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import SessionNotCreatedException

SITE_URL   = "https://mp3juice.co/"
CSV_PATH = Path("my_song_list.csv")  # must have 'track_name' and optional 'test_names'/'test_name'
OUTPUT_CSV = Path("log.csv")
DOWNLOADS = Path("downloads").resolve()
WAIT       = 45
HEADLESS   = False
CONCURRENCY = 4  # adjust to 16+ if your machine and the site can handle it
def build_driver(download_dir: Path | None = None):
    base_dir = download_dir or DOWNLOADS
    base_dir.mkdir(parents=True, exist_ok=True)
    snap_common = Path.home() / "snap" / "chromium" / "common"
    cache_root = (snap_common if snap_common.exists() else Path.home() / ".cache") / "selenium_profiles"
    cache_root.mkdir(parents=True, exist_ok=True)
    user_data = None

    opts = Options()
    # Point to your system Chromium if needed:
    for p in ("/usr/bin/chromium-browser", "/usr/bin/chromium", "/snap/bin/chromium"):
        if Path(p).exists(): opts.binary_location = p; break

    if HEADLESS:
        opts.add_argument("--headless=new")
    # keep flags minimal
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument(f"--user-data-dir={user_data}")
    opts.add_argument("--remote-debugging-port=0")  # avoids DevToolsActivePort issue
    # avoid first-run and default-browser interstitials that can trap us on a start page
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-search-engine-choice-screen")
    opts.add_argument("--password-store=basic")
    opts.add_argument("--use-mock-keychain")

    # silent downloads
    opts.add_experimental_option("prefs", {
        # Downloads
        "download.default_directory": str(DOWNLOADS),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1,  # allow multiple downloads
        # Permissions: always allow notifications so no prompt appears
        "profile.default_content_setting_values.notifications": 1,
        "profile.managed_default_content_settings.notifications": 1,
    })

    service = ChromeService()  # Selenium Manager fetches chromedriver
    # retry up to 3 times with fresh user-data-dirs if we hit profile lock
    last_err = None
    for attempt in range(3):
        try:
            if user_data is not None and Path(user_data).exists():
                shutil.rmtree(user_data, ignore_errors=True)
            user_data = tempfile.mkdtemp(prefix="chromium_profile_", dir=str(cache_root))
            # refresh the argument for user-data-dir
            try:
                # remove any previous flag occurrences by rebuilding Options
                opts2 = Options()
                for p in ("/usr/bin/chromium-browser", "/usr/bin/chromium", "/snap/bin/chromium"):
                    if Path(p).exists(): opts2.binary_location = p; break
                if HEADLESS:
                    opts2.add_argument("--headless=new")
                opts2.add_argument("--no-sandbox")
                opts2.add_argument("--disable-dev-shm-usage")
                opts2.add_argument("--window-size=1280,900")
                opts2.add_argument(f"--user-data-dir={user_data}")
                opts2.add_argument("--remote-debugging-port=0")
                opts2.add_argument("--no-first-run")
                opts2.add_argument("--no-default-browser-check")
                opts2.add_argument("--disable-search-engine-choice-screen")
                opts2.add_argument("--password-store=basic")
                opts2.add_argument("--use-mock-keychain")
                opts2.add_experimental_option("prefs", {
                    "download.default_directory": str(base_dir),
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True,
                    "safebrowsing.enabled": True,
                    "profile.default_content_setting_values.automatic_downloads": 1,
                    "profile.default_content_setting_values.notifications": 1,
                    "profile.managed_default_content_settings.notifications": 1,
                })
                d = webdriver.Chrome(service=service, options=opts2)
                d._temp_user_data = user_data
                return d
            except SessionNotCreatedException as e:
                last_err = e
                continue
        except SessionNotCreatedException as e:
            last_err = e
            time.sleep(1)
            continue
    raise last_err or RuntimeError("Failed to create Chrome session after retries")

def cleanup_driver(d):
    try: d.quit()
    finally:
        tmp = getattr(d, "_temp_user_data", None)
        if tmp and Path(tmp).exists(): shutil.rmtree(tmp, ignore_errors=True)

def _wait_ready(d, timeout=15):
    WebDriverWait(d, timeout).until(lambda drv: drv.execute_script("return document.readyState") == "complete")

def grant_notifications(d):
    """Grant notifications permission for the current top-level origin via CDP."""
    try:
        origin = d.execute_script("return location.origin")
    except Exception:
        origin = None
    if origin:
        try:
            d.execute_cdp_cmd("Browser.grantPermissions", {"origin": origin, "permissions": ["notifications"]})
        except Exception:
            pass

def wait_for_download(new_after_ts: float, timeout: int = 90, download_dir: Path | None = None) -> Path | None:
    """Track the download that starts after new_after_ts by following its .crdownload file.
    This ties a specific click to the specific partial file to avoid cross-thread confusion.
    """
    end = time.time() + timeout
    ddir = download_dir or DOWNLOADS
    candidate: Path | None = None
    # Phase 1: find the .crdownload that started for this click
    while time.time() < end:
        latest = None
        for p in ddir.glob('*.crdownload'):
            try:
                st = p.stat()
            except FileNotFoundError:
                continue
            if st.st_mtime >= new_after_ts:
                if latest is None or st.st_mtime > latest[1]:
                    latest = (p, st.st_mtime)
        if latest:
            candidate = latest[0]
            break
        time.sleep(0.2)
    if candidate is None:
        # Fallback: look for a new non-hidden, non-temp file created after click
        for p in ddir.glob('*'):
            if p.name.startswith('.') or p.suffix == '.crdownload':
                continue
            try:
                if p.stat().st_mtime >= new_after_ts and p.is_file():
                    return p
            except FileNotFoundError:
                pass
        return None
    # Phase 2: wait for that .crdownload to disappear, then return the final file
    target_stem = candidate.name[:-len('.crdownload')]
    while time.time() < end:
        if not candidate.exists():
            final = ddir / target_stem
            if final.exists() and final.is_file():
                return final
            # Some sites rename; return any new non-temp created after click
            for p in ddir.glob('*'):
                if p.name.startswith('.') or p.suffix == '.crdownload':
                    continue
                try:
                    if p.stat().st_mtime >= new_after_ts and p.is_file():
                        return p
                except FileNotFoundError:
                    continue
        time.sleep(0.3)
    return None

def force_nav(d, url):
    """Navigate robustly and make sure we're not stuck on the start page.
    We don't assume any particular element yet; just ensure load + correct origin.
    """
    targets = [
        ("direct", lambda: d.get(url)),
        ("js", lambda: (d.get("about:blank"), d.execute_script("location.href = arguments[0];", url))),
        ("new_tab", lambda: (d.switch_to.new_window("tab"), d.get(url))),
        ("cdp", lambda: (d.get("about:blank"), d.execute_cdp_cmd("Page.navigate", {"url": url}))),
    ]
    last_err = None
    for label, action in targets:
        try:
            action()
            _wait_ready(d, timeout=15)
            if "mp3juice" in (d.current_url or "").lower():
                return
        except Exception as e:
            last_err = e
            continue
    # if still not there, raise the last error for visibility
    raise last_err or RuntimeError("Failed to navigate to site")

def read_queries():
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            t = (row.get("track_name") or "").strip()
            u = (row.get("artist_name") or row.get("artist_names") or "").strip()
            if not t:
                continue
            yield f"{t} - {u}" if u else t

def simplify_query(q: str) -> str:
    """Return a shorter, safer query focusing on the track title only.
    Examples:
    "Song Title - Very Long Artist List" -> "Song Title"
    Trim extra qualifiers in parentheses/brackets and keep under ~90 chars.
    """
    base = q.split(" - ")[0].strip()
    # remove trailing qualifiers like (Live), [Remastered], etc.
    for sep in ["(", "["]:
        if sep in base:
            base = base.split(sep)[0].strip()
    if len(base) > 90:
        base = base[:90]
    return base

def close_new_tabs(d, baseline_handles: set[str], original_handle: str, grant_before_close: bool = True):
    """Close any newly opened tabs/windows not present in baseline_handles and
    return focus to original_handle.
    """
    try:
        current = set(d.window_handles)
        new_ones = [h for h in current if h not in baseline_handles]
        for h in new_ones:
            try:
                d.switch_to.window(h)
                if grant_before_close:
                    grant_notifications(d)
                d.close()
            except Exception:
                pass
        # ensure we're back on the original
        d.switch_to.window(original_handle)
    except Exception:
        pass

def process_worker(worker_id: int, tasks: list[tuple[int, str]]):
    """Persistent worker that reuses one browser and its download folder.
    tasks: list of (global_index, query)
    """
    worker_dir = DOWNLOADS / f"worker_{worker_id+1}"
    d = build_driver(download_dir=worker_dir)
    try:
        # small stagger to avoid stampede
        time.sleep(0.25 * worker_id)
        # If first task, navigate to home
        if tasks:
            force_nav(d, SITE_URL)
            grant_notifications(d)
        # helper to search on current page (home or results page)
        def search_here(q: str):
            input_candidates = [
                (By.ID, "q"),
                (By.NAME, "q"),
                (By.ID, "query"),
                (By.NAME, "query"),
                (By.CSS_SELECTOR, "input[type='search']"),
                (By.CSS_SELECTOR, "form input[type='text']"),
            ]
            box = None
            for how, what in input_candidates:
                try:
                    box = WebDriverWait(d, 5).until(EC.element_to_be_clickable((how, what)))
                    if box and box.is_displayed():
                        break
                except Exception:
                    continue
            if not box:
                # if no box found, ensure we're on home and try again
                force_nav(d, SITE_URL)
                return search_here(q)
            # limit extremely long queries and fallback if needed
            original_q = q
            if len(q) > 140:
                q = simplify_query(q)
            box.clear(); box.send_keys(q)
            # ensure the input actually has the text (site may debounce/clear)
            try:
                WebDriverWait(d, 2).until(lambda drv: (box.get_attribute("value") or "")[:3] == q[:3])
            except Exception:
                pass
            if (box.get_attribute("value") or "").strip() != q.strip():
                # force set value via JS and dispatch events
                d.execute_script(
                    "arguments[0].focus(); arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', {bubbles:true}));",
                    box, q,
                )
            time.sleep(0.15)
            # click submit
            submit_candidates = [
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//button[normalize-space()='Search']"),
                (By.XPATH, "//form//button"),
            ]
            clicked = False
            for how, what in submit_candidates:
                try:
                    btn = WebDriverWait(d, 5).until(EC.element_to_be_clickable((how, what)))
                    d.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                    btn.click()
                    clicked = True
                    break
                except Exception:
                    continue
            if not clicked:
                from selenium.webdriver.common.keys import Keys
                box.send_keys(Keys.ENTER)

            # If results do not appear quickly, retry once with simplified query
            try:
                WebDriverWait(d, 6).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".result")))
            except Exception:
                # Retry with a simplified query only if we didn't already simplify
                simp = simplify_query(original_q)
                if simp != q:
                    # try to find the input again on the current page
                    try:
                        box2 = None
                        for how, what in input_candidates:
                            try:
                                box2 = WebDriverWait(d, 3).until(EC.element_to_be_clickable((how, what)))
                                if box2 and box2.is_displayed():
                                    break
                            except Exception:
                                continue
                        if box2:
                            box2.clear(); box2.send_keys(simp)
                            for how, what in submit_candidates:
                                try:
                                    btn2 = WebDriverWait(d, 3).until(EC.element_to_be_clickable((how, what)))
                                    d.execute_script("arguments[0].scrollIntoView({block:'center'});", btn2)
                                    btn2.click(); break
                                except Exception:
                                    continue
                        else:
                            # navigate home and try again with simplified
                            force_nav(d, SITE_URL)
                            return search_here(simp)
                    except Exception:
                        # navigate home and try again if anything fails
                        force_nav(d, SITE_URL)
                        return search_here(simp)

        for global_idx, query in tasks:
            print(f"[{global_idx+1}] {query}")
            # Search on current page (results page has its own box)
            search_here(query)

            # Wait for results and locate target block
            WebDriverWait(d, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".result")))
            time.sleep(0.5)
            results = d.find_elements(By.CSS_SELECTOR, ".result")
            if not results:
                print(f"[{global_idx+1}] -> No results found")
                continue
            target_block, mp3_btn = None, None
            for block in results[:6]:
                try:
                    mp3_btn = block.find_element(By.XPATH, ".//a[normalize-space()='MP3 Download']")
                    target_block = block
                    break
                except Exception:
                    continue
            if target_block is None:
                target_block = results[0]
                mp3_btn = WebDriverWait(target_block, WAIT).until(EC.presence_of_element_located((By.XPATH, ".//a[normalize-space()='MP3 Download']")))

            d.execute_script("arguments[0].scrollIntoView({block:'center'})", target_block)
            d.execute_script("arguments[0].click();", mp3_btn)

            # Download link
            dl_btn = WebDriverWait(target_block, WAIT).until(EC.presence_of_element_located((By.XPATH, ".//a[normalize-space()='Download']")))
            end = time.time() + 20
            while time.time() < end and not (dl_btn.get_attribute("href") or "").startswith("http"):
                time.sleep(0.2)
            d.execute_script("arguments[0].setAttribute('target','_self');", dl_btn)
            try:
                d.execute_script("window._origOpen = window.open; window.open = function(){ return null; };")
            except Exception:
                pass

            # Click and wait for any new file in this worker folder
            before_names = {p.name for p in worker_dir.glob('*')}
            # capture window handles to identify only new tabs spawned by this click
            original = d.current_window_handle
            before_handles = set(d.window_handles)
            click_time = time.time()
            d.execute_script("arguments[0].click();", dl_btn)
            # wait for a new file (even hidden) to appear in worker_dir
            end_watch = time.time() + 90
            new_file: Path | None = None
            while time.time() < end_watch and new_file is None:
                for p in worker_dir.glob('*'):
                    if p.name not in before_names:
                        try:
                            if p.stat().st_mtime >= click_time:
                                new_file = p
                                break
                        except FileNotFoundError:
                            continue
                time.sleep(0.2)
            # Restore popup behavior and close stray tabs (only those newly opened)
            try:
                d.execute_script("if (window._origOpen) window.open = window._origOpen;")
            except Exception:
                pass
            close_new_tabs(d, baseline_handles=before_handles, original_handle=original, grant_before_close=True)

            # Wait for completion in worker folder
            got = wait_for_download(new_after_ts=click_time, timeout=120, download_dir=worker_dir)
            if got:
                target = DOWNLOADS / got.name
                if target.exists():
                    stem, suf = target.stem, target.suffix
                    k = 1
                    while True:
                        cand = DOWNLOADS / f"{stem} ({k}){suf}"
                        if not cand.exists():
                            target = cand
                            break
                        k += 1
                try:
                    shutil.move(str(got), str(target))
                except Exception:
                    target = got
                print(f"[{global_idx+1}] -> Downloaded: {target.name}")
            else:
                print(f"[{global_idx+1}] -> Warning: No completed file detected")
    finally:
        cleanup_driver(d)

def run():
    queries = list(read_queries())
    if not queries:
        print("No queries to process.")
        return
    print(f"Total queries: {len(queries)} | Concurrency: {CONCURRENCY}")
    # Partition by modulo so worker k handles indices k, k+CONCURRENCY, ...
    buckets: list[list[tuple[int, str]]] = [[] for _ in range(CONCURRENCY)]
    for i, q in enumerate(queries):
        buckets[i % CONCURRENCY].append((i, q))
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(process_worker, wid, tasks) for wid, tasks in enumerate(buckets) if tasks]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                print(f"Worker error: {e}")
    print(f"Done. Files saved to: {DOWNLOADS}")

if __name__ == "__main__":
    run()

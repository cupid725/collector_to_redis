"""
naver_check_excel.py (undetected-chromedriver 버전)
- Excel에서 [서비스명, 사이트]를 읽어서
- 네이버 "웹" 검색 결과(1~N페이지)에서 '사이트'가 정규화 후 정확히 매칭되는 링크가 있는지 확인
- 진행상황/탐색결과를 콘솔 로그로 출력
- 결과를 새 엑셀로 저장

필수 설치:
  pip install -U undetected-chromedriver selenium pandas openpyxl

실행 예:
  python naver_check_excel.py --input "log_2025-12-17-08-45-32.xlsx" --pages 10
  python naver_check_excel.py --input "log_2025-12-17-08-45-32.xlsx" --pages 10 --verbose
"""

from __future__ import annotations

import argparse
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qs

import pandas as pd

# ✅ UC 사용
import undetected_chromedriver as uc

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ----------------------------
# 사용자 옵션(여기만 수정해도 됨)
# ----------------------------

# 프록시 (원하면 값 넣고, 아니면 None)
# 예: PROXY = "socks://111.111.111.1:1111"
# 예: PROXY = "socks5://111.111.111.1:1111"
# 예: PROXY = "http://111.111.111.1:1111"
PROXY: Optional[str] = None

DEFAULT_WHERE = "web"     # 네이버 웹사이트 검색 탭
DEFAULT_PAGES = 10

# URL 매칭 정책
STRICT_NORMALIZED_MATCH = True   # http/https, www, trailing slash 무시 후 "정규화 정확히 일치" 판정
ALLOW_SUBPAGES = False           # True면 사이트가 https://a.com 일 때 https://a.com/xxx 도 노출 인정
ALLOW_M_SUBDOMAIN = False        # True면 m.example.com 을 example.com 과 동일 취급

# 네이버 차단/캡차 대응
DETECT_BLOCK_PAGES = False
BLOCK_COOLDOWN_SEC = 15


# ----------------------------
# 로그
# ----------------------------
def log(msg: str) -> None:
    print(msg, flush=True)


def logv(verbose: bool, msg: str) -> None:
    if verbose:
        print(msg, flush=True)


# ----------------------------
# 유틸
# ----------------------------
def _strip_www(host: str) -> str:
    host = (host or "").strip().lower()
    if host.startswith("www."):
        return host[4:]
    return host


def _strip_m(host: str) -> str:
    host = (host or "").strip().lower()
    if host.startswith("m."):
        return host[2:]
    return host


def normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", u):
        u = "https://" + u

    sp = urlsplit(u)
    host = sp.hostname or ""
    host = _strip_www(host)
    if ALLOW_M_SUBDOMAIN:
        host = _strip_m(host)

    path = sp.path or ""
    path = re.sub(r"/+$", "", path)
    port = f":{sp.port}" if sp.port else ""
    return f"{host}{port}{path}"


def make_naver_search_url(query: str, where: str = DEFAULT_WHERE, start: int = 1) -> str:
    params = {"where": where, "query": query, "start": str(start)}
    return "https://search.naver.com/search.naver?" + urlencode(params)


def make_paged_url_from_base(base_url: str, start: int, where: str = DEFAULT_WHERE, query_fallback: str = "") -> str:
    if not base_url:
        return make_naver_search_url(query_fallback, where=where, start=start)

    u = base_url.strip()
    if not u.startswith("http"):
        return make_naver_search_url(query_fallback, where=where, start=start)

    sp = urlsplit(u)
    qs = parse_qs(sp.query)

    q = (qs.get("query", [query_fallback]) or [query_fallback])[0]
    if not q:
        q = query_fallback

    qs["where"] = [where]
    qs["query"] = [q]
    qs["start"] = [str(start)]

    new_query = urlencode({k: v[0] for k, v in qs.items() if v}, doseq=False)
    return urlunsplit((sp.scheme, sp.netloc, sp.path, new_query, ""))


def looks_like_block_page(html: str) -> bool:
    if not html:
        return False
    needles = [
        "자동입력", "captcha", "비정상적인", "접속이 제한", "차단", "robot", "봇", "보안", "잠시 후 다시",
        "검색 서비스를 이용할 수 없습니다",
    ]
    h = html.lower()
    return any(n.lower() in h for n in needles)


def normalize_proxy(proxy: str) -> str:
    """
    사용자가 socks:// 로 주면 크롬이 애매해할 수 있어서 socks5:// 로 보정.
    """
    p = (proxy or "").strip()
    if not p:
        return p
    if p.startswith("socks://"):
        return "socks5://" + p[len("socks://") :]
    return p


@dataclass
class FoundInfo:
    found: bool
    found_page: Optional[int] = None
    found_rank: Optional[int] = None
    found_url: Optional[str] = None
    checked_pages: int = 0
    error: Optional[str] = None


# ----------------------------
# Excel 로드
# ----------------------------
def load_excel_first_sheet(path: str, sheet: Optional[str]) -> tuple[pd.DataFrame, str]:
    xls = pd.ExcelFile(path)
    sheet_names = xls.sheet_names
    if not sheet_names:
        raise ValueError("엑셀에 시트가 없습니다.")

    if sheet is None:
        chosen = sheet_names[0]
        df = pd.read_excel(path, sheet_name=chosen)
        return df, chosen

    if isinstance(sheet, str) and sheet.isdigit():
        idx = int(sheet)
        if idx < 0 or idx >= len(sheet_names):
            raise ValueError(f"--sheet 인덱스 범위 오류: {idx} (0~{len(sheet_names)-1})")
        chosen = sheet_names[idx]
        df = pd.read_excel(path, sheet_name=chosen)
        return df, chosen

    chosen = sheet
    df = pd.read_excel(path, sheet_name=chosen)
    return df, chosen


# ----------------------------
# UC Driver
# ----------------------------
def build_driver(headless: bool = False, user_data_dir: Optional[str] = None, proxy: Optional[str] = None) -> uc.Chrome:
    options = uc.ChromeOptions()

    # 헤드리스(원하면)
    if headless:
        options.add_argument("--headless=new")

    # 기본 안정 옵션
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=ko-KR,ko")
    options.add_argument("--window-size=1200,900")

    # 프록시(옵션)
    if proxy:
        p = normalize_proxy(proxy)
        options.add_argument(f"--proxy-server={p}")

    # 프로필(선택)
    if user_data_dir:
        options.add_argument(f"--user-data-dir={user_data_dir}")

    # ✅ UC 드라이버 생성
    driver = uc.Chrome(options=options)

    return driver


def extract_external_links(driver: uc.Chrome) -> list[str]:
    hrefs: list[str] = []
    selectors = ["#main_pack a[href]", "#content a[href]", "a[href]"]

    for sel in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            for e in els:
                try:
                    href = e.get_attribute("href")
                    if not href:
                        continue
                    if href.startswith("javascript:"):
                        continue
                    if "search.naver.com" in href:
                        continue
                    if href.startswith("https://m.search.naver.com"):
                        continue
                    hrefs.append(href)
                except Exception:
                    continue
            if hrefs:
                break
        except Exception:
            continue

    # dedupe
    seen = set()
    uniq = []
    for h in hrefs:
        if h in seen:
            continue
        seen.add(h)
        uniq.append(h)
    return uniq


def check_one_query(
    driver: uc.Chrome,
    service_name: str,
    site_url: str,
    pages: int,
    base_search_url: str,
    where: str,
    wait_sec: int,
    verbose: bool,
) -> FoundInfo:
    target_norm = normalize_url(site_url) if STRICT_NORMALIZED_MATCH else (site_url or "").strip()
    if not target_norm:
        return FoundInfo(found=False, checked_pages=0, error="사이트 URL이 비어있음")

    logv(verbose, f"    [TARGET] raw='{site_url}' norm='{target_norm}'")

    for page in range(1, pages + 1):
        start = 1 + (page - 1) * 10
        url = make_paged_url_from_base(base_search_url, start=start, where=where, query_fallback=service_name)

        try:
            log(f"    [PAGE {page}/{pages}] GET start={start}  url={url}")
            driver.get(url)

            WebDriverWait(driver, wait_sec).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#wrap, body"))
            )

            html = driver.page_source or ""
            if DETECT_BLOCK_PAGES and looks_like_block_page(html):
                log(f"    [WARN] 차단/캡차 의심 페이지 감지 → {BLOCK_COOLDOWN_SEC}s 대기")
                time.sleep(BLOCK_COOLDOWN_SEC)
                html2 = driver.page_source or ""
                if looks_like_block_page(html2):
                    return FoundInfo(found=False, checked_pages=page, error="네이버 차단/캡차로 보이는 페이지 감지")

            links = extract_external_links(driver)
            log(f"    [PAGE {page}] 외부링크 수집: {len(links)}개")

            rank = 0
            for href in links:
                rank += 1

                if STRICT_NORMALIZED_MATCH:
                    href_norm = normalize_url(href)
                    if not href_norm:
                        continue

                    if verbose and rank <= 20:
                        logv(verbose, f"        - rank#{rank} href_norm='{href_norm}'")

                    if href_norm == target_norm:
                        log(f"    [FOUND] page={page} rank={rank} href={href}")
                        return FoundInfo(True, page, rank, href, checked_pages=page)

                    if ALLOW_SUBPAGES and href_norm.startswith(target_norm + "/"):
                        log(f"    [FOUND] (subpage) page={page} rank={rank} href={href}")
                        return FoundInfo(True, page, rank, href, checked_pages=page)

                else:
                    if (href or "").strip() == (site_url or "").strip():
                        log(f"    [FOUND] page={page} rank={rank} href={href}")
                        return FoundInfo(True, page, rank, href, checked_pages=page)

            log(f"    [MISS] page={page} 에서 미발견 (다음 페이지)")
            time.sleep(random.uniform(0.8, 2.6))

        except Exception as e:
            log(f"    [ERROR] page={page} {type(e).__name__}: {e}")
            return FoundInfo(found=False, checked_pages=page, error=f"{type(e).__name__}: {e}")

    return FoundInfo(found=False, checked_pages=pages)


# ----------------------------
# 메인
# ----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="log_2025-12-17-08-45-32.xlsx", help="입력 엑셀 경로")
    ap.add_argument("--sheet", default=None, help="시트명 또는 시트 인덱스(0부터). 미지정 시 첫 시트")
    ap.add_argument("--pages", type=int, default=DEFAULT_PAGES, help="확인할 페이지 수 (기본 10)")
    ap.add_argument("--where", default=DEFAULT_WHERE, help="네이버 where 파라미터 (web 권장)")
    ap.add_argument("--headless", action="store_true", help="헤드리스 모드(권장X, 차단될 수 있음)")
    ap.add_argument("--profile", default="", help="크롬 user-data-dir 경로(선택)")
    ap.add_argument("--out", default="", help="출력 엑셀 경로(기본: 자동 생성)")
    ap.add_argument("--verbose", action="store_true", help="상세 비교 로그(너무 시끄러우면 끄기)")
    ap.add_argument("--wait", type=int, default=8, help="페이지 로딩 대기(초) (기본 8)")
    ap.add_argument("--proxy", default="", help="(옵션) 프록시. 주면 PROXY 변수보다 우선 (예: socks://ip:port)")
    args = ap.parse_args()

    in_path = args.input
    if not os.path.exists(in_path):
        log(f"[ERROR] 파일 없음: {in_path}")
        return 2

    try:
        df, used_sheet = load_excel_first_sheet(in_path, args.sheet)
        log(f"[INFO] 사용 시트: {used_sheet} / rows={len(df)}")
    except Exception as e:
        try:
            xls = pd.ExcelFile(in_path)
            log("[INFO] 시트 목록: " + ", ".join(xls.sheet_names))
        except Exception:
            pass
        log(f"[ERROR] 엑셀 로드 실패: {type(e).__name__}: {e}")
        return 2

    col_service = "서비스명"
    col_site = "사이트"
    col_search = "네이버 검색경로" if "네이버 검색경로" in df.columns else None

    if col_service not in df.columns or col_site not in df.columns:
        log("[ERROR] 엑셀에 '서비스명' 또는 '사이트' 컬럼이 없습니다. 컬럼명을 확인하세요.")
        log("현재 컬럼: " + ", ".join(map(str, df.columns)))
        return 2

    # 결과 컬럼
    df["네이버노출"] = ""
    df["노출페이지"] = ""
    df["노출순위(대략)"] = ""
    df["노출링크"] = ""
    df["체크페이지수"] = ""
    df["체크시각"] = ""
    df["에러"] = ""

    # 프록시 결정: CLI 우선 > 상단 PROXY
    effective_proxy = args.proxy.strip() or PROXY
    log(f"[INFO] UC Chrome start headless={args.headless} profile={(args.profile.strip() or '(none)')}")
    log(f"[INFO] PROXY={(normalize_proxy(effective_proxy) if effective_proxy else '(none)')}")

    driver = build_driver(
        headless=args.headless,
        user_data_dir=(args.profile.strip() or None),
        proxy=effective_proxy,
    )

    try:
        total = len(df)
        for idx, row in df.iterrows():
            service = str(row.get(col_service, "")).strip()
            site = str(row.get(col_site, "")).strip()
            base_search_url = str(row.get(col_search, "")).strip() if col_search else ""

            log("=" * 80)
            log(f"[ROW {idx+1}/{total}] 서비스명='{service}'  사이트='{site}'")

            if not service or not site or site.lower() == "nan":
                log("    [SKIP] 서비스명/사이트 누락")
                df.at[idx, "네이버노출"] = "SKIP"
                df.at[idx, "에러"] = "서비스명/사이트 누락"
                continue

            info = check_one_query(
                driver=driver,
                service_name=service,
                site_url=site,
                pages=args.pages,
                base_search_url=base_search_url,
                where=args.where,
                wait_sec=args.wait,
                verbose=args.verbose,
            )

            df.at[idx, "네이버노출"] = "Y" if info.found else "N"
            df.at[idx, "노출페이지"] = info.found_page if info.found_page is not None else ""
            df.at[idx, "노출순위(대략)"] = info.found_rank if info.found_rank is not None else ""
            df.at[idx, "노출링크"] = info.found_url if info.found_url else ""
            df.at[idx, "체크페이지수"] = info.checked_pages
            df.at[idx, "체크시각"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df.at[idx, "에러"] = info.error or ""

            log(f"[ROW {idx+1}] RESULT = {'FOUND' if info.found else 'NOT FOUND'} / checked_pages={info.checked_pages} / err={info.error or '-'}")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = args.out.strip() or f"naver_check_results_{ts}.xlsx"
        df.to_excel(out_path, index=False)
        log(f"\n[OK] 저장 완료: {out_path}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

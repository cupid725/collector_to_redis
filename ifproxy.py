import requests
import time
from typing import Optional, Tuple
from functools import lru_cache
from collections import Counter

# ===================== 설정 영역 =====================

# 테스트할 프록시 (주석 해제하여 선택)
PROXY = "socks5://130.193.57.247:1080"
#PROXY = "http://36.110.143.55:8080"    #Singapore (SG)       | Type: Datacenter

#PROXY = "socks5://37.18.73.60:5566"     #datacenter|regidential
#PROXY = "socks5://84.54.227.27:1080"
#PROXY = "socks5://185.54.178.193:1080"
#PROXY = "socks5://192.168.140.219:37919"
#PROXY = "http://88.99.26.62:3128"
#PROXY = "socks5://203.25.208.163:1100"
#PROXY = "socks5://36.110.143.55:8080"

# 같은 프록시로 몇 번 테스트할지
NUM_RUNS = 5

# IP 체크 서비스들 (HTTP와 HTTPS 혼합, 우선순위순)
IP_CHECK_URLS = [
    # HTTP 우선 (HTTP 프록시에서도 잘 작동)
    ("http://api.ipify.org?format=text", "http"),
    ("http://icanhazip.com", "http"),
    ("http://ifconfig.me/ip", "http"),
    ("http://checkip.amazonaws.com", "http"),
    ("http://ipinfo.io/ip", "http"),
    # HTTPS 백업 (SOCKS나 터널링 지원 프록시용)
    ("https://api.ipify.org?format=text", "https"),
    ("https://icanhazip.com", "https"),
    ("https://ifconfig.me/ip", "https"),
]

# 타임아웃 설정 (초) - 느린 프록시를 위해 증가
CONNECT_TIMEOUT = 15  # 연결 타임아웃
READ_TIMEOUT = 15      # 읽기 타임아웃

# GeoIP 조회용 URL (추가 필드: isp, org, as, mobile, proxy, hosting)
GEOIP_URL = "http://ip-api.com/json/{ip}?fields=status,country,countryCode,query,message,isp,org,as,mobile,proxy,hosting"

# ===================== 함수들 =====================

def check_ip_once(proxy: Optional[str] = None) -> Tuple[str, str]:
    """
    프록시를 통해 IP를 체크하고 반환
    Returns: (ip, service_url) 튜플
    """
    
    # 프록시 설정
    proxies = None
    if proxy:
        proxies = {
            "http": proxy,
            "https": proxy
        }
    
    # 여러 서비스를 순차적으로 시도
    errors = []
    
    for url, protocol in IP_CHECK_URLS:
        try:
            response = requests.get(
                url,
                proxies=proxies,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
            response.raise_for_status()
            ip = response.text.strip()
            
            # IP 형식 검증 (IPv4 또는 IPv6)
            if ip and ('.' in ip or ':' in ip):
                # 기본적인 IP 형식 체크
                if len(ip) < 50:  # 비정상적으로 긴 응답 필터링
                    return ip, url
            
        except requests.exceptions.ProxyError as e:
            errors.append(f"[{protocol.upper()}] {url}: ProxyError - {str(e)[:100]}")
        except requests.exceptions.Timeout as e:
            errors.append(f"[{protocol.upper()}] {url}: Timeout")
        except requests.exceptions.ConnectionError as e:
            errors.append(f"[{protocol.upper()}] {url}: ConnectionError - {str(e)[:100]}")
        except Exception as e:
            errors.append(f"[{protocol.upper()}] {url}: {type(e).__name__} - {str(e)[:100]}")
        
        # HTTP가 실패하면 다음 시도 전 짧은 대기
        time.sleep(0.5)
    
    # 모든 서비스가 실패한 경우
    error_msg = "\n".join(errors[:5])  # 상위 5개 에러만 표시
    raise Exception(f"All IP check services failed:\n{error_msg}")


@lru_cache(maxsize=None)
def get_ip_info(ip: str) -> dict:
    """
    IP에 대한 상세 정보를 반환
    Returns: dict with 'country', 'type', 'isp', 'org', 'as'
    'type'은 'Residential', 'Datacenter', 'Mobile', 'Unknown'
    """
    try:
        resp = requests.get(
            GEOIP_URL.format(ip=ip),
            timeout=(5, 5),
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "success":
            country = data.get("country")
            code = data.get("countryCode")
            country_str = f"{country} ({code})" if country and code else country or "Unknown"
            
            isp = data.get("isp", "Unknown")
            org = data.get("org", "Unknown")
            as_ = data.get("as", "Unknown")
            mobile = data.get("mobile", False)
            proxy = data.get("proxy", False)
            hosting = data.get("hosting", False)
            
            # Proxy 타입 분류
            if hosting:
                ip_type = "Datacenter"
            elif mobile:
                ip_type = "Mobile"
            elif not hosting and not mobile:
                ip_type = "Residential"
            else:
                ip_type = "Unknown"
            
            return {
                "country": country_str,
                "type": ip_type,
                "isp": isp,
                "org": org,
                "as": as_,
                "proxy_detected": proxy
            }
    except Exception:
        pass

    return {
        "country": "Unknown",
        "type": "Unknown",
        "isp": "Unknown",
        "org": "Unknown",
        "as": "Unknown",
        "proxy_detected": False
    }


def diagnose_proxy(proxy: str) -> None:
    """프록시 타입과 연결 가능성 진단"""
    print("\n🔧 프록시 진단 중...")
    
    proxy_type = "Unknown"
    if proxy.startswith("http://"):
        proxy_type = "HTTP Proxy"
    elif proxy.startswith("https://"):
        proxy_type = "HTTPS Proxy"
    elif proxy.startswith("socks5://"):
        proxy_type = "SOCKS5 Proxy"
    elif proxy.startswith("socks4://"):
        proxy_type = "SOCKS4 Proxy"
    
    print(f"   프록시 타입: {proxy_type}")
    
    # HTTP 프록시인 경우 HTTPS 터널링 경고
    if proxy.startswith("http://"):
        print("   ⚠️  HTTP 프록시는 HTTPS 사이트 접속 시 CONNECT 터널링이 필요합니다")
        print("       일부 프록시는 이를 차단할 수 있습니다 (405 Not Allowed)")


def analyze_results(results: list[Tuple[str, str]]) -> None:
    """수집된 IP들을 분석해서 RR 방식인지 판단 + 국가 정보 + Residential 여부 출력"""
    print("\n" + "="*60)
    print("📊 분석 결과")
    print("="*60)
    
    ips = [ip for ip, _ in results]
    unique_ips = list(sorted(set(ips)))
    
    print(f"✅ 총 {len(ips)}번 시도 중 {len(unique_ips)}개의 고유 IP 발견")
    
    # 사용된 서비스 통계
    services = [svc for _, svc in results]
    service_counts = Counter(services)
    print(f"\n🌐 사용된 IP 체크 서비스:")
    for svc, count in service_counts.most_common():
        protocol = "🔒 HTTPS" if svc.startswith("https") else "🔓 HTTP"
        print(f"   {protocol} {svc}: {count}회")
    
    print("\n📋 발견된 IP 목록 (국가, 타입, ISP 포함):")
    for ip in unique_ips:
        info = get_ip_info(ip)
        print(f"   • {ip} - {info['country']} | Type: {info['type']} | ISP: {info['isp']} | Org: {info['org']} | AS: {info['as']} | Proxy Detected: {info['proxy_detected']}")
    
    # RR 판단
    if len(unique_ips) == 1:
        print("\n🔴 결론: 고정 프록시 (Static Proxy)")
        print("   → 항상 같은 IP를 사용합니다")
    elif len(unique_ips) == len(ips):
        print("\n🟢 결론: 완전 회전형 프록시 (Full Rotating Proxy)")
        print("   → 매번 다른 IP를 사용합니다")
    else:
        print(f"\n🟡 결론: 부분 회전형 프록시 (Partial Rotating Proxy)")
        print(f"   → IP 풀에서 {len(unique_ips)}개를 순환 사용합니다")
        
        # 각 IP의 출현 빈도 표시
        print("\n📈 IP별 출현 횟수:")
        ip_counts = Counter(ips)
        for ip, count in ip_counts.most_common():
            info = get_ip_info(ip)
            percentage = (count / len(ips)) * 100
            print(f"   • {ip} - {info['country']} | Type: {info['type']}: {count}회 ({percentage:.1f}%)")


if __name__ == "__main__":
    print("="*60)
    print("🔍 프록시 RR (Round-Robin) 테스트")
    print("="*60)
    print(f"📌 프록시: {PROXY}")
    print(f"🔄 시도 횟수: {NUM_RUNS}번")
    print(f"⏱️  타임아웃: 연결 {CONNECT_TIMEOUT}초 / 읽기 {READ_TIMEOUT}초")
    
    # 프록시 진단
    diagnose_proxy(PROXY)
    
    print("\n" + "="*60)
    print("테스트 시작...\n")
    
    results = []
    success_count = 0
    
    for i in range(NUM_RUNS):
        try:
            ip, service = check_ip_once(PROXY)
            info = get_ip_info(ip)
            service_short = service.split('//')[1].split('/')[0]  # 도메인만 추출
            print(f"✓ Run {i + 1:2d}: {ip:15s} - {info['country']:20s} | Type: {info['type']:12s} (via {service_short})")
            results.append((ip, service))
            success_count += 1
        except Exception as e:
            print(f"✗ Run {i + 1:2d}: 실패")
            # 첫 번째 실패 시에만 상세 에러 출력
            if success_count == 0 and i == 0:
                print(f"\n⚠️  첫 시도 실패 - 상세 에러:")
                print(f"{str(e)}\n")
        
        # 마지막 시도가 아니면 잠시 대기
        if i < NUM_RUNS - 1:
            time.sleep(1.5)
    
    # 결과 분석
    if results:
        analyze_results(results)
        print(f"\n✅ 성공률: {success_count}/{NUM_RUNS} ({success_count/NUM_RUNS*100:.1f}%)")
    else:
        print("\n" + "="*60)
        print("❌ 모든 시도 실패")
        print("="*60)
        print("\n💡 문제 해결 방법:")
        print("   1. 프록시 주소와 포트가 정확한지 확인")
        print("   2. 프록시가 실제로 작동 중인지 확인")
        print("   3. 방화벽이 프록시 연결을 차단하지 않는지 확인")
        print("   4. HTTP 프록시인 경우 HTTPS 터널링을 지원하는지 확인")
        print("   5. SOCKS 프록시인 경우 requests[socks] 설치 확인:")
        print("      pip install requests[socks]")
    
    print("\n" + "="*60)
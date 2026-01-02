import random
import time
import config
from DrissionPage import ChromiumPage

class MobileHumanEvent:
    """
    모바일 웹(m.youtube.com) 환경에서 인간과 유사한 동작을 수행하는 클래스.
    DrissionPage를 사용하여 요소 탐색 및 인터랙션을 수행합니다.
    """

    def __init__(self, page: ChromiumPage):
        self.page = page
        self.keywords = [
            "funny videos", "gaming", "cooking", "sports", "redpanda",
            "travel", "music", "mrbeast", "lofi", "nba", "news", "deepseek"
        ]

        # 대기 관련 기본값 (필요하면 여기만 조절)
        self.NAV_TIMEOUT = 15          # 홈/검색 결과 DOM 대기
        self.VIDEO_READY_TIMEOUT = 25  # 영상 준비(video src/readyState) 대기
        self.URL_CHANGE_TIMEOUT = 8    # 클릭 후 URL 변화 대기

    def execute_random_action(self):
        """
        1~3번 동작 중 하나를 랜덤하게 선택하여 실행합니다.
        외부에서는 이 함수만 호출하면 됩니다.
        """
        actions = [
            self.move_to_next_shorts,     # 1번: 다음 영상(Shorts) 이동
            self.click_home_and_random,   # 2번: 홈 이동 후 추천 영상 클릭
            self.search_and_click_video   # 3번: 검색 후 영상 클릭
        ]

        selected_action = random.choice(actions)
        print(f"[HumanEvent] 🎲 선택된 동작: {selected_action.__name__}")

        try:
            selected_action()
            return True
        except Exception as e:
            print(f"[HumanEvent] ❌ 동작 실행 중 오류: {e}")
            return False

    # -------------------------
    # 공통 유틸 (셀렉터 추가 없이, 대기만 강화)
    # -------------------------
    def _sleep_human(self, a=0.4, b=1.2):
        time.sleep(random.uniform(a, b))

    def _safe_url(self) -> str:
        try:
            return self.page.url or ""
        except:
            return ""

    def _wait_url_change(self, before_url: str, timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            cur = self._safe_url()
            if cur and cur != before_url:
                return True
            time.sleep(0.2)
        return False

    def _wait_any_of_elements(self, selector: str, timeout: float) -> bool:
        """
        selector가 등장할 때까지 기다림(존재만 확인)
        """
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                if self.page.ele(selector, timeout=0.2):
                    return True
            except:
                pass
            time.sleep(0.2)
        return False

    def _video_state(self):
        """
        video가 실제로 재생 가능한 상태인지 JS로 판정.
        - 성공 조건: currentSrc 존재 + readyState >= 1
        """
        try:
            return self.page.run_js("""
                const v = document.querySelector('video');
                if (!v) return null;
                return { src: (v.currentSrc || v.src || ''), rs: v.readyState, ns: v.networkState, paused: v.paused };
            """)
        except:
            return None

    def _wait_video_ready(self, timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            st = self._video_state()
            if st and st.get("src") and (st.get("rs", 0) >= 1):
                return True
            time.sleep(0.25)
        return False

    def _click_and_wait_video_ready(self, click_fn, debug_tag: str) -> bool:
        """
        클릭 -> (URL변화 조금 기다림) -> video ready까지 대기.
        느린/흰화면 꼬임이면 refresh 1회로 복구 시도.
        """
        before = self._safe_url()

        try:
            click_fn()
        except:
            pass

        # SPA는 URL이 안 바뀌는 경우도 있어 "보조"로만 짧게 기다림
        self._wait_url_change(before, timeout=self.URL_CHANGE_TIMEOUT)

        ok = self._wait_video_ready(timeout=self.VIDEO_READY_TIMEOUT)
        if ok:
            return True

        print(f"[HumanEvent] ⚠️ {debug_tag}: video 준비 지연 -> refresh 1회 시도")
        try:
            self.page.refresh()
        except:
            pass

        ok2 = self._wait_video_ready(timeout=20)
        if not ok2:
            print(f"[HumanEvent] ❌ {debug_tag}: video 준비 실패(최종)")
        return ok2

    # --- [1번 동작] Shorts 다음 영상 이동 ---
    def move_to_next_shorts(self):
        """
        모바일 쇼츠 인터페이스에서 1~20번째 중 랜덤하게 아래로 이동합니다.
        """
        n = random.randint(1, 20)
        print(f"[HumanEvent] ⬇️ Shorts 다음 영상으로 {n}회 이동 시도")

        for i in range(n):
            self.page.actions.key_down('DOWN').key_up('DOWN')
            time.sleep(random.uniform(0.5, 1.2))

            # 너무 공격적으로 기다리면 느려지니, 짧게만 체크 (없으면 그냥 진행)
            _ = self._wait_video_ready(timeout=4)

            if (i + 1) % 5 == 0:
                print(f"[HumanEvent]    ... {i + 1}번째 이동 중")

        print(f"[HumanEvent] ✅ {n}회 이동 완료")

    # --- [2번 동작] 홈 이동 후 추천 영상 클릭 ---
    def click_home_and_random(self):
        """
        유튜브 홈 버튼 클릭 -> 홈 이동 확인 -> 1~10번째 추천 영상 중 클릭
        """
        print("[HumanEvent] 🏠 홈 버튼 클릭 및 추천 영상 탐색")

        before = self._safe_url()

        # (원본 셀렉터 유지) 모바일 홈 버튼(로고) 클릭
        home_btn = self.page.ele('@@id=logo@@tag=a', timeout=3)
        if home_btn:
            try:
                home_btn.click()
            except:
                pass
            try:
                self.page.wait.load_start()
            except:
                pass
        else:
            self.page.get('https://m.youtube.com', timeout=config.PAGE_LOAD_TIMEOUT)

        # ✅ 홈 전환 확인: URL변화 + 홈 리스트 DOM(원본 셀렉터) 뜰 때까지 기다림
        self._wait_url_change(before, timeout=self.URL_CHANGE_TIMEOUT)
        if not self._wait_any_of_elements('tag:ytm-rich-item-renderer', timeout=self.NAV_TIMEOUT):
            print("[HumanEvent] ⚠️ 홈 리스트 로드 지연 -> refresh 1회 시도")
            try:
                self.page.refresh()
            except:
                pass
            if not self._wait_any_of_elements('tag:ytm-rich-item-renderer', timeout=10):
                print("[HumanEvent] ❌ 홈에서 영상을 찾지 못했습니다(홈 DOM 미등장).")
                return

        self._sleep_human(0.8, 1.8)

        # (원본 셀렉터 유지)
        videos = self.page.eles('tag:ytm-rich-item-renderer', timeout=5)
        if videos:
            max_idx = min(10, len(videos))
            target_idx = random.randint(0, max_idx - 1)
            print(f"[HumanEvent] 🎯 {target_idx + 1}번째 추천 영상 클릭")

            def _do_click():
                videos[target_idx].click()

            self._click_and_wait_video_ready(_do_click, debug_tag="home_click")
        else:
            print("[HumanEvent] ⚠️ 홈에서 영상을 찾지 못했습니다.")

    # --- [3번 동작] 검색 후 영상 클릭 ---
    def search_and_click_video(self):
        """
        홈으로 이동 -> 검색창 찾기 -> 키워드 입력 -> 결과 중 1~20번째 중 클릭
        """
        keyword = random.choice(self.keywords)
        print(f"[HumanEvent] 🔍 검색어 입력: '{keyword}'")

        # 1. 검색 버튼(원본 셀렉터) 클릭하여 입력창 활성화
        search_open_btn = self.page.ele('@@class^=search-box@@tag=button', timeout=3)
        if search_open_btn:
            try:
                search_open_btn.click()
            except:
                pass
            time.sleep(0.6)

        # 2. 검색창 입력 (원본 셀렉터)
        search_input = self.page.ele('tag:input@@name=search_query', timeout=3)
        if not search_input:
            print("[HumanEvent] ⚠️ 검색창을 찾지 못했습니다.")
            return

        try:
            search_input.input(keyword)
        except:
            pass

        # 엔터로 검색 실행 (원본 동작 유지)
        try:
            self.page.actions.key_down('ENTER').key_up('ENTER')
        except:
            pass

        try:
            self.page.wait.load_start()
        except:
            pass

        # ✅ 검색 결과 DOM(원본 셀렉터) 뜰 때까지 기다림
        if not self._wait_any_of_elements('tag:ytm-video-with-context-renderer', timeout=self.NAV_TIMEOUT):
            print("[HumanEvent] ⚠️ 검색 결과 로드 지연 -> refresh 1회 시도")
            try:
                self.page.refresh()
            except:
                pass
            if not self._wait_any_of_elements('tag:ytm-video-with-context-renderer', timeout=10):
                print("[HumanEvent] ❌ 검색 결과를 찾지 못했습니다(결과 DOM 미등장).")
                return

        self._sleep_human(0.8, 1.6)

        # 3. 검색 결과 중 1~20번째 중 하나 클릭 (원본 셀렉터)
        results = self.page.eles('tag:ytm-video-with-context-renderer', timeout=5)
        if results:
            max_idx = min(20, len(results))
            target_idx = random.randint(0, max_idx - 1)
            print(f"[HumanEvent] 🎯 검색 결과 {target_idx + 1}번째 영상 클릭")

            def _do_click():
                results[target_idx].click()

            self._click_and_wait_video_ready(_do_click, debug_tag="search_result_click")
        else:
            print("[HumanEvent] ⚠️ 검색 결과를 찾지 못했습니다.")
            
class MobileHumanEvent_old:
    """
    모바일 웹(m.youtube.com) 환경에서 인간과 유사한 동작을 수행하는 클래스.
    DrissionPage를 사용하여 요소 탐색 및 인터랙션을 수행합니다.
    """
    
    def __init__(self, page: ChromiumPage):
        self.page = page
        # live_human_events.py의 키워드 리스트 참조 (일부 발췌)
        self.keywords = [
            "funny videos", "gaming", "cooking", "sports", "redpanda", 
            "travel", "music", "mrbeast", "lofi", "nba", "news", "deepseek"
        ]

    def execute_random_action(self):
        """
        1~3번 동작 중 하나를 랜덤하게 선택하여 실행합니다.
        외부에서는 이 함수만 호출하면 됩니다.
        """
        actions = [
            self.move_to_next_shorts,     # 1번: 다음 영상(Shorts) 이동
            self.click_home_and_random,   # 2번: 홈 이동 후 추천 영상 클릭
            self.search_and_click_video   # 3번: 검색 후 영상 클릭
        ]
        
        selected_action = random.choice(actions)
        print(f"[HumanEvent] 🎲 선택된 동작: {selected_action.__name__}")
        
        try:
            selected_action()
            return True
        except Exception as e:
            print(f"[HumanEvent] ❌ 동작 실행 중 오류: {e}")
            return False

    # --- [1번 동작] Shorts 다음 영상 이동 ---
    def move_to_next_shorts(self):
        """
        모바일 쇼츠 인터페이스에서 1~20번째 중 랜덤하게 아래로 이동합니다.
        """
        n = random.randint(1, 20)
        print(f"[HumanEvent] ⬇️ Shorts 다음 영상으로 {n}회 이동 시도")
        
        for i in range(n):
            # 모바일 웹 쇼츠는 'swipe' 동작 혹은 방향키 DOWN으로 제어
            # DrissionPage의 단축키 입력 사용
            self.page.actions.key_down('DOWN').key_up('DOWN')
            time.sleep(random.uniform(0.5, 1.2))
            if (i + 1) % 5 == 0:
                print(f"[HumanEvent]    ... {i + 1}번째 이동 중")
        
        print(f"[HumanEvent] ✅ {n}회 이동 완료")

    # --- [2번 동작] 홈 이동 후 추천 영상 클릭 ---
    def click_home_and_random(self):
        """
        유튜브 홈 버튼 클릭 -> 홈 이동 확인 -> 1~10번째 추천 영상 중 클릭
        """
        print("[HumanEvent] 🏠 홈 버튼 클릭 및 추천 영상 탐색")
        
        # 모바일 홈 버튼(로고) 클릭
        home_btn = self.page.ele('@@id=logo@@tag=a', timeout=3)
        if home_btn:
            home_btn.click()
            self.page.wait.load_start()
        else:
            self.page.get('https://m.youtube.com') # 버튼 못 찾으면 직접 이동
            
        time.sleep(random.uniform(2, 4))
        
        # 모바일 홈의 비디오 아이템들 추출 (리스트 형태)
        # m.youtube.com의 비디오 아이템 셀렉터 최적화
        videos = self.page.eles('tag:ytm-rich-item-renderer', timeout=5)
        
        if videos:
            max_idx = min(10, len(videos))
            target_idx = random.randint(0, max_idx - 1)
            print(f"[HumanEvent] 🎯 {target_idx + 1}번째 추천 영상 클릭")
            videos[target_idx].click()
        else:
            print("[HumanEvent] ⚠️ 홈에서 영상을 찾지 못했습니다.")

    # --- [3번 동작] 검색 후 영상 클릭 ---
    def search_and_click_video(self):
        """
        홈으로 이동 -> 검색창 찾기 -> 키워드 입력 -> 결과 중 1~20번째 중 클릭
        """
        keyword = random.choice(self.keywords)
        print(f"[HumanEvent] 🔍 검색어 입력: '{keyword}'")

        # 1. 검색 버튼(돋보기) 클릭하여 입력창 활성화
        search_open_btn = self.page.ele('@@class^=search-box@@tag=button', timeout=3)
        if search_open_btn:
            search_open_btn.click()
            time.sleep(1)

        # 2. 검색창 입력
        search_input = self.page.ele('tag:input@@name=search_query', timeout=3)
        if search_input:
            search_input.input(keyword)
            self.page.actions.key_down('ENTER').key_up('ENTER')
            self.page.wait.load_start()
            time.sleep(random.uniform(3, 5))
            
            # 3. 검색 결과 중 1~20번째 중 하나 클릭
            results = self.page.eles('tag:ytm-video-with-context-renderer', timeout=5)
            if results:
                max_idx = min(20, len(results))
                target_idx = random.randint(0, max_idx - 1)
                print(f"[HumanEvent] 🎯 검색 결과 {target_idx + 1}번째 영상 클릭")
                results[target_idx].click()
            else:
                print("[HumanEvent] ⚠️ 검색 결과를 찾지 못했습니다.")
        else:
            print("[HumanEvent] ⚠️ 검색창을 찾지 못했습니다.")
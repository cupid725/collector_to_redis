import random
import time
from DrissionPage import ChromiumPage

class MobileHumanEvent:
    """
    DrissionPage를 사용한 모바일 YouTube 인간 행동 시뮬레이션
    
    주요 동작:
    1. mouse_scroll: Shorts에서 다음 영상으로 이동 (1~20회)
    2. click_youtube_home: 홈 이동 후 추천 영상 클릭
    3. search_and_click_video: 검색 후 결과에서 영상 클릭
    """

    ACTIONS = (
        "mouse_scroll",
        "click_youtube_home",
        "search_and_click_video",
    )

    ACTION_WEIGHTS = (0.3, 0.3, 0.4)

    def __init__(self, page: ChromiumPage):
        self.page = page
        self.keywords = [
            "mr redpanda", "funny videos", "gaming", "cooking", "sports",
            "snow", "christmas", "travel", "redpanda", "entertainment",
            "comedy", "movies", "snowman", "reviews", "puppy",
            "asmr", "happy", "trailers", "podcasts", "cute",
            "trump", "mrbeast", "music", "lofi",
            "sidemen", "apt", "asmongold", "kendrick lamar", "nba",
            "bad bunny", "wwe", "die with a smile", "ishowspeed", "bruno mars",
            "ufc", "song", "karaoke", "not like us", "minecraft",
            "real madrid", "mr beast", "coryxkenshin", "joe rogan", "marvel rivals",
            "songs", "markiplier", "snl", "phonk", "samay raina",
            "study with me", "f1", "penguinz0", "podcast", "eminem",
            "kendrick lamar super bowl", "drake", "linkin park", "speed", "jennie",
            "gta 6", "kingdom come deliverance 2", "musica", "tmkoc", "cocomelon",
            "fox news", "lady gaga", "playboi carti", "solo leveling", "sigma boy",
            "caseoh", "white noise", "ign", "news", "deepseek",
            "billie eilish", "cnn", "monster hunter wilds", "the weeknd", "youtube",
            "lck", "lakers", "liverpool", "study music", "poppy playtime chapter 4",
            "destiny", "fortnite", "review phim", "trailer", "dhruv rathee",
            "arsenal", "xqc", "valorant", "ludwig", "doechii",
        ]

    def execute_random_action(self) -> bool:
        """
        랜덤으로 동작을 선택하고 실행
        Returns: 실행 성공 여부
        """
        actions = [getattr(self, name) for name in self.ACTIONS]
        weights = list(self.ACTION_WEIGHTS)

        if sum(weights) > 0:
            selected_action = random.choices(actions, weights=weights, k=1)[0]
        else:
            selected_action = random.choice(actions)

        print(f"[MobileHumanEvent] 🎲 선택된 동작: {selected_action.__name__}")

        try:
            selected_action()
            return True
        except Exception as e:
            print(f"[MobileHumanEvent] ❌ 동작 실행 중 오류: {e}")
            return False

    def _sleep_human(self, a=0.4, b=1.2):
        """인간처럼 랜덤 대기"""
        time.sleep(random.uniform(a, b))

    def _safe_url(self) -> str:
        """안전하게 현재 URL 가져오기"""
        try:
            return self.page.url or ""
        except:
            return ""

    # ========================================
    # 1️⃣ mouse_scroll - Shorts 다음 영상 이동
    # ========================================
    def mouse_scroll(self):
        """
        키보드 다운버튼을 통해 1~20번 중 랜덤으로 페이지 이동
        모바일 Shorts에서 주로 사용
        """
        print("[MobileHumanEvent] ⬇️ 모바일 스크롤 실행")

        try:
            current_url = self._safe_url().lower()
            scroll_count = random.randint(1, 20)
            print(f"   [MobileHumanEvent] {scroll_count}번 스크롤 예정")

            if "shorts" in current_url:
                print("   [MobileHumanEvent] YouTube Shorts 감지")

            for i in range(scroll_count):
                # DrissionPage 키보드 입력
                self.page.actions.key_down('DOWN').key_up('DOWN')
                time.sleep(random.uniform(0.5, 2.0))

                if (i + 1) % 5 == 0:
                    print(f"   [MobileHumanEvent] {i+1}/{scroll_count} 이동 완료")

            print(f"   [MobileHumanEvent] ✅ 스크롤 완료 ({scroll_count}번)")

        except Exception as e:
            print(f"[MobileHumanEvent] ❌ 스크롤 실패: {e}")

    # ========================================
    # 2️⃣ click_youtube_home - 홈 이동 후 영상 클릭
    # ========================================
    def click_youtube_home(self):
        """
        유튜브 홈 버튼 클릭 → 홈 이동 → 1~20번 스크롤 → 랜덤 영상 클릭
        """
        print("[MobileHumanEvent] 🏠 모바일 유튜브 홈 이동 및 영상 클릭 시도")

        try:
            # 1. 홈 버튼 찾기 (모바일용 셀렉터)
            home_selectors = [
                "button[role='link'][aria-label*='YouTube 홈']",
                "button[role='link'][aria-label*='YouTube Home']",
                "button.logo-in-player-endpoint",
                "button[key='logo']",
                "c3-icon#home-icon",
                "#home-icon",
                "button:has(c3-icon#home-icon)",
                
                # 일반적인 YouTube 홈 버튼 셀렉터
                "a#logo",
                "ytd-topbar-logo-renderer a",
                "ytd-masthead a",
                "[href='/'][aria-label*='YouTube']",
                "button[aria-label*='홈']",
                "button[aria-label*='Home']",
                
                # 위치 기반 선택 (왼쪽 상단)
                "button:left-of(:text('YouTube'))",
                ":near(:text('YouTube'), 50) button",
            ]

            home_button = None
            for selector in home_selectors:
                try:
                    home_button = self.page.ele(f'css:{selector}', timeout=3)
                    if home_button:
                        print(f"   [MobileHumanEvent] 홈 버튼 발견: {selector}")
                        break
                except:
                    continue

            if home_button:
                self._sleep_human(0.3, 0.7)
                try:
                    home_button.click()
                except:
                    # JavaScript 클릭 시도
                    self.page.run_js("arguments[0].click();", home_button)
                
                print("   [MobileHumanEvent] ✅ 홈 버튼 클릭 완료")
                time.sleep(random.uniform(2, 4))
            else:
                print("   [MobileHumanEvent] ⚠️ 홈 버튼 미발견, 현재 페이지에서 진행")

            # 2. 랜덤 스크롤 다운 (1~20번)
            scroll_count = random.randint(1, 20)
            print(f"   [MobileHumanEvent] {scroll_count}번 스크롤 다운 예정")

            for i in range(scroll_count):
                self.page.actions.key_down('DOWN').key_up('DOWN')
                time.sleep(random.uniform(0.5, 1.5))

                if (i + 1) % 5 == 0:
                    print(f"   [MobileHumanEvent] {i+1}/{scroll_count} 스크롤 완료")

            print(f"   [MobileHumanEvent] ✅ 스크롤 다운 완료 ({scroll_count}번)")

            # 3. 스크롤 후 대기
            time.sleep(random.uniform(1, 2))

            # 4. 화면에 보이는 비디오 찾기
            video_selectors = [
                "tag:ytm-video-with-context-renderer",
                "tag:ytm-compact-video-renderer",
                "tag:ytm-rich-item-renderer",
                "css:a.media-item-thumbnail-container",
            ]

            videos = []
            for selector in video_selectors:
                try:
                    found = self.page.eles(selector, timeout=3)
                    if found:
                        # 표시되고 클릭 가능한 요소만 필터링
                        for v in found:
                            try:
                                # DrissionPage는 자동으로 표시 여부 체크
                                videos.append(v)
                            except:
                                continue
                    
                    if videos:
                        print(f"   [MobileHumanEvent] 비디오 발견: {len(videos)}개")
                        break
                except:
                    continue

            if not videos:
                print("   [MobileHumanEvent] ⚠️ 비디오를 찾을 수 없음")
                return

            # 5. 랜덤 비디오 선택 (1~10번째 중)
            max_video = min(10, len(videos))
            video_index = random.randint(0, max_video - 1)
            selected_video = videos[video_index]

            print(f"   [MobileHumanEvent] 선택된 비디오: {video_index + 1}번째")

            # 6. 비디오 클릭
            try:
                selected_video.click()
                print("   [MobileHumanEvent] ✅ 비디오 클릭 완료")
                time.sleep(random.uniform(3, 5))
                print("   [MobileHumanEvent] ✅ 영상 시청페이지로 이동 완료")

            except Exception as e:
                print(f"   [MobileHumanEvent] ⚠️ 비디오 클릭 실패: {e}")

        except Exception as e:
            print(f"[MobileHumanEvent] ❌ 홈 이동 및 영상 클릭 실패: {e}")

    # ========================================
    # 3️⃣ search_and_click_video - 검색 후 영상 클릭
    # ========================================
    def search_and_click_video(self):
        """
        홈 이동 → 검색창 찾기 → 키워드 입력 → 결과에서 1~10번째 중 클릭
        """
        print("[MobileHumanEvent] 🔍 모바일 유튜브 홈 이동 및 검색 시도")

        try:
            # 1. 홈 버튼 클릭 (선택사항)
            home_selectors = [
                "button[role='link'][aria-label*='YouTube 홈']",
                "button[role='link'][aria-label*='YouTube Home']",
                "button.logo-in-player-endpoint",
                "button[key='logo']",
                "c3-icon#home-icon",
                "#home-icon",
                "button:has(c3-icon#home-icon)",
                
                # 일반적인 YouTube 홈 버튼 셀렉터
                "a#logo",
                "ytd-topbar-logo-renderer a",
                "ytd-masthead a",
                "[href='/'][aria-label*='YouTube']",
                "button[aria-label*='홈']",
                "button[aria-label*='Home']",
                
                # 위치 기반 선택 (왼쪽 상단)
                "button:left-of(:text('YouTube'))",
                ":near(:text('YouTube'), 50) button",
            ]

            for selector in home_selectors:
                try:
                    home_button = self.page.ele(f'css:{selector}', timeout=3)
                    if home_button:
                        self._sleep_human(0.3, 0.7)
                        home_button.click()
                        print("   [MobileHumanEvent] ✅ 홈 버튼 클릭 완료")
                        time.sleep(random.uniform(2, 4))
                        break
                except:
                    continue

            # 2. 검색창 찾기
            search_box = self._find_search_box()

            if not search_box:
                print("   [MobileHumanEvent] ⚠️ 검색창을 찾을 수 없음")
                return

            # 3. 랜덤 키워드 선택
            keyword = random.choice(self.keywords)
            print(f"   [MobileHumanEvent] 검색 키워드: '{keyword}'")

            self._sleep_human(0.5, 1.0)

            # 4. 검색창 클릭
            try:
                search_box.click()
            except:
                pass

            self._sleep_human(0.3, 0.6)

            # 5. 기존 내용 지우기 (선택사항)
            try:
                search_box.clear()
            except:
                pass

            # 6. 타이핑 (인간처럼)
            for char in keyword:
                try:
                    search_box.input(char)
                    time.sleep(random.uniform(0.05, 0.15))
                except:
                    break

            self._sleep_human(0.3, 0.6)

            # 7. 엔터로 검색 실행
            try:
                self.page.actions.key_down('ENTER').key_up('ENTER')
                print("   [MobileHumanEvent] ✅ 검색 실행")
            except:
                print("   [MobileHumanEvent] ⚠️ 엔터 키 실패")

            # 8. 검색 결과 대기
            time.sleep(random.uniform(4, 8))

            # 9. 비디오 찾기
            video_selectors = [
                "tag:ytm-video-with-context-renderer",
                "tag:ytm-compact-video-renderer",
                "css:a.media-item-thumbnail-container",
            ]

            videos = []
            for selector in video_selectors:
                try:
                    found = self.page.eles(selector, timeout=5)
                    if found:
                        videos.extend(found)
                    
                    if videos:
                        print(f"   [MobileHumanEvent] 비디오 발견: {len(videos)}개")
                        break
                except:
                    continue

            if not videos:
                print("   [MobileHumanEvent] ⚠️ 검색 결과에서 비디오를 찾을 수 없음")
                return

            # 10. 랜덤 비디오 선택 (1~10번째)
            max_video = min(10, len(videos))
            video_index = random.randint(0, max_video - 1)
            selected_video = videos[video_index]

            print(f"   [MobileHumanEvent] 선택된 비디오: {video_index + 1}번째")

            # 11. 비디오 클릭
            try:
                selected_video.click()
                print("   [MobileHumanEvent] ✅ 비디오 클릭 완료")
                time.sleep(random.uniform(3, 5))
                print("   [MobileHumanEvent] ✅ 영상 시청페이지로 이동 완료")

            except Exception as e:
                print(f"   [MobileHumanEvent] ⚠️ 비디오 클릭 실패: {e}")

        except Exception as e:
            print(f"[MobileHumanEvent] ❌ 홈 이동 및 검색 실패: {e}")

    # ========================================
    # 헬퍼 함수
    # ========================================
    def _find_search_box(self):
        """
        검색창을 찾는 함수 (모바일용)
        """
        # 1. 먼저 검색 버튼 클릭 시도 (모바일에서는 검색 버튼을 먼저 눌러야 할 수 있음)
        search_button_selectors = [
            "button[aria-label='Search YouTube']",
            "button.icon-button.topbar-menu-button-avatar-button",
            "button[aria-label*='Search'][aria-label*='YouTube']",
        ]

        for btn_selector in search_button_selectors:
            try:
                search_button = self.page.ele(f'css:{btn_selector}', timeout=2)
                if search_button:
                    search_button.click()
                    print("   [MobileHumanEvent] 검색 버튼 클릭")
                    time.sleep(random.uniform(0.5, 1.0))
                    break
            except:
                continue

        # 2. 검색창 찾기
        search_selectors = [
            "input#searchbox-input",
            "input[name='search_query']",
            "input[placeholder='검색']",
            "input[placeholder='Search']",
            "input[type='text'][role='combobox']",
            "ytm-search-box input",
            "input.searchbox-input",
            "input#search",
            "#search-input input",
            "ytd-searchbox input",
            "input[type='search']",
        ]

        for selector in search_selectors:
            try:
                search_box = self.page.ele(f'css:{selector}', timeout=3)
                if search_box:
                    print(f"   [MobileHumanEvent] 검색창 찾음: {selector}")
                    return search_box
            except:
                continue

        return None
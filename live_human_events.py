# human_events.py

import random
import time
from typing import Dict, List
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


        
class HumanEventMobile:
    """
    인간과 같은 이벤트를 생성하는 클래스 (모바일 웹용)
    다양한 동작 중 랜덤으로 선택하여 실행
    """

    ACTIONS = (
        "mouse_scroll",
        "click_youtube_home",
        "search_and_click_video",
        #"combine_action",
    )

    #ACTION_WEIGHTS = (0.2, 0.3, 0.3, 0.2)
    ACTION_WEIGHTS = (0.3, 0.3, 0.4)

    def __init__(self, driver):
        self.driver = driver
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
            "peliculas completas en español latino", "dhar mann", "candace owens", "batang quiapo", "my mix",
            "skibidi toilet", "neon man", "moresidemen", "stand up comedy", "movies",
            "tn en vivo", "elon musk", "barcelona", "lisa", "movie reaction",
            "los ratones", "kai cenat", "aaj tak live", "assassin's creed shadows", "sabrina carpenter",
            "india's got latent", "mukbang", "blue", "michael jackson", "meem se mohabbat"
        ]
        
        # YouTube 모바일 요소 선택자
        self.selectors = {
            'home_button': 'a[aria-label="YouTube 홈"]',
            'search_button': 'button[aria-label="검색"]',
            'search_box': 'input[type="text"]',
            'video_links': 'a.media-item-thumbnail-container',
            'shorts_container': 'ytm-shorts',
        }
    
    def execute_random_action(self) -> bool:
        """
        랜덤으로 동작을 선택하고 실행
        Returns: 실행 성공 여부
        """
        actions = [getattr(self, name) for name in self.ACTIONS]
        weights = list(self.ACTION_WEIGHTS)
        
        if len(weights) != len(actions):
            weights = [0] * len(actions)

        if sum(weights) > 0:
            selected_action = random.choices(actions, weights=weights, k=1)[0]
        else:
            selected_action = random.choice(actions)
        
        print(f"[HumanEventMobile] 선택된 동작: {selected_action.__name__}")
        
        try:
            selected_action()
            return True
        except Exception as e:
            print(f"[HumanEventMobile] 동작 실행 중 오류: {e}")
            return False
    
    def combine_action(self) -> bool:
        """
        ACTIONS 중 서로 중복되지 않게 2개를 랜덤 선택해서, 순차 실행
        Returns: 전체 실행 성공 여부
        """
        candidates = [n for n in self.ACTIONS if n != "combine_action"]
        picked_names = random.sample(candidates, 2)
        print(f"[HumanEventMobile] combine_action 선택: {picked_names}")

        for name in picked_names:
            action = getattr(self, name, None)
            if not callable(action):
                print(f"[HumanEventMobile] ⚠️ 액션 메서드가 존재하지 않음: {name}")
                return False

            print(f"[HumanEventMobile] combine_action 실행: {name}")
            try:
                action()
            except Exception as e:
                print(f"[HumanEventMobile] combine_action 중 오류({name}): {e}")
                return False

        return True

    def mouse_scroll(self):
        """
        키보드 다운버튼을 통해 1~20까지중 랜덤으로 페이지 이동후 대기(클릭 안함)
        모바일에서도 키보드 다운 이벤트 사용
        """
        print("[HumanEventMobile] 모바일 스크롤 실행")
        
        try:
            # 현재 URL 확인
            current_url = self.driver.current_url.lower()
            
            # 랜덤 스크롤 횟수 결정
            scroll_count = random.randint(1, 20)
            print(f"   [HumanEventMobile] {scroll_count}번 스크롤 예정")
            
            # Shorts인 경우
            if "shorts" in current_url:
                print("   [HumanEventMobile] YouTube Shorts 감지 - N번째 영상으로 이동")
            
            # body 요소 찾기
            try:
                body = self.driver.find_element(By.TAG_NAME, 'body')
                
                for i in range(scroll_count):
                    # 키보드 ARROW_DOWN 이벤트 발생
                    body.send_keys(Keys.ARROW_DOWN)
                    time.sleep(random.uniform(0.5, 2.))
                    print(f"   [HumanEventMobile] {i+1}/{scroll_count} 이동 완료")
                
                print(f"   [HumanEventMobile] ✅ 스크롤 완료 ({scroll_count}번)")
                    
            except Exception as e:
                print(f"   [HumanEventMobile] ⚠️ 스크롤 중 오류: {e}")
                
        except Exception as e:
            print(f"[HumanEventMobile] ❌ 스크롤 실패: {e}")
 
    
    def search_and_click_video(self):
        """
        유튜브 홈 버튼을 찾아 클릭하여 홈으로 이동 후 검색 수행, 검색결과페이지내에서
        1~10까지중 렌덤하게 클릭해서 시청페이지 이동
        명시적 대기를 사용하여 페이지 로딩 확인
        """
        print("[HumanEventMobile] 모바일 유튜브 홈 이동 및 검색 시도")
        
        try:
            # 1. 홈 버튼 찾기 (모바일용 선택자)
            home_selectors = [
                    "button[role='link'][aria-label*='YouTube 홈']",
                    "button[role='link'][aria-label*='YouTube Home']",
                    "button.logo-in-player-endpoint",
                    "button[key='logo']",
                    "c3-icon#home-icon",
                    "#home-icon",
                    "button:has(c3-icon#home-icon)",
                    
                    # 일반적인 YouTube 홈 버튼 선택자
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
                    home_button = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    if home_button:
                        break
                except:
                    continue
            
            if home_button:
                time.sleep(random.uniform(0.3, 0.7))
                home_button.click()
                print("   [HumanEventMobile] ✅ 홈 버튼 클릭 완료")
                
                # 페이지 로딩 대기
                try:
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: "youtube.com" in driver.current_url
                    )
                    time.sleep(random.uniform(2, 4))
                    print("   [HumanEventMobile] ✅ 홈 페이지 로드 완료")
                except TimeoutException:
                    print("   [HumanEventMobile] ⚠️ 페이지 로딩 시간 초과")
                    time.sleep(random.uniform(3, 5))
            
            # 2. 검색 실행
            search_box = self.find_search_box()
            
            if not search_box:
                print("   [HumanEventMobile] ⚠️ 검색창을 찾을 수 없음")
                return
            
            # 3. 랜덤 키워드 선택 및 검색
            keyword = random.choice(self.keywords)
            print(f"   [HumanEventMobile] 검색 키워드: '{keyword}'")
            
            time.sleep(random.uniform(0.5, 1.0))
            
            try:
                search_box.click()
            except:
                self.driver.execute_script("arguments[0].click();", search_box)
            
            time.sleep(random.uniform(0.3, 0.6))
            
            # 기존 내용 지우기
            try:
                search_box.clear()
            except:
                pass
            
            # 타이핑
            for char in keyword:
                try:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.05, 0.15))
                except:
                    break
            
            time.sleep(random.uniform(0.3, 0.6))
            
            # 4. 검색 실행
            try:
                search_box.send_keys(Keys.RETURN)
                print("   [HumanEventMobile] ✅ 검색 실행")
            except:
                print("   [HumanEventMobile] ⚠️ 엔터 키 실패")
            
            # 5. 검색 결과 대기
            time.sleep(random.uniform(4, 8))
            
            # 6. 비디오 찾기 (모바일용 선택자)
            video_selectors = [
                'ytm-video-with-context-renderer a',
                'ytm-compact-video-renderer a',
                'a.media-item-thumbnail-container',
                'ytm-item-section-renderer a',
            ]
            
            videos = []
            for selector in video_selectors:
                try:
                    found_videos = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for video in found_videos:
                        try:
                            if video.is_displayed() and video.is_enabled():
                                videos.append(video)
                        except:
                            continue
                    
                    if videos:
                        print(f"   [HumanEventMobile] 비디오 찾음: {len(videos)}개")
                        break
                except:
                    continue
            
            if not videos:
                print("   [HumanEventMobile] ⚠️ 검색 결과에서 비디오를 찾을 수 없음")
                return
            
            # 7. 랜덤 비디오 선택
            max_video = min(10, len(videos))
            video_index = random.randint(0, max_video - 1)
            selected_video = videos[video_index]
            
            print(f"   [HumanEventMobile] 선택된 비디오: {video_index + 1}번째")
            
            # 8. 비디오 클릭
            try:
                self.driver.execute_script("arguments[0].click();", selected_video)
                print("   [HumanEventMobile] ✅ 비디오 클릭 완료")
                time.sleep(random.uniform(3, 5))
                print("   [HumanEventMobile] ✅ 영상 시청페이지로 이동 완료")
                
            except Exception as e:
                print(f"   [HumanEventMobile] ⚠️ 비디오 클릭 실패: {e}")
                
        except Exception as e:
            print(f"[HumanEventMobile] ❌ 홈 이동 및 검색 실패: {e}")
    
    def find_search_box(self):
        """
        검색창을 찾는 별도 함수 (모바일용)
        """
        # 모바일 YouTube 검색창 선택자
        search_selectors = [
            'input#searchbox-input',
            'input[name="search_query"]',
            'input[placeholder="검색"]',
            'input[placeholder="Search"]',
            'input[type="text"][role="combobox"]',
            'ytm-search-box input',
            'input.searchbox-input',
            "input#search",
            "#search-input input",
            "ytd-searchbox input",
            "input[type='search']",
            "input[name='search_query']",            
        ]
        
        search_box = None
        
        # 먼저 검색 버튼 클릭 시도 (모바일에서는 검색 버튼을 먼저 눌러야 할 수 있음)
        try:
            search_button_selectors = [
                "button[aria-label='Search YouTube']",
                "button.icon-button.topbar-menu-button-avatar-button",
                "button[aria-label*='Search'][aria-label*='YouTube']",
            ]
            
            for btn_selector in search_button_selectors:
                try:
                    search_button = self.driver.find_element(By.CSS_SELECTOR, btn_selector)
                    if search_button.is_displayed():
                        search_button.click()
                        print("   [HumanEventMobile] 검색 버튼 클릭")
                        time.sleep(random.uniform(0.5, 1.0))
                        break
                except:
                    continue
        except:
            pass
        
        # 검색창 찾기
        for selector in search_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    try:
                        if elem.is_displayed() and elem.is_enabled():
                            search_box = elem
                            print(f"   [HumanEventMobile] 검색창 찾음: {selector}")
                            break
                    except:
                        continue
                if search_box:
                    break
            except:
                continue
        
        return search_box
    
    def click_youtube_home(self):
        """
        유튜브 홈 버튼을 찾아 홈으로 이동 후 1~20까지중 랜덤하게 스크롤 다운해서 영상클릭해서 시청페이지 진입
        명시적 대기를 사용하여 페이지 로딩 확인
        """
        print("[HumanEventMobile] 모바일 유튜브 홈 이동 및 영상 클릭 시도")
        
        try:
            # 1. 홈 버튼 찾기 (모바일용 선택자)
            home_selectors = [
                "button[role='link'][aria-label*='YouTube 홈']",
                "button[role='link'][aria-label*='YouTube Home']",
                "button.logo-in-player-endpoint",
                "button[key='logo']",
                "c3-icon#home-icon",
                "#home-icon",
                "button:has(c3-icon#home-icon)",
                
                # 일반적인 YouTube 홈 버튼 선택자
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
                    home_button = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    if home_button:
                        break
                except:
                    continue
            
            if home_button:
                time.sleep(random.uniform(0.3, 0.7))
                home_button.click()
                print("   [HumanEventMobile] ✅ 홈 버튼 클릭 완료")
                
                # 페이지 로딩 대기
                try:
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: "youtube.com" in driver.current_url
                    )
                    time.sleep(random.uniform(2, 4))
                    print("   [HumanEventMobile] ✅ 홈 페이지 로드 완료")
                except TimeoutException:
                    print("   [HumanEventMobile] ⚠️ 페이지 로딩 시간 초과")
                    time.sleep(random.uniform(3, 5))
            else:
                print("   [HumanEventMobile] ⚠️ 홈 버튼을 찾을 수 없음, 현재 페이지에서 진행")
            
            # 2. 랜덤 스크롤 다운 (1~20번)
            scroll_count = random.randint(1, 20)
            print(f"   [HumanEventMobile] {scroll_count}번 스크롤 다운 예정")
            
            try:
                body = self.driver.find_element(By.TAG_NAME, 'body')
                
                for i in range(scroll_count):
                    body.send_keys(Keys.ARROW_DOWN)
                    time.sleep(random.uniform(0.5, 1.5))
                    print(f"   [HumanEventMobile] {i+1}/{scroll_count} 스크롤 완료")
                
                print(f"   [HumanEventMobile] ✅ 스크롤 다운 완료 ({scroll_count}번)")
                
            except Exception as e:
                print(f"   [HumanEventMobile] ⚠️ 스크롤 중 오류: {e}")
            
            # 3. 스크롤 후 대기
            time.sleep(random.uniform(1, 2))
            
            # 4. 현재 화면에 보이는 비디오 찾기 (모바일용 선택자)
            video_selectors = [
                'ytm-video-with-context-renderer a',
                'ytm-compact-video-renderer a',
                'a.media-item-thumbnail-container',
                'ytm-item-section-renderer a',
                'ytm-rich-item-renderer a',
            ]
            
            videos = []
            for selector in video_selectors:
                try:
                    found_videos = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for video in found_videos:
                        try:
                            if video.is_displayed() and video.is_enabled():
                                videos.append(video)
                        except:
                            continue
                    
                    if videos:
                        print(f"   [HumanEventMobile] 비디오 찾음: {len(videos)}개")
                        break
                except:
                    continue
            
            if not videos:
                print("   [HumanEventMobile] ⚠️ 비디오를 찾을 수 없음")
                return
            
            # 5. 랜덤 비디오 선택 (1~10번째 중)
            max_video = min(10, len(videos))
            video_index = random.randint(0, max_video - 1)
            selected_video = videos[video_index]
            
            print(f"   [HumanEventMobile] 선택된 비디오: {video_index + 1}번째")
            
            # 6. 비디오 클릭
            try:
                # 비디오 제목 가져오기
                video_title = selected_video.get_attribute('title') or selected_video.text
                if video_title:
                    print(f"   [HumanEventMobile] 비디오 제목: {video_title[:50]}...")
                
                self.driver.execute_script("arguments[0].click();", selected_video)
                print("   [HumanEventMobile] ✅ 비디오 클릭 완료")
                time.sleep(random.uniform(3, 5))
                print("   [HumanEventMobile] ✅ 영상 시청페이지로 이동 완료")
                
            except Exception as e:
                print(f"   [HumanEventMobile] ⚠️ 비디오 클릭 실패: {e}")
                
        except Exception as e:
            print(f"[HumanEventMobile] ❌ 홈 이동 및 영상 클릭 실패: {e}")
 
        


class HumanEvent:
    """
    인간과 같은 이벤트를 생성하는 클래스
    다양한 동작 중 랜덤으로 선택하여 실행
    """

    # ✅ 클래스 전역(클래스 변수) 액션 목록
    # - 클래스 변수에서는 self를 직접 참조할 수 없으므로 "메서드 이름(str)"로 보관합니다.
    # - 실행 시 getattr(self, name)으로 바운드 메서드를 꺼내 호출합니다.
    ACTIONS = (
        "mouse_scroll",
        "click_youtube_home",
        "search_and_click_video",
        "combine_action",  # ✅ 액션 리스트 마지막에 포함
    )

    # ✅ 액션 가중치(임시: 전부 0)
    # - weights 합이 0이면 execute_random_action()은 균등 랜덤(random.choice)으로 동작하도록 처리했습니다.
    # - 원하는 값으로 나중에 수정하세요. (ACTIONS와 길이 동일해야 함)
    ACTION_WEIGHTS = (0.2, 0.3, 0.3, 0.2)

    def __init__(self, driver):
        self.driver = driver
        self.keywords = [

        ]
        self.keywords = [
            "mr redpanda", "funny videos", "gaming", "cooking", "sports",
            "snow", "christmas", "travel", "redpanda", "entertainment",
            "comedy", "movies", "snowman", "reviews", "puppy",
            "asmr", "happy", "trailers", "podcasts", "cute"
            "asmr", "trump", "mrbeast", "music", "lofi",
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
            "peliculas completas en español latino", "dhar mann", "candace owens", "batang quiapo", "my mix",
            "skibidi toilet", "neon man", "moresidemen", "stand up comedy", "movies",
            "tn en vivo", "elon musk", "barcelona", "lisa", "movie reaction",
            "los ratones", "kai cenat", "aaj tak live", "assassin's creed shadows", "sabrina carpenter",
            "india's got latent", "mukbang", "blue", "michael jackson", "meem se mohabbat"
        ]
        
        
        # YouTube 요소 선택자
        self.selectors = {
            'home_button': 'a#logo',  # YouTube 홈 버튼
            'search_box': 'input#search',  # 검색창
            'search_button': 'button#search-icon-legacy',  # 검색 버튼
            'video_links': 'ytd-video-renderer a#video-title',  # 비디오 링크
            'shorts_next': '#shorts-player',  # Shorts 다음 버튼 (스크롤 대체)
        }
    
    def execute_random_action(self) -> bool:
        """
        랜덤으로 동작을 선택하고 실행
        Returns: 실행 성공 여부
        """
        # ✅ 클래스 전역 ACTIONS에서 바운드 메서드로 변환
        actions = [getattr(self, name) for name in self.ACTIONS]

        # ✅ 가중치 기반 선택 (임시: ACTION_WEIGHTS 기본값은 전부 0)
        weights = list(self.ACTION_WEIGHTS)
        if len(weights) != len(actions):
            weights = [0] * len(actions)

        if sum(weights) > 0:
            selected_action = random.choices(actions, weights=weights, k=1)[0]
        else:
            # weights가 전부 0이면 균등 랜덤으로 선택
            selected_action = random.choice(actions)
        
        print(f"[HumanEvent] 선택된 동작: {selected_action.__name__}")
        
        try:
            selected_action()
            return True
        except Exception as e:
            print(f"[HumanEvent] 동작 실행 중 오류: {e}")
            return False
    
    def combine_action(self) -> bool:
        """
        ACTIONS 중 서로 중복되지 않게 2개를 랜덤 선택해서, 순차 실행합니다.
        Returns: 전체 실행 성공 여부
        """
        candidates = [n for n in self.ACTIONS if n != "combine_action"]
        picked_names = random.sample(candidates, 2)
        print(f"[HumanEvent] combine_action 선택: {picked_names}")

        for name in picked_names:
            action = getattr(self, name, None)
            if not callable(action):
                print(f"[HumanEvent] ⚠️ 액션 메서드가 존재하지 않음: {name}")
                return False

            print(f"[HumanEvent] combine_action 실행: {name}")
            try:
                action()
            except Exception as e:
                print(f"[HumanEvent] combine_action 중 오류({name}): {e}")
                return False

        return True


    def mouse_scroll(self):
        """
        마우스 스크롤 (다음 영상으로 넘어가는 정도)
        수정: Shorts에서 N번째 영상으로 랜덤 이동
        """
        print("[HumanEvent] 마우스 스크롤 실행")
        
        try:
            # 1. YouTube Shorts인지 확인
            current_url = self.driver.current_url.lower()
            if "shorts" in current_url:
                print("   [HumanEvent] YouTube Shorts 감지 - N번째 영상으로 이동")
                
                # 랜덤 N값 결정 (1-20 사이)
                n = random.randint(1, 20)
                print(f"   [HumanEvent] {n}번째 영상으로 이동")
                
                # N번 ARROW_DOWN 키 입력
                try:
                    body = self.driver.find_element(By.TAG_NAME, 'body')
                    for i in range(n):
                        body.send_keys(Keys.ARROW_DOWN)
                        # 각 이동 사이에 약간의 지연 (인간처럼)
                        time.sleep(random.uniform(0.5, 1.5))
                        print(f"   [HumanEvent] {i+1}/{n} 이동 완료")
                    
                    print(f"   [HumanEvent] ✅ {n}번째 Shorts 영상으로 이동 완료")
                    return
                    
                except Exception as e:
                    print(f"   [HumanEvent] ⚠️ Shorts 이동 실패: {e}")
            
            # 2. 일반 페이지 스크롤 (기존 로직)
            try:
                scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                viewport_height = self.driver.execute_script("return window.innerHeight")
                current_pos = self.driver.execute_script("return window.pageYOffset")
                
                # 스크롤 가능한 높이 확인
                if scroll_height > viewport_height:
                    # 다음 컨텐츠 영역으로 스크롤
                    scroll_amount = random.randint(400, 800)
                    target_pos = min(current_pos + scroll_amount, scroll_height - viewport_height)
                    
                    # 부드러운 스크롤
                    step = random.randint(50, 150)
                    while current_pos < target_pos:
                        current_pos += step
                        self.driver.execute_script(f"window.scrollTo(0, {current_pos});")
                        time.sleep(random.uniform(0.02, 0.1))
                    
                    print(f"   [HumanEvent] ✅ 일반 페이지 스크롤 완료 ({scroll_amount}px 이동)")
                else:
                    self.driver.execute_script(f"window.scrollBy(0, {random.randint(100, 300)});")
                    print("   [HumanEvent] ✅ 미세 스크롤 완료")
                    
            except Exception as e:
                print(f"   [HumanEvent] ⚠️ 스크롤 중 오류: {e}")
                self.driver.execute_script("window.scrollBy(0, 500);")
                
        except Exception as e:
            print(f"[HumanEvent] ❌ 스크롤 실패: {e}")
        
    def click_youtube_home(self):
        """
        유튜브 홈 버튼을 찾아 클릭하여 홈으로 이동 후 검색 수행
        명시적 대기를 사용하여 페이지 로딩 확인
        """
        print("[HumanEvent] 유튜브 홈 이동 및 검색 시도")
        
        try:
            # 여러 방법으로 홈 버튼 찾기
            selectors = [
                'a#logo',  # YouTube 로고
                'a[title="YouTube Home"]',
                'a[aria-label="YouTube Home"]',
                'ytd-topbar-logo-renderer a',
                'yt-icon-button#logo-icon-button',
            ]
            
            home_button = None
            for selector in selectors:
                try:
                    home_button = WebDriverWait(self.driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    if home_button:
                        break
                except:
                    continue
            
            if home_button:
                # 마우스 이동 후 클릭
                time.sleep(random.uniform(0.3, 0.7))
                home_button.click()
                print("   [HumanEvent] ✅ 홈 버튼 클릭 완료")
                
                # ============ 명시적 대기 시작 ============
                try:
                    # 1. URL이 홈 페이지로 변경될 때까지 대기 (10초 제한)
                    WebDriverWait(self.driver, 10).until(
                        lambda driver: (
                            "youtube.com" in driver.current_url and 
                            ("/feed/" in driver.current_url or 
                            driver.current_url.endswith("youtube.com/"))
                        )
                    )
                    print("   [HumanEvent] ✅ 홈 페이지 URL 확인됨")
                    
                    # 2. 홈 페이지의 특징적인 요소들이 로드될 때까지 대기
                    home_page_selectors = [
                        'ytd-rich-grid-renderer',  # 추천 영상 그리드
                        '#contents ytd-rich-item-renderer',  # 영상 컨텐츠
                        'ytd-browse[page-subtype="home"]',  # 홈 페이지 표시자
                        '#primary #contents',  # 메인 컨텐츠 영역
                        'ytd-rich-grid-row',  # 영상 행
                    ]
                    
                    for selector in home_page_selectors:
                        try:
                            WebDriverWait(self.driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            print(f"   [HumanEvent] ✅ 홈 페이지 요소 로드됨: {selector}")
                            break
                        except:
                            continue
                    
                    # 3. 추가 안전장치: 페이지가 완전히 로드될 때까지 대기
                    WebDriverWait(self.driver, 5).until(
                        lambda driver: driver.execute_script("return document.readyState") == "complete"
                    )
                    print("   [HumanEvent] ✅ 페이지 완전 로드됨")
                    
                    # 4. 최소 대기 시간 보장 (랜덤)
                    time.sleep(random.uniform(1, 3))
                    
                except TimeoutException as e:
                    print(f"   [HumanEvent] ⚠️ 페이지 로딩 시간 초과, 계속 진행: {e}")
                    # 그래도 최소 대기 시간
                    time.sleep(random.uniform(3, 5))
                # ============ 명시적 대기 끝 ============

            
            # 못찾아도 현재 페이지내의 검색창을 찾아서 진행
            # 1. 검색창 찾기
            search_box = self.find_search_box()
            
            if not search_box:
                print("   [HumanEvent] ⚠️ 검색창을 찾을 수 없음")
                return
            
            # 2. 랜덤 키워드 선택
            keyword = random.choice(self.keywords)
            print(f"   [HumanEvent] 검색 키워드: '{keyword}'")
            
            # 3. 검색창 클릭 및 키워드 입력
            time.sleep(random.uniform(0.5, 1.0))
            
            # JavaScript로 클릭 시도
            try:
                self.driver.execute_script("arguments[0].click();", search_box)
            except:
                try:
                    search_box.click()
                except Exception as e:
                    print(f"   [HumanEvent] ⚠️ 검색창 클릭 실패: {e}")
                    return
            
            time.sleep(random.uniform(0.2, 0.5))
            
            # 기존 내용 지우기
            try:
                search_box.send_keys(Keys.CONTROL + "a")
                time.sleep(0.1)
                search_box.send_keys(Keys.DELETE)
            except:
                try:
                    search_box.clear()
                except:
                    pass
            
            time.sleep(random.uniform(0.1, 0.3))
            
            # 타이핑
            for char in keyword:
                try:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.05, 0.15))
                except:
                    break
            
            time.sleep(random.uniform(0.3, 0.6))
            
            # 4. 검색 실행
            try:
                search_box.send_keys(Keys.RETURN)
                print("   [HumanEvent] ✅ 검색 실행 (엔터 키)")
            except Exception as e:
                print(f"   [HumanEvent] ⚠️ 엔터 키 실패: {e}")
            
            # 5. 검색 결과 대기
            time.sleep(random.uniform(4, 8))
            
            # 6. 비디오 목록 찾기
            video_selectors = [
                'ytd-video-renderer a#video-title',
                'a#video-title',
                'ytd-video-renderer ytd-thumbnail a',
                '#contents ytd-video-renderer a',
            ]
            
            videos = []
            for selector in video_selectors:
                try:
                    found_videos = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for video in found_videos:
                        try:
                            if video.is_displayed() and video.is_enabled():
                                videos.append(video)
                        except:
                            continue
                    
                    if videos:
                        print(f"   [HumanEvent] 비디오 찾음: {len(videos)}개")
                        break
                except:
                    continue
            
            if not videos or len(videos) == 0:
                print("   [HumanEvent] ⚠️ 검색 결과에서 비디오를 찾을 수 없음")
                return
            
            # 7. 랜덤 비디오 선택 (1~10번째 중)
            max_video = min(10, len(videos))
            if max_video == 0:
                return
                
            video_index = random.randint(0, max_video - 1)
            selected_video = videos[video_index]
            
            print(f"   [HumanEvent] 선택된 비디오: {video_index + 1}번째")
            
            # 8. 비디오 클릭
            try:
                # 비디오 제목 가져오기
                video_title = selected_video.get_attribute('title') or selected_video.text
                if video_title:
                    print(f"   [HumanEvent] 비디오 제목: {video_title[:50]}...")
                
                # JavaScript로 클릭
                self.driver.execute_script("arguments[0].click();", selected_video)
                print("   [HumanEvent] ✅ 비디오 클릭 완료")
                
                # 9. 시청페이지 로딩 대기
                time.sleep(random.uniform(3, 5))
                print("   [HumanEvent] ✅ 영상 시청페이지로 이동 완료")
                
            except Exception as e:
                print(f"   [HumanEvent] ⚠️ 비디오 클릭 실패: {e}")
                
        except Exception as e:
            print(f"[HumanEvent] ❌ 홈 이동 및 검색 실패: {e}")
    
    def find_search_box(self):
        """
        검색창을 찾는 별도 함수
        원본 소스코드의 search_and_click_video 함수에서 검색창 찾기 로직만 분리
        """
        search_selectors = [
            'input.yt-searchbox-input',  # 메인 검색창 클래스
            'input[name="search_query"]',  # name 속성
            'input[placeholder="검색"]',  # 한국어 placeholder
            'input[placeholder="Search"]',  # 영어 placeholder
            'input[role="combobox"]',  # role 속성
            'yt-searchbox input',  # yt-searchbox 요소 내 input
            'form[action="/results"] input',  # 검색 폼 내 input
        ]
        
        search_box = None
        for selector in search_selectors:
            try:
                # 요소가 존재하는지 확인
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    try:
                        if elem.is_displayed() and elem.is_enabled():
                            search_box = elem
                            print(f"   [HumanEvent] 검색창 찾음: {selector}")
                            break
                    except:
                        continue
                if search_box:
                    break
            except:
                continue
        
        if not search_box:
            print("   [HumanEvent] ⚠️ 검색창을 찾을 수 없음")
            # 현재 페이지의 HTML 구조 로그 (디버깅용)
            try:
                page_html = self.driver.page_source[:1000]
                print(f"   [HumanEvent] 현재 페이지 일부 HTML: {page_html}")
            except:
                pass
        
        return search_box
    
    def search_and_click_video(self):
        """
        검색창을 찾아 랜덤 키워드로 검색 후 랜덤 동영상 클릭
        find_search_box 함수를 사용하여 검색창 찾기
        """
        print("[HumanEvent] 검색 및 동영상 클릭 시도")
        
        try:
            # 1. 검색창 찾기 (별도 함수 사용)
            search_box = self.find_search_box()
            
            if not search_box:
                print("   [HumanEvent] ⚠️ 검색창을 찾을 수 없음")
                return
            
            # 2. 랜덤 키워드 선택
            keyword = random.choice(self.keywords)
            print(f"   [HumanEvent] 검색 키워드: '{keyword}'")
            
            # 3. 검색창 클릭 및 키워드 입력
            time.sleep(random.uniform(0.5, 1.0))
            
            # JavaScript로 클릭 시도 (더 안정적)
            try:
                self.driver.execute_script("arguments[0].click();", search_box)
            except:
                try:
                    search_box.click()
                except Exception as e:
                    print(f"   [HumanEvent] ⚠️ 검색창 클릭 실패: {e}")
                    return
            
            time.sleep(random.uniform(0.2, 0.5))
            
            # 기존 내용 지우기 (Ctrl+A + Delete)
            try:
                search_box.send_keys(Keys.CONTROL + "a")
                time.sleep(0.1)
                search_box.send_keys(Keys.DELETE)
            except:
                try:
                    search_box.clear()
                except:
                    pass
            
            time.sleep(random.uniform(0.1, 0.3))
            
            # 인간처럼 타이핑 (글자마다 약간의 지연)
            for char in keyword:
                try:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.05, 0.15))
                except:
                    break
            
            time.sleep(random.uniform(0.3, 0.6))
            
            # 4. 검색 실행 (엔터 키)
            try:
                search_box.send_keys(Keys.RETURN)
                print("   [HumanEvent] ✅ 검색 실행 (엔터 키)")
            except Exception as e:
                print(f"   [HumanEvent] ⚠️ 엔터 키 실패: {e}")
                # 검색 버튼 찾아서 클릭 시도
                try:
                    button_selectors = [
                        'button.ytSearchboxComponentSearchButton',
                        'button[aria-label="Search"]',
                        'button[title="검색"]',
                        'button[type="submit"]',
                    ]
                    
                    for btn_selector in button_selectors:
                        try:
                            buttons = self.driver.find_elements(By.CSS_SELECTOR, btn_selector)
                            for btn in buttons:
                                if btn.is_displayed() and btn.is_enabled():
                                    btn.click()
                                    print("   [HumanEvent] ✅ 검색 버튼 클릭")
                                    break
                        except:
                            continue
                except:
                    pass
            
            # 5. 검색 결과 대기
            time.sleep(random.uniform(4, 8))
            
            # 6. 비디오 목록 찾기
            video_selectors = [
                'ytd-video-renderer a#video-title',
                'a#video-title',
                'ytd-video-renderer ytd-thumbnail a',
                '#contents ytd-video-renderer a',
            ]
            
            videos = []
            for selector in video_selectors:
                try:
                    found_videos = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for video in found_videos:
                        try:
                            if video.is_displayed() and video.is_enabled():
                                videos.append(video)
                        except:
                            continue
                    
                    if videos:
                        print(f"   [HumanEvent] 비디오 찾음: {len(videos)}개")
                        break
                except:
                    continue
            
            if not videos or len(videos) == 0:
                print("   [HumanEvent] ⚠️ 검색 결과에서 비디오를 찾을 수 없음")
                return
            
            # 7. 랜덤 비디오 선택 (1-10번째 중)
            max_video = min(10, len(videos))
            if max_video == 0:
                return
                
            video_index = random.randint(0, max_video - 1)
            selected_video = videos[video_index]
            
            print(f"   [HumanEvent] 선택된 비디오: {video_index + 1}번째")
            
            # 8. 비디오 클릭
            try:
                # 비디오 제목 가져오기
                video_title = selected_video.get_attribute('title') or selected_video.text
                if video_title:
                    print(f"   [HumanEvent] 비디오 제목: {video_title[:50]}...")
                
                # JavaScript로 클릭 (더 안정적)
                self.driver.execute_script("arguments[0].click();", selected_video)
                print("   [HumanEvent] ✅ 비디오 클릭 완료")
                
                # 9. 비디오 로딩 대기
                time.sleep(random.uniform(3, 5))
                
            except Exception as e:
                print(f"   [HumanEvent] ⚠️ 비디오 클릭 실패: {e}")
                # 대체: 첫 번째 비디오 시도
                try:
                    if videos and len(videos) > 0:
                        self.driver.execute_script("arguments[0].click();", videos[0])
                        print("   [HumanEvent] ✅ 첫 번째 비디오로 대체 클릭")
                except:
                    pass
                
        except Exception as e:
            print(f"[HumanEvent] ❌ 검색 및 클릭 실패: {e}")
# 사용 예시 (기존 코드에 통합하는 방법)
"""
# 1. HumanEvent 클래스 임포트
from human_events import HumanEvent

# 2. monitor_service 함수 내에서 사용
def monitor_service(...):
    # ... 기존 코드 ...
    
    # HumanEvent 인스턴스 생성
    human_event = HumanEvent(driver)
    
    # 랜덤 동작 실행
    human_event.execute_random_action()
    
    # 또는 특정 동작 실행
    # human_event.mouse_scroll()
    # human_event.click_youtube_home()
    # human_event.search_and_click_video()
    
    # ... 나머지 코드 ...
"""
# patchright_human_events.py
# Ported from the user's Selenium-based human_events.py to Patchright/Playwright async Page API.
#
# Usage:
#   from patchright_human_events import HumanEvent, HumanEventMobile
#   human = HumanEvent(page)          # desktop
#   human_m = HumanEventMobile(page)  # mobile
#   await human.execute_random_action()
#
# Notes:
# - This file assumes you pass a Patchright/Playwright-like `page` object (async API),
#   supporting: page.url, page.goto, page.wait_for_selector, page.wait_for_load_state,
#   page.locator, page.keyboard.press, page.evaluate, etc.

from __future__ import annotations

import asyncio
import random
from typing import Callable, Iterable, List, Optional, Sequence


class _HumanEventBase:
    def __init__(self, page):
        self.page = page

    async def _sleep(self, a: float, b: float) -> None:
        await asyncio.sleep(random.uniform(a, b))

    async def _first_visible_locator(
        self,
        selectors: Sequence[str],
        *,
        timeout_ms: int = 3000,
    ):
        """
        Return the first locator that becomes visible (or is already visible) among selectors.
        """
        for sel in selectors:
            try:
                loc = self.page.locator(sel).first
                # Try quick visibility check first (no waiting)
                try:
                    if await loc.is_visible():
                        return loc
                except Exception:
                    pass

                # Then wait a bit
                try:
                    await loc.wait_for(state="visible", timeout=timeout_ms)
                    return loc
                except Exception:
                    continue
            except Exception:
                continue
        return None

    async def _click_locator(self, loc, *, timeout_ms: int = 3000) -> bool:
        if loc is None:
            return False
        try:
            await loc.click(timeout=timeout_ms)
            return True
        except Exception:
            # Fallback: force click (some overlay situations)
            try:
                await loc.click(timeout=timeout_ms, force=True)
                return True
            except Exception:
                # Fallback: JS click
                try:
                    await self.page.evaluate("(el) => el.click()", await loc.element_handle())
                    return True
                except Exception:
                    return False

    async def _type_like_human(self, loc, text: str) -> None:
        """
        Human-ish typing: per-char delays.
        """
        for ch in text:
            try:
                await loc.type(ch, delay=int(random.uniform(30, 120)))
            except Exception:
                # Fallback: insert text via keyboard
                try:
                    await self.page.keyboard.type(ch, delay=int(random.uniform(30, 120)))
                except Exception:
                    break

    async def _clear_input(self, loc) -> None:
        # Try common patterns
        try:
            await loc.click()
        except Exception:
            pass

        # Ctrl+A Delete (Windows/Linux); also try Meta+A for mac layouts
        for combo in ("Control+A", "Meta+A"):
            try:
                await self.page.keyboard.press(combo)
                await asyncio.sleep(0.05)
                await self.page.keyboard.press("Delete")
                return
            except Exception:
                continue

        # Fallback: fill empty
        try:
            await loc.fill("")
        except Exception:
            pass

    async def _collect_clickable_anchors(self, selectors: Sequence[str], *, max_n: int = 50):
        """
        Collect visible & enabled anchors matching selectors.
        """
        items = []
        for sel in selectors:
            try:
                loc = self.page.locator(sel)
                count = await loc.count()
                if count <= 0:
                    continue

                for i in range(min(count, max_n)):
                    one = loc.nth(i)
                    try:
                        if await one.is_visible():
                            items.append(one)
                    except Exception:
                        continue

                if items:
                    return items
            except Exception:
                continue
        return items

    async def _wait_youtubeish(self, *, timeout_ms: int = 10000) -> None:
        """
        Best-effort wait after navigation.
        """
        try:
            await self.page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        except Exception:
            pass
        try:
            await self.page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except Exception:
            pass


class HumanEventMobile(_HumanEventBase):
    """
    인간과 같은 이벤트를 생성하는 클래스 (모바일 웹용) - Patchright/Playwright async 포팅
    """

    ACTIONS = (
        "mouse_scroll",
        "click_youtube_home",
        "search_and_click_video",
        # "combine_action",
    )

    ACTION_WEIGHTS = (0.3, 0.3, 0.4)

    def __init__(self, page):
        super().__init__(page)
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

        self.selectors = {
            "home_button": 'a[aria-label="YouTube 홈"]',
            "search_button": 'button[aria-label="검색"]',
            "search_box": 'input[type="text"]',
            "video_links": "a.media-item-thumbnail-container",
            "shorts_container": "ytm-shorts",
        }

    async def execute_random_action(self) -> bool:
        actions = [getattr(self, name) for name in self.ACTIONS]
        weights = list(self.ACTION_WEIGHTS)
        if len(weights) != len(actions):
            weights = [0] * len(actions)

        selected = random.choices(actions, weights=weights, k=1)[0] if sum(weights) > 0 else random.choice(actions)
        print(f"[HumanEventMobile] 선택된 동작: {selected.__name__}")

        try:
            await selected()
            return True
        except Exception as e:
            print(f"[HumanEventMobile] 동작 실행 중 오류: {e}")
            return False

    async def combine_action(self) -> bool:
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
                await action()
            except Exception as e:
                print(f"[HumanEventMobile] combine_action 중 오류({name}): {e}")
                return False

        return True

    async def mouse_scroll(self) -> None:
        """
        키보드 ArrowDown을 통해 1~20 랜덤으로 스크롤.
        Shorts인 경우도 동일하게 다음 영상으로 내려가게 됨.
        """
        print("[HumanEventMobile] 모바일 스크롤 실행")

        current_url = (getattr(self.page, "url", "") or "").lower()
        scroll_count = random.randint(1, 20)
        print(f"   [HumanEventMobile] {scroll_count}번 스크롤 예정")

        if "shorts" in current_url:
            print("   [HumanEventMobile] YouTube Shorts 감지 - N번째 영상으로 이동")

        for i in range(scroll_count):
            try:
                await self.page.keyboard.press("ArrowDown")
            except Exception:
                try:
                    await self.page.evaluate("() => window.scrollBy(0, 300)")
                except Exception:
                    pass
            await self._sleep(0.5, 2.0)
            print(f"   [HumanEventMobile] {i+1}/{scroll_count} 이동 완료")

        print(f"   [HumanEventMobile] ✅ 스크롤 완료 ({scroll_count}번)")

    async def find_search_box(self):
        """
        모바일 YouTube 검색창 찾기.
        """
        # 먼저 검색 버튼을 눌러야 input이 나타나는 경우가 있어 버튼 클릭 시도
        try:
            search_button_selectors = [
                "button[aria-label='Search YouTube']",
                "button.icon-button.topbar-menu-button-avatar-button",
                "button[aria-label*='Search'][aria-label*='YouTube']",
            ]
            btn = await self._first_visible_locator(search_button_selectors, timeout_ms=1500)
            if btn:
                if await self._click_locator(btn, timeout_ms=2000):
                    print("   [HumanEventMobile] 검색 버튼 클릭")
                    await self._sleep(0.5, 1.0)
        except Exception:
            pass

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

        loc = await self._first_visible_locator(search_selectors, timeout_ms=2000)
        if loc:
            print("   [HumanEventMobile] 검색창 찾음")
        return loc

    async def search_and_click_video(self) -> None:
        """
        홈으로 이동(가능하면) 후 검색 수행, 결과에서 1~10 중 랜덤 클릭.
        """
        print("[HumanEventMobile] 모바일 유튜브 홈 이동 및 검색 시도")

        # 1) 홈 버튼 시도
        home_selectors = [
            "button[role='link'][aria-label*='YouTube 홈']",
            "button[role='link'][aria-label*='YouTube Home']",
            "button.logo-in-player-endpoint",
            "button[key='logo']",
            "c3-icon#home-icon",
            "#home-icon",
            "button:has(c3-icon#home-icon)",
            "a#logo",
            "ytd-topbar-logo-renderer a",
            "ytd-masthead a",
            "[href='/'][aria-label*='YouTube']",
            "button[aria-label*='홈']",
            "button[aria-label*='Home']",
        ]

        home_btn = await self._first_visible_locator(home_selectors, timeout_ms=3000)
        if home_btn:
            await self._sleep(0.3, 0.7)
            if await self._click_locator(home_btn, timeout_ms=3000):
                print("   [HumanEventMobile] ✅ 홈 버튼 클릭 완료")
                await self._wait_youtubeish(timeout_ms=10000)
                await self._sleep(2, 4)
                print("   [HumanEventMobile] ✅ 홈 페이지 로드 완료")
        else:
            print("   [HumanEventMobile] ⚠️ 홈 버튼을 찾을 수 없음, 현재 페이지에서 진행")

        # 2) 검색창
        search_box = await self.find_search_box()
        if not search_box:
            print("   [HumanEventMobile] ⚠️ 검색창을 찾을 수 없음")
            return

        keyword = random.choice(self.keywords)
        print(f"   [HumanEventMobile] 검색 키워드: '{keyword}'")

        await self._sleep(0.5, 1.0)
        try:
            await search_box.click()
        except Exception:
            await self._click_locator(search_box, timeout_ms=2000)

        await self._sleep(0.2, 0.6)
        await self._clear_input(search_box)
        await self._sleep(0.1, 0.3)
        await self._type_like_human(search_box, keyword)
        await self._sleep(0.3, 0.6)

        # 검색 실행
        try:
            await self.page.keyboard.press("Enter")
            print("   [HumanEventMobile] ✅ 검색 실행")
        except Exception:
            print("   [HumanEventMobile] ⚠️ 엔터 키 실패")

        await self._sleep(4, 8)

        # 3) 비디오 찾고 클릭
        video_selectors = [
            "ytm-video-with-context-renderer a",
            "ytm-compact-video-renderer a",
            "a.media-item-thumbnail-container",
            "ytm-item-section-renderer a",
        ]

        videos = await self._collect_clickable_anchors(video_selectors, max_n=60)
        if not videos:
            print("   [HumanEventMobile] ⚠️ 검색 결과에서 비디오를 찾을 수 없음")
            return

        print(f"   [HumanEventMobile] 비디오 찾음: {len(videos)}개")

        max_video = min(10, len(videos))
        video_index = random.randint(0, max_video - 1)
        selected = videos[video_index]
        print(f"   [HumanEventMobile] 선택된 비디오: {video_index + 1}번째")

        if await self._click_locator(selected, timeout_ms=4000):
            print("   [HumanEventMobile] ✅ 비디오 클릭 완료")
            await self._sleep(3, 5)
            print("   [HumanEventMobile] ✅ 영상 시청페이지로 이동 완료")
        else:
            print("   [HumanEventMobile] ⚠️ 비디오 클릭 실패")

    async def click_youtube_home(self) -> None:
        """
        홈 이동 후 랜덤 스크롤(1~20) 뒤 랜덤 영상 클릭.
        """
        print("[HumanEventMobile] 모바일 유튜브 홈 이동 및 영상 클릭 시도")

        home_selectors = [
            "button[role='link'][aria-label*='YouTube 홈']",
            "button[role='link'][aria-label*='YouTube Home']",
            "button.logo-in-player-endpoint",
            "button[key='logo']",
            "c3-icon#home-icon",
            "#home-icon",
            "button:has(c3-icon#home-icon)",
            "a#logo",
            "ytd-topbar-logo-renderer a",
            "ytd-masthead a",
            "[href='/'][aria-label*='YouTube']",
            "button[aria-label*='홈']",
            "button[aria-label*='Home']",
        ]

        home_btn = await self._first_visible_locator(home_selectors, timeout_ms=3000)
        if home_btn:
            await self._sleep(0.3, 0.7)
            if await self._click_locator(home_btn, timeout_ms=3000):
                print("   [HumanEventMobile] ✅ 홈 버튼 클릭 완료")
                await self._wait_youtubeish(timeout_ms=10000)
                await self._sleep(2, 4)
                print("   [HumanEventMobile] ✅ 홈 페이지 로드 완료")
        else:
            print("   [HumanEventMobile] ⚠️ 홈 버튼을 찾을 수 없음, 현재 페이지에서 진행")

        # 스크롤 (ArrowDown)
        scroll_count = random.randint(1, 20)
        print(f"   [HumanEventMobile] {scroll_count}번 스크롤 다운 예정")
        for i in range(scroll_count):
            try:
                await self.page.keyboard.press("ArrowDown")
            except Exception:
                try:
                    await self.page.evaluate("() => window.scrollBy(0, 300)")
                except Exception:
                    pass
            await self._sleep(0.5, 1.5)
            print(f"   [HumanEventMobile] {i+1}/{scroll_count} 스크롤 완료")

        print(f"   [HumanEventMobile] ✅ 스크롤 다운 완료 ({scroll_count}번)")
        await self._sleep(1, 2)

        video_selectors = [
            "ytm-video-with-context-renderer a",
            "ytm-compact-video-renderer a",
            "a.media-item-thumbnail-container",
            "ytm-item-section-renderer a",
            "ytm-rich-item-renderer a",
        ]

        videos = await self._collect_clickable_anchors(video_selectors, max_n=60)
        if not videos:
            print("   [HumanEventMobile] ⚠️ 비디오를 찾을 수 없음")
            return

        print(f"   [HumanEventMobile] 비디오 찾음: {len(videos)}개")

        max_video = min(10, len(videos))
        video_index = random.randint(0, max_video - 1)
        selected = videos[video_index]
        print(f"   [HumanEventMobile] 선택된 비디오: {video_index + 1}번째")

        if await self._click_locator(selected, timeout_ms=4000):
            print("   [HumanEventMobile] ✅ 비디오 클릭 완료")
            await self._sleep(3, 5)
            print("   [HumanEventMobile] ✅ 영상 시청페이지로 이동 완료")
        else:
            print("   [HumanEventMobile] ⚠️ 비디오 클릭 실패")


class HumanEvent(_HumanEventBase):
    """
    인간과 같은 이벤트를 생성하는 클래스 (데스크탑 웹용) - Patchright/Playwright async 포팅
    """

    ACTIONS = (
        "mouse_scroll",
        "click_youtube_home",
        "search_and_click_video",
        "combine_action",
    )

    ACTION_WEIGHTS = (0.2, 0.3, 0.3, 0.2)

    def __init__(self, page):
        super().__init__(page)
        self.keywords = [
            "mr redpanda", "funny videos", "gaming", "cooking", "sports",
            "snow", "christmas", "travel", "redpanda", "entertainment",
            "comedy", "movies", "snowman", "reviews", "puppy",
            "asmr", "happy", "trailers", "podcasts", "cute",
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

        self.selectors = {
            "home_button": "a#logo",
            "search_box": "input#search",
            "search_button": "button#search-icon-legacy",
            "video_links": "ytd-video-renderer a#video-title",
            "shorts_next": "#shorts-player",
        }

    async def execute_random_action(self) -> bool:
        actions = [getattr(self, name) for name in self.ACTIONS]
        weights = list(self.ACTION_WEIGHTS)
        if len(weights) != len(actions):
            weights = [0] * len(actions)

        selected = random.choices(actions, weights=weights, k=1)[0] if sum(weights) > 0 else random.choice(actions)
        print(f"[HumanEvent] 선택된 동작: {selected.__name__}")

        try:
            await selected()
            return True
        except Exception as e:
            print(f"[HumanEvent] 동작 실행 중 오류: {e}")
            return False

    async def combine_action(self) -> bool:
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
                await action()
            except Exception as e:
                print(f"[HumanEvent] combine_action 중 오류({name}): {e}")
                return False

        return True

    async def mouse_scroll(self) -> None:
        """
        Shorts면 N번 ArrowDown으로 다음 영상 이동.
        일반 페이지면 랜덤 스크롤(부드럽게).
        """
        print("[HumanEvent] 마우스 스크롤 실행")

        current_url = (getattr(self.page, "url", "") or "").lower()
        if "shorts" in current_url:
            print("   [HumanEvent] YouTube Shorts 감지 - N번째 영상으로 이동")
            n = random.randint(1, 20)
            print(f"   [HumanEvent] {n}번째 영상으로 이동")

            for i in range(n):
                try:
                    await self.page.keyboard.press("ArrowDown")
                except Exception:
                    try:
                        await self.page.evaluate("() => window.scrollBy(0, 600)")
                    except Exception:
                        pass
                await self._sleep(0.5, 1.5)
                print(f"   [HumanEvent] {i+1}/{n} 이동 완료")

            print(f"   [HumanEvent] ✅ {n}번째 Shorts 영상으로 이동 완료")
            return

        # 일반 페이지: scrollHeight/viewport 등을 읽어서 랜덤 스크롤
        try:
            scroll_height = await self.page.evaluate("() => document.body.scrollHeight")
            viewport_height = await self.page.evaluate("() => window.innerHeight")
            current_pos = await self.page.evaluate("() => window.pageYOffset")

            if scroll_height and viewport_height and scroll_height > viewport_height:
                scroll_amount = random.randint(400, 800)
                target_pos = min(current_pos + scroll_amount, scroll_height - viewport_height)

                step = random.randint(50, 150)
                while current_pos < target_pos:
                    current_pos = min(current_pos + step, target_pos)
                    await self.page.evaluate("(y) => window.scrollTo(0, y)", current_pos)
                    await asyncio.sleep(random.uniform(0.02, 0.1))

                print(f"   [HumanEvent] ✅ 일반 페이지 스크롤 완료 ({scroll_amount}px 이동)")
            else:
                await self.page.evaluate("(dy) => window.scrollBy(0, dy)", random.randint(100, 300))
                print("   [HumanEvent] ✅ 미세 스크롤 완료")
        except Exception as e:
            print(f"   [HumanEvent] ⚠️ 스크롤 중 오류: {e}")
            try:
                await self.page.evaluate("() => window.scrollBy(0, 500)")
            except Exception:
                pass

    async def find_search_box(self):
        """
        데스크탑 YouTube 검색창 찾기.
        """
        search_selectors = [
            "input.yt-searchbox-input",
            "input[name='search_query']",
            "input[placeholder='검색']",
            "input[placeholder='Search']",
            "input[role='combobox']",
            "yt-searchbox input",
            "form[action='/results'] input",
            "input#search",
        ]
        loc = await self._first_visible_locator(search_selectors, timeout_ms=2500)
        if loc:
            print("   [HumanEvent] 검색창 찾음")
        else:
            print("   [HumanEvent] ⚠️ 검색창을 찾을 수 없음")
        return loc

    async def click_youtube_home(self) -> None:
        """
        홈 이동(가능하면) 후 검색 수행하고, 검색 결과에서 랜덤 영상 클릭.
        """
        print("[HumanEvent] 유튜브 홈 이동 및 검색 시도")

        home_selectors = [
            "a#logo",
            'a[title="YouTube Home"]',
            'a[aria-label="YouTube Home"]',
            "ytd-topbar-logo-renderer a",
            "yt-icon-button#logo-icon-button",
        ]

        home_btn = await self._first_visible_locator(home_selectors, timeout_ms=3000)
        if home_btn:
            await self._sleep(0.3, 0.7)
            if await self._click_locator(home_btn, timeout_ms=3000):
                print("   [HumanEvent] ✅ 홈 버튼 클릭 완료")

                # 홈 URL/요소 대기 (best-effort)
                try:
                    # URL이 youtube.com 이고 홈/feed 형태면 OK
                    await self.page.wait_for_function(
                        """() => {
                            const u = location.href;
                            return u.includes('youtube.com') && (u.includes('/feed/') || u.endsWith('youtube.com/') || u.endsWith('youtube.com'));
                        }""",
                        timeout=10000,
                    )
                    print("   [HumanEvent] ✅ 홈 페이지 URL 확인됨")
                except Exception as e:
                    print(f"   [HumanEvent] ⚠️ 홈 URL 확인 대기 실패: {e}")

                # 홈 페이지 특징적 요소 대기
                home_page_selectors = [
                    "ytd-rich-grid-renderer",
                    "#contents ytd-rich-item-renderer",
                    'ytd-browse[page-subtype="home"]',
                    "#primary #contents",
                    "ytd-rich-grid-row",
                ]
                await self._first_visible_locator(home_page_selectors, timeout_ms=5000)

                await self._wait_youtubeish(timeout_ms=8000)
                await self._sleep(1, 3)
        else:
            print("   [HumanEvent] ⚠️ 홈 버튼을 찾을 수 없음, 현재 페이지에서 진행")

        # 검색 로직
        search_box = await self.find_search_box()
        if not search_box:
            return

        keyword = random.choice(self.keywords)
        print(f"   [HumanEvent] 검색 키워드: '{keyword}'")

        await self._sleep(0.5, 1.0)
        await self._click_locator(search_box, timeout_ms=2000)
        await self._sleep(0.2, 0.5)

        await self._clear_input(search_box)
        await self._sleep(0.1, 0.3)
        await self._type_like_human(search_box, keyword)
        await self._sleep(0.3, 0.6)

        # 엔터로 검색
        try:
            await self.page.keyboard.press("Enter")
            print("   [HumanEvent] ✅ 검색 실행 (엔터 키)")
        except Exception as e:
            print(f"   [HumanEvent] ⚠️ 엔터 키 실패: {e}")
            # 검색 버튼 클릭 fallback
            btn_selectors = [
                "button.ytSearchboxComponentSearchButton",
                "button[aria-label='Search']",
                "button[title='검색']",
                "button[type='submit']",
                "button#search-icon-legacy",
            ]
            btn = await self._first_visible_locator(btn_selectors, timeout_ms=2000)
            await self._click_locator(btn, timeout_ms=2000)

        await self._sleep(4, 8)

        # 검색 결과에서 비디오 찾기
        video_selectors = [
            "ytd-video-renderer a#video-title",
            "a#video-title",
            "ytd-video-renderer ytd-thumbnail a",
            "#contents ytd-video-renderer a",
        ]
        videos = await self._collect_clickable_anchors(video_selectors, max_n=80)
        if not videos:
            print("   [HumanEvent] ⚠️ 검색 결과에서 비디오를 찾을 수 없음")
            return

        print(f"   [HumanEvent] 비디오 찾음: {len(videos)}개")

        max_video = min(10, len(videos))
        if max_video <= 0:
            return

        idx = random.randint(0, max_video - 1)
        selected = videos[idx]
        print(f"   [HumanEvent] 선택된 비디오: {idx + 1}번째")

        if await self._click_locator(selected, timeout_ms=4000):
            print("   [HumanEvent] ✅ 비디오 클릭 완료")
            await self._sleep(3, 5)
            print("   [HumanEvent] ✅ 영상 시청페이지로 이동 완료")
        else:
            print("   [HumanEvent] ⚠️ 비디오 클릭 실패")
            # fallback: 첫 번째 비디오
            if videos:
                try:
                    await self._click_locator(videos[0], timeout_ms=4000)
                    print("   [HumanEvent] ✅ 첫 번째 비디오로 대체 클릭")
                except Exception:
                    pass

    async def search_and_click_video(self) -> None:
        """
        검색창을 찾아 랜덤 키워드로 검색 후 랜덤 동영상 클릭 (홈 이동 없이).
        """
        print("[HumanEvent] 검색 및 동영상 클릭 시도")

        search_box = await self.find_search_box()
        if not search_box:
            return

        keyword = random.choice(self.keywords)
        print(f"   [HumanEvent] 검색 키워드: '{keyword}'")

        await self._sleep(0.5, 1.0)
        await self._click_locator(search_box, timeout_ms=2000)
        await self._sleep(0.2, 0.5)

        await self._clear_input(search_box)
        await self._sleep(0.1, 0.3)

        await self._type_like_human(search_box, keyword)
        await self._sleep(0.3, 0.6)

        # 검색 실행
        try:
            await self.page.keyboard.press("Enter")
            print("   [HumanEvent] ✅ 검색 실행 (엔터 키)")
        except Exception as e:
            print(f"   [HumanEvent] ⚠️ 엔터 키 실패: {e}")

        await self._sleep(4, 8)

        video_selectors = [
            "ytd-video-renderer a#video-title",
            "a#video-title",
            "ytd-video-renderer ytd-thumbnail a",
            "#contents ytd-video-renderer a",
        ]
        videos = await self._collect_clickable_anchors(video_selectors, max_n=80)
        if not videos:
            print("   [HumanEvent] ⚠️ 검색 결과에서 비디오를 찾을 수 없음")
            return

        print(f"   [HumanEvent] 비디오 찾음: {len(videos)}개")

        max_video = min(10, len(videos))
        if max_video <= 0:
            return

        idx = random.randint(0, max_video - 1)
        selected = videos[idx]
        print(f"   [HumanEvent] 선택된 비디오: {idx + 1}번째")

        if await self._click_locator(selected, timeout_ms=4000):
            print("   [HumanEvent] ✅ 비디오 클릭 완료")
            await self._sleep(3, 5)
        else:
            print("   [HumanEvent] ⚠️ 비디오 클릭 실패")

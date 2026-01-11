import json
import re
from collections import defaultdict
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from typing import Dict, Any, Optional
import time
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import math


class SteamProfileCommentParser:
    def __init__(self):
        """
        Steam profile comment parser with pagination support
        """
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        self.comments_data = defaultdict(lambda: {"count": 0, "comments": []})
        self.total_comments = 0
        self.profile_url = None
        self.comments_per_page = 50  # steam shows 50 comments per page
        self.base_comments_url = None
        self.seen_comment_ids = set()  # track unique comments

    def parse_profile(self, profile_url: str, max_pages: int = 200) -> Dict[str, Any]:
        """
        Main parsing function
        
        Args:
            profile_url: Steam profile URL
            max_pages: maximum pages to parse
            
        Returns:
            dictionary with comment data
        """
        self.profile_url = profile_url
        print(f"starting profile parsing: {profile_url}")

        try:
            # load first page
            print("loading first page...")
            response = self.session.get(profile_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # get total comment count
            total_comments = self._get_total_comments_count(soup)
            print(f"total comments according to steam: {total_comments}")

            if total_comments > 0:
                # get all comments URL
                all_comments_url = self._get_all_comments_url(soup, profile_url)

                if all_comments_url:
                    self.base_comments_url = all_comments_url
                    print(f"comments base URL: {all_comments_url}")

                    # calculate pages to parse
                    total_pages = math.ceil(total_comments / self.comments_per_page)
                    if max_pages:
                        total_pages = min(total_pages, max_pages)

                    print(f"total pages to parse: {total_pages}")

                    # parse first page
                    print("parsing first page...")
                    self._parse_comments_from_html(response.text)

                    # parse remaining pages
                    if total_pages > 1:
                        self._parse_all_pages_optimized(total_pages)
                else:
                    # fallback to first page only
                    print("all comments link not found, parsing first page only...")
                    self._parse_comments_from_html(response.text)
            else:
                print("no comments to parse")
                self._parse_comments_from_html(response.text)

            print(f"\nparsing complete!")
            print(f"total comments parsed: {self.total_comments}")
            print(f"total users: {len(self.comments_data)}")

            return dict(self.comments_data)

        except requests.RequestException as e:
            print(f"page load error: {e}")
            return {}
        except Exception as e:
            print(f"unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def _get_total_comments_count(self, soup: BeautifulSoup) -> int:
        """extract total comment count from page"""
        try:
            # method 1: look for InitializeCommentThread script
            script_tags = soup.find_all("script")
            for script in script_tags:
                if script.string and "InitializeCommentThread" in script.string:
                    pattern = r"InitializeCommentThread\s*\(\s*[^,]+,\s*[^,]+,\s*({[^}]+})"
                    match = re.search(pattern, script.string, re.DOTALL)
                    if match:
                        try:
                            data = json.loads(match.group(1))
                            if "total_count" in data:
                                return int(data["total_count"])
                        except:
                            pass

            # method 2: "All comments" link with count
            all_comments_link = soup.find("a", class_="commentthread_allcommentslink")
            if all_comments_link:
                text = all_comments_link.get_text(strip=True)
                match = re.search(r"\((\d+)\)", text)
                if match:
                    return int(match.group(1))

            # method 3: element containing totalcount
            totalcount_elem = soup.find(id=lambda x: x and "totalcount" in x)
            if totalcount_elem:
                try:
                    return int(totalcount_elem.text.strip())
                except:
                    pass

            return 0

        except Exception as e:
            print(f"error getting comment count: {e}")
            return 0

    def _get_all_comments_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """construct URL for all comments page"""
        try:
            # method 1: find "All comments" link
            all_comments_link = soup.find("a", class_="commentthread_allcommentslink")
            if all_comments_link and all_comments_link.get("href"):
                href = all_comments_link.get("href")
                return self._normalize_url(href, base_url)

            # method 2: extract from profile data
            steamid_match = re.search(r"g_rgProfileData\s*=\s*({[^}]+})", soup.text)
            if steamid_match:
                try:
                    profile_data = json.loads(steamid_match.group(1))
                    steamid = profile_data.get("steamid")
                    if steamid:
                        return f"https://steamcommunity.com/profiles/{steamid}/allcomments"
                except:
                    pass

            # method 3: construct from URL pattern
            if "/profiles/" in base_url:
                match = re.search(r"/profiles/(\d+)", base_url)
                if match:
                    return f"https://steamcommunity.com/profiles/{match.group(1)}/allcomments"
            elif "/id/" in base_url:
                match = re.search(r"/id/([^/]+)", base_url)
                if match:
                    return f"https://steamcommunity.com/id/{match.group(1)}/allcomments"

            return None

        except Exception as e:
            print(f"error getting comments URL: {e}")
            return None

    def _normalize_url(self, url: str, base_url: str) -> str:
        """normalize relative URLs to absolute"""
        if url.startswith("/"):
            base_domain = f"https://{urlparse(base_url).netloc}"
            return urljoin(base_domain, url)
        elif not url.startswith("http"):
            return urljoin(base_url, url)
        return url

    def _parse_all_pages_optimized(self, total_pages: int):
        """parse all pages with duplicate detection"""
        print(f"\nstarting to parse {total_pages} pages...")
        no_new_comments_pages = 0

        # start from page 2 (page 1 already parsed)
        for page_num in range(2, total_pages + 1):
            try:
                page_url = self._get_page_url(page_num)

                # rate limiting
                time.sleep(0.5)

                print(f"page {page_num}/{total_pages}...", end="\r")

                response = self.session.get(page_url, timeout=30)
                response.raise_for_status()

                # parse comments
                comments_count = self._parse_comments_from_html(response.text)

                # stop if no new comments for 2 consecutive pages
                if comments_count == 0:
                    no_new_comments_pages += 1
                    if no_new_comments_pages >= 2:
                        print(f"\nstopping: {no_new_comments_pages} pages without new comments")
                        break
                else:
                    no_new_comments_pages = 0

                time.sleep(0.2)

            except requests.RequestException as e:
                print(f"\nerror loading page {page_num}: {e}")
                continue
            except Exception as e:
                print(f"\nerror parsing page {page_num}: {e}")
                continue

        print("\n" + " " * 50)

    def _get_page_url(self, page_num: int) -> str:
        """generate URL for specific page number"""
        if not self.base_comments_url:
            return ""

        parsed_url = urlparse(self.base_comments_url)
        query_params = parse_qs(parsed_url.query)

        # return original URL for page 1 if no 'p' parameter
        if page_num == 1 and "p" not in query_params:
            return self.base_comments_url

        # add/update page parameter
        query_params["p"] = [str(page_num)]

        # reconstruct URL
        new_query = urlencode(query_params, doseq=True)
        new_url = parsed_url._replace(query=new_query).geturl()

        return new_url

    def _parse_comments_from_html(self, html_content: str) -> int:
        """parse comments from HTML with uniqueness check"""
        soup = BeautifulSoup(html_content, "html.parser")

        # find comment blocks
        comment_blocks = soup.find_all("div", class_="commentthread_comment")

        # fallback for alternative class names
        if not comment_blocks:
            comment_blocks = soup.find_all(
                "div", class_=lambda x: x and "comment" in x.lower()
            )

        comments_count = 0

        for comment in comment_blocks:
            try:
                comment_data = self._parse_single_comment(comment)
                if comment_data:
                    # skip empty comments
                    if not comment_data.get("comment_text") or not comment_data["comment_text"].strip():
                        continue

                    # skip system messages
                    if "Это сообщение ещё не проанализировано нашей системой" in comment_data["comment_text"]:
                        continue

                    comment_id = comment_data["comment_id"]

                    # skip already seen comments
                    if comment_id in self.seen_comment_ids:
                        continue

                    # mark as seen
                    self.seen_comment_ids.add(comment_id)

                    user = comment_data["user"]
                    self.comments_data[user]["count"] += 1
                    self.comments_data[user]["comments"].append(comment_data)
                    self.total_comments += 1
                    comments_count += 1

                    # progress indicator
                    if self.total_comments % 50 == 0:
                        print(f"comments parsed: {self.total_comments}", end="\r")

            except Exception:
                continue

        return comments_count

    def _parse_single_comment(self, comment_element) -> Optional[Dict[str, Any]]:
        """parse single comment element"""
        try:
            # username
            user_link = comment_element.find("a", class_="commentthread_author_link")
            if not user_link:
                user_link = comment_element.find(
                    "a", href=lambda x: x and ("/profiles/" in x or "/id/" in x)
                )
                if not user_link:
                    return None

            user_name = user_link.get_text(strip=True)

            # profile URL
            user_profile_link = user_link.get("href", "")

            # steam ID extraction
            steam_id = None
            if user_profile_link:
                id_match = re.search(r"/profiles/(\d+)", user_profile_link)
                if id_match:
                    steam_id = id_match.group(1)
                else:
                    username_match = re.search(r"/id/([^/]+)", user_profile_link)
                    if username_match:
                        steam_id = username_match.group(1)

            # comment text
            comment_text_div = comment_element.find(
                "div", class_="commentthread_comment_text"
            )
            if not comment_text_div:
                comment_text_div = comment_element.find(
                    "div", class_=lambda x: x and "text" in x.lower()
                )

            comment_text = comment_text_div.get_text(strip=True) if comment_text_div else ""

            # timestamp
            timestamp_span = comment_element.find(
                "span", class_="commentthread_comment_timestamp"
            )
            if not timestamp_span:
                timestamp_span = comment_element.find(
                    "span", class_=lambda x: x and "timestamp" in x.lower()
                )

            timestamp = timestamp_span.get("title", "") if timestamp_span else ""
            if not timestamp and timestamp_span:
                timestamp = timestamp_span.get_text(strip=True)

            # avatar
            avatar_img = comment_element.find(
                "img", src=re.compile(r"avatars\.fastly\.steamstatic\.com")
            )
            if not avatar_img:
                avatar_img = comment_element.find(
                    "img", src=re.compile(r"steamstatic\.com")
                )

            avatar_url = avatar_img.get("src", "") if avatar_img else ""

            # online status
            avatar_div = comment_element.find(
                "div", class_=re.compile(r"commentthread_comment_avatar|playerAvatar")
            )
            status = "unknown"
            if avatar_div:
                classes = avatar_div.get("class", [])
                classes_str = " ".join(classes)
                if "online" in classes_str:
                    status = "online"
                elif "offline" in classes_str:
                    status = "offline"
                elif "in-game" in classes_str:
                    status = "in-game"

            # comment ID
            comment_id = comment_element.get("id", "")
            if comment_id.startswith("comment_"):
                comment_id = comment_id.replace("comment_", "")

            # generate ID if missing
            if not comment_id:
                comment_id = f"{user_name}_{timestamp}_{hash(comment_text) % 1000000}"

            # parse time
            parsed_time = datetime.now().isoformat()

            return {
                "user": user_name,
                "steam_id": steam_id,
                "profile_url": user_profile_link,
                "comment_text": comment_text,
                "timestamp": timestamp,
                "avatar_url": avatar_url,
                "status": status,
                "comment_id": comment_id,
                "parsed_time": parsed_time,
            }

        except Exception as e:
            print(f"comment parsing error: {e}")
            return None

    def save_to_json(self, filename: str = "steam_comments.json") -> Dict[str, Any]:
        """save results to JSON file"""
        result = {
            "profile_url": self.profile_url,
            "total_users": len(self.comments_data),
            "total_comments": self.total_comments,
            "parse_date": datetime.now().isoformat(),
            "data": dict(self.comments_data),
        }

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"\nsaved to JSON file: {filename}")
        return result

    def save_to_csv(self, filename: str = "steam_comments.csv"):
        """save results to CSV file"""
        try:
            import csv

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                # headers
                writer.writerow([
                    "user",
                    "steam_id",
                    "profile_url",
                    "comment_text",
                    "timestamp",
                    "avatar_url",
                    "status",
                    "comment_id",
                    "parsed_time",
                ])

                # data rows
                for user_data in self.comments_data.values():
                    for comment in user_data["comments"]:
                        writer.writerow([
                            comment.get("user", ""),
                            comment.get("steam_id", ""),
                            comment.get("profile_url", ""),
                            comment.get("comment_text", ""),
                            comment.get("timestamp", ""),
                            comment.get("avatar_url", ""),
                            comment.get("status", ""),
                            comment.get("comment_id", ""),
                            comment.get("parsed_time", ""),
                        ])

            print(f"saved to CSV file: {filename}")

        except ImportError:
            print("CSV module required for CSV export")
        except Exception as e:
            print(f"CSV save error: {e}")

    def print_summary(self, top_n: int = 20):
        """print parsing statistics"""
        print("\n" + "=" * 60)
        print("PARSING STATISTICS")
        print("=" * 60)
        print(f"profile: {self.profile_url}")
        print(f"total comments: {self.total_comments}")
        print(f"total users: {len(self.comments_data)}")

        if self.comments_data:
            print(f"\ntop-{top_n} users by comment count:")
            print("-" * 60)

            sorted_users = sorted(
                self.comments_data.items(),
                key=lambda x: x[1]["count"],
                reverse=True
            )[:top_n]

            for i, (user, data) in enumerate(sorted_users, 1):
                print(f"{i:3}. {user[:40]:40} - {data['count']:5} comments")
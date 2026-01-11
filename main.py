import os
import json
import re
import time
from src.parser import SteamProfileCommentParser


def main():
    """Main program function"""
    try:
        # get profile URL
        print("\n" + "=" * 60)
        print("STEAM PROFILE COMMENT PARSER")
        print("=" * 60)
        print("\nSupported URL formats:")
        print("1. https://steamcommunity.com/id/username/")
        print("2. https://steamcommunity.com/profiles/7656119xxxxxxxxxx/")

        profile_url = input("\nEnter Steam profile URL: ").strip()

        if not profile_url:
            print("❌ URL cannot be empty!")
            return

        # add https:// if missing
        if not profile_url.startswith(("http://", "https://")):
            profile_url = "https://" + profile_url

        # validate Steam profile URL format
        if not re.match(
            r"https?://steamcommunity\.com/(id|profiles)/[^/]+/?", profile_url
        ):
            print("❌ Invalid URL format! Use one of the formats above.")
            return

        # get page count
        max_pages_input = input("\nMax pages to parse (Enter for 200): ").strip()
        max_pages = 200 if not max_pages_input else int(max_pages_input)

        # create parser instance
        parser = SteamProfileCommentParser()

        # start parsing
        start_time = time.time()
        print("\n" + "=" * 60)
        print("STARTING PARSING")
        print("=" * 60)

        comments_data = parser.parse_profile(profile_url, max_pages=max_pages)

        elapsed_time = time.time() - start_time

        if not comments_data:
            print("\n❌ Failed to get comments. Check URL and try again.")
            return

        # save results
        os.makedirs("data", exist_ok=True)  # create folder if doesn't exist

        base_filename = "steam_profile_comments"
        json_file = f"data/{base_filename}.json"
        csv_file = f"data/{base_filename}.csv"

        parser.save_to_json(json_file)
        parser.save_to_csv(csv_file)

        # display statistics
        parser.print_summary(top_n=25)

        # final summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Execution time: {elapsed_time:.1f} seconds")
        if elapsed_time > 0:
            print(
                f"Parsing speed: {parser.total_comments / elapsed_time:.1f} comments/sec"
            )

        # ask to show sample data
        show_example = input("\nShow sample data? (y/n): ").lower().strip()
        if show_example == "y":
            print("\nSample data structure (first 3 commenters):")
            sample_data = {}
            count = 0
            for user, data in parser.comments_data.items():
                if data["comments"]:
                    sample_data[user] = {
                        "count": data["count"],
                        "comments": data["comments"][:1],
                    }
                    count += 1
                    if count >= 3:
                        break

            print(json.dumps(sample_data, ensure_ascii=False, indent=2))

        print("\n✅ Parsing completed successfully!")

    except KeyboardInterrupt:
        print("\n\n❌ Parsing interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()

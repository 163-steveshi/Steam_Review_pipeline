import requests

import json


def requestReview(gameId: int = 3180070):

    res = requests.get(
        f"https://store.steampowered.com/appreviews/{gameId}?json=1&filter=all&language=all&purchase_type=all&num_per_page=100&cursor=*"
    )
    print(res.json())
    result = res.json()
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)


def main():
    requestReview()


if __name__ == "__main__":
    main()

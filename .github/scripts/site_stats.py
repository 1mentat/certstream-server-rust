import json
import os
import re
import urllib.request

REPO = "reloading01/certstream-server-rust"
PACKAGE_PAGE = f"https://github.com/{REPO}/pkgs/container/certstream-server-rust"


def get(url, api=False):
    headers = {"User-Agent": "site-stats"}
    token = os.environ.get("GITHUB_TOKEN")
    if api and token:
        headers["Authorization"] = f"Bearer {token}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as r:
        return r.read().decode()


repo = json.loads(get(f"https://api.github.com/repos/{REPO}", api=True))
page = get(PACKAGE_PAGE)

match = re.search(r'Total downloads</span>\s*<h3 title="(\d+)"', page)
if not match:
    raise SystemExit("container pull count not found on the package page")

stats = {
    "stars": repo["stargazers_count"],
    "container_pulls": int(match.group(1)),
}

with open("docs/stats.json", "w") as f:
    json.dump(stats, f, indent=2)
    f.write("\n")
print(stats)

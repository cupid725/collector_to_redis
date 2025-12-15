import json, re

path = "region_profiles.json"
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

TARGET_MAJOR = "143"

for region, prof in data.items():
    uas = prof.get("user_agents", [])
    new_uas = []
    for ua in uas:
        ua = re.sub(r"Chrome/\d+\.\d+\.\d+\.\d+", f"Chrome/{TARGET_MAJOR}.0.0.0", ua)
        ua = re.sub(r"Chrome/\d+", f"Chrome/{TARGET_MAJOR}", ua)  # 혹시 단축형 대비
        new_uas.append(ua)
    prof["user_agents"] = new_uas

with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("done")

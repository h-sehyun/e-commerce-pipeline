"""
Faker 고정 유저 생성기
- 1만 명의 유저 프로필을 생성해 data/users.json 에 저장 (1회 실행 고정)
- Producer가 이 파일을 로드해 OTTO 이벤트에 user_id를 랜덤 배정
"""

import json
import random
import uuid
from pathlib import Path

from faker import Faker

# ── 설정 ──────────────────────────────────────────────
NUM_USERS  = 50_000
USERS_PATH = Path("data/users.json")
SEED       = 42          # 재현 가능하도록 고정 시드

fake = Faker("ko_KR")
Faker.seed(SEED)
random.seed(SEED)

# ── 한국 지역 목록 ─────────────────────────────────────
REGIONS = [
    "서울", "부산", "인천", "대구", "대전", "광주", "울산", "세종",
    "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
]

REGION_WEIGHTS = [
    30, 7, 6, 5, 4, 4, 3, 1,
    22, 2, 2, 2, 2, 2, 3, 3, 2,
]

AGE_GROUPS = ["10대", "20대", "30대", "40대", "50대", "60대 이상"]
AGE_WEIGHTS = [5, 25, 30, 20, 12, 8]

GENDERS = ["M", "F"]

DEVICE_PREFS = ["web", "ios", "android"]
DEVICE_WEIGHTS = [40, 35, 25]

MEMBERSHIP = ["일반", "실버", "골드", "플래티넘"]
MEMBERSHIP_WEIGHTS = [60, 25, 12, 3]


def generate_users(n: int = NUM_USERS) -> list[dict]:
    users = []
    for i in range(n):
        gender     = random.choice(GENDERS)
        age_group  = random.choices(AGE_GROUPS, weights=AGE_WEIGHTS, k=1)[0]
        region     = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]

        if gender == "M":
            name = fake.name_male()
        else:
            name = fake.name_female()

        user = {
            "user_id":          str(uuid.UUID(int=random.getrandbits(128), version=4)),
            "name":             name,
            "gender":           gender,
            "age_group":        age_group,
            "region":           region,
            "city":             fake.city(),
            "email":            fake.email(),
            "phone":            fake.phone_number(),
            "membership":       random.choices(MEMBERSHIP, weights=MEMBERSHIP_WEIGHTS, k=1)[0],
            "preferred_device": random.choices(DEVICE_PREFS, weights=DEVICE_WEIGHTS, k=1)[0],
            "signup_date":      fake.date_between(start_date="-5y", end_date="today").isoformat(),
        }
        users.append(user)

        if (i + 1) % 1000 == 0:
            print(f"  생성 중... {i + 1:,}/{n:,}")

    return users


def save_users(users: list[dict]):
    USERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(USERS_PATH, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)
    print(f"저장 완료: {USERS_PATH}  ({len(users):,}명)")


def print_stats(users: list[dict]):
    from collections import Counter

    genders    = Counter(u["gender"] for u in users)
    ages       = Counter(u["age_group"] for u in users)
    regions    = Counter(u["region"] for u in users)
    membership = Counter(u["membership"] for u in users)

    print("\n=== 유저 분포 통계 ===")
    print(f"  총 유저 수 : {len(users):,}명")
    print(f"\n  성별:")
    for k, v in sorted(genders.items()):
        print(f"    {k}: {v:,}명 ({v/len(users)*100:.1f}%)")
    print(f"\n  연령대:")
    for k in AGE_GROUPS:
        v = ages[k]
        print(f"    {k}: {v:,}명 ({v/len(users)*100:.1f}%)")
    print(f"\n  상위 5개 지역:")
    for k, v in regions.most_common(5):
        print(f"    {k}: {v:,}명")
    print(f"\n  멤버십:")
    for k, v in membership.most_common():
        print(f"    {k}: {v:,}명 ({v/len(users)*100:.1f}%)")


if __name__ == "__main__":
    if USERS_PATH.exists():
        print(f"{USERS_PATH} 이미 존재합니다.")
        ans = input("다시 생성하시겠습니까? 기존 데이터가 삭제됩니다 (y/N): ").strip().lower()
        if ans != "y":
            print("취소합니다.")
            # 기존 데이터 통계만 출력
            with open(USERS_PATH, encoding="utf-8") as f:
                users = json.load(f)
            print_stats(users)
            exit(0)

    print(f"Faker 고정 유저 {NUM_USERS:,}명 생성 중...")
    users = generate_users(NUM_USERS)
    save_users(users)
    print_stats(users)
    print(f"\n샘플 유저:")
    for u in users[:2]:
        print(f"  {u}")
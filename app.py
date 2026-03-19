import os
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st

APP_TITLE = "MISHARP CRM OS"
BASE_DIR = Path(__file__).resolve().parent
SNAPSHOT_DIR = BASE_DIR / "snapshots"
SNAPSHOT_DIR.mkdir(exist_ok=True)

st.set_page_config(page_title=APP_TITLE, page_icon="📈", layout="wide")

EXPECTED_COLS = [
    '이름', '아이디', '회원등급', '총구매금액', '실결제금액', '총 실주문건수', '누적주문건수',
    '총 방문횟수(1년 내)', '회원구분', '최종접속일', '최종주문일', 'SMS 수신여부',
    'e메일 수신여부', '총 사용 적립금', '총예치금', '총적립금', '미가용 적립금',
    '사용가능 적립금', '휴대폰번호', '이메일', '회원 가입일', '회원 가입경로', '특별회원',
    '평생회원', '휴면처리일', '탈퇴구분', '탈퇴여부', '탈퇴일', '주소1', '주소2',
    '생년월일', '결혼기념일', '결혼여부', '관심분야', '나이', '답변', '모바일앱 이용여부',
    '불량회원', '성별', '양력(T)/음력(F)', '접속 IP', '직업', '직종', '최종학력'
]

BOOL_COLS = ['SMS 수신여부', 'e메일 수신여부', '모바일앱 이용여부', '특별회원', '평생회원', '탈퇴여부']
NUMERIC_COLS = [
    '총구매금액', '실결제금액', '총 실주문건수', '누적주문건수', '총 방문횟수(1년 내)',
    '총 사용 적립금', '총예치금', '총적립금', '미가용 적립금', '사용가능 적립금', '나이'
]
DATE_COLS = ['최종접속일', '최종주문일', '회원 가입일', '휴면처리일', '탈퇴일', '생년월일', '결혼기념일']

SEGMENT_ORDER = [
    "신규 가입 후 미구매",
    "첫 구매 후 재구매 대기",
    "최근 방문·미구매",
    "활성 재구매 고객",
    "VIP/고액 활성 고객",
    "고액 이탈 위험",
    "장기 휴면/이탈",
    "비수신 분석 대상",
]

ACTION_TEXT = {
    "신규 가입 후 미구매": "웰컴 메시지 + 실패 적은 입문 상품 3종 추천",
    "첫 구매 후 재구매 대기": "첫 구매 연관 코디 추천 + 재구매 유도",
    "최근 방문·미구매": "최근 본 카테고리/베스트 상품 중심 리마인드",
    "활성 재구매 고객": "신상품/베스트 재구매 캠페인",
    "VIP/고액 활성 고객": "프라이빗 추천 + 조기 공개/우선 혜택",
    "고액 이탈 위험": "대표 추천 메시지 + 복귀 혜택 검토",
    "장기 휴면/이탈": "휴면 복귀 캠페인, 단 지나친 할인 남발 금지",
    "비수신 분석 대상": "비수신 고객은 분석 중심 관리, 사이트 내 개인화/앱 유도",
}

STRATEGY_LIBRARY = {
    "신규 가입 후 미구매": {
        "event": "웰컴 입문 추천 캠페인",
        "objective": "첫 구매 전환",
        "media": ["SMS", "앱푸시", "이메일"],
        "timing": "가입 후 3~7일, 화/수 오전 10~11시",
        "copy": [
            "미샵이 처음이시라면 실패 적은 대표 상품부터 가볍게 만나보세요",
            "4050 고객 만족도가 높은 입문 상품만 골라 추천드립니다",
            "첫 구매 고민 덜어드릴 대표 코디를 지금 확인해보세요",
        ],
        "product": "베스트 팬츠 / 체형 커버 상의 / 기본 코디 세트",
        "note": "강한 할인보다 입문용 대표상품 제안이 더 적합합니다.",
    },
    "첫 구매 후 재구매 대기": {
        "event": "첫 구매 고객 재구매 트리거",
        "objective": "두 번째 구매 유도",
        "media": ["SMS", "앱푸시", "이메일"],
        "timing": "첫 구매 후 14~30일, 화/수 오전 10~11시",
        "copy": [
            "지난번 만족하셨다면 이 상품도 잘 맞으실 거예요. 실패 없는 코디로 추천드립니다",
            "한 번 입어보셔서 아시죠. 이번엔 더 만족하실 추천 상품을 모아봤어요",
            "지금 가장 많이 나가는 연관 코디를 확인해보세요",
        ],
        "product": "첫 구매 상품과 연결되는 카테고리 / 코디 세트 / 재구매율 높은 베스트",
        "note": "과도한 할인보다 추천형 메시지가 더 안전합니다.",
    },
    "최근 방문·미구매": {
        "event": "방문 리마인드 캠페인",
        "objective": "구매 직전 고객 전환",
        "media": ["SMS", "앱푸시"],
        "timing": "최근 접속 후 1~3일 내, 평일 오후 2~5시",
        "copy": [
            "눈여겨보신 상품과 잘 어울리는 베스트 코디를 준비했습니다",
            "지금 많이 찾는 체형 커버 상품만 다시 모아드렸어요",
            "코디 고민 줄여주는 대표 아이템을 확인해보세요",
        ],
        "product": "베스트셀러 / 카테고리 대표상품 / 즉시 구매 가능 조합",
        "note": "리마인드 톤으로 가볍게, 반복 발송은 줄이는 편이 좋습니다.",
    },
    "활성 재구매 고객": {
        "event": "재구매 강화 캠페인",
        "objective": "주문 빈도 유지 및 객단가 상승",
        "media": ["SMS", "앱푸시", "이메일"],
        "timing": "최근 주문 후 20~45일, 화/수 오전 10~11시",
        "copy": [
            "이번 주 가장 반응 좋은 신상과 베스트를 함께 추천드립니다",
            "늘 선택해주시는 고객님께 잘 맞을 코디 조합을 준비했어요",
            "지금 시기에 꼭 필요한 대표 아이템을 확인해보세요",
        ],
        "product": "신상 + 베스트 믹스 / 세트 추천 / 시즌 전환 상품",
        "note": "구매 경험이 있는 고객이므로 신상 소개와 코디 제안을 함께 쓰는 게 좋습니다.",
    },
    "VIP/고액 활성 고객": {
        "event": "프라이빗 VIP 케어",
        "objective": "관계 강화 및 충성도 유지",
        "media": ["SMS", "앱푸시", "이메일"],
        "timing": "신상 오픈 전 / 주요 프로모션 1~2일 전",
        "copy": [
            "항상 믿고 찾아주셔서 감사합니다. 먼저 보시면 만족하실 상품만 골랐습니다",
            "VIP 고객님께 우선 추천드리는 이번 주 대표 상품을 확인해보세요",
            "미샵이 자신 있게 추천드리는 프라이빗 코디 제안입니다",
        ],
        "product": "신상 우선 공개 / 고급 라인 / 세트 제안",
        "note": "혜택보다 관계형 톤이 중요합니다.",
    },
    "고액 이탈 위험": {
        "event": "우수고객 복귀 캠페인",
        "objective": "고액 고객 복귀",
        "media": ["SMS", "앱푸시"],
        "timing": "최근 주문 종료 후 60~120일, 화/수 오전 10~11시",
        "copy": [
            "오랜만에 다시 보셔도 만족도 높은 대표 상품만 모았습니다",
            "예전에 좋아해주셨던 무드로 다시 고른 베스트 상품을 소개드립니다",
            "지금 미샵에서 가장 반응 좋은 아이템을 다시 확인해보세요",
        ],
        "product": "과거 반응 좋았던 카테고리 / 실패 적은 베스트 / 시즌 대표 상품",
        "note": "강한 세일보다는 복귀 명분을 주는 메시지가 좋습니다.",
    },
    "장기 휴면/이탈": {
        "event": "휴면 복귀 캠페인",
        "objective": "재방문 및 복귀",
        "media": ["SMS", "앱푸시"],
        "timing": "월 1회 이하 테스트 발송, 화/수 오전 10~11시",
        "copy": [
            "오랜만에 다시 보셔도 실패 없을 대표 상품만 골랐습니다",
            "지금 다시 시작하기 좋은 미샵 베스트 아이템을 확인해보세요",
            "쌓아둔 적립금이나 최근 인기 상품으로 가볍게 다시 만나보세요",
        ],
        "product": "입문 베스트 / 고정 판매 팬츠 / 체형 커버 상의 / 적립금 활용 상품",
        "note": "전체 일괄 발송보다 복귀 가능성 높은 고객부터 우선 실행하세요.",
    },
    "비수신 분석 대상": {
        "event": "비수신 분석 관리",
        "objective": "발송 대상 제외 후 분석 활용",
        "media": ["분석만"],
        "timing": "정기 리포트 확인",
        "copy": [
            "비수신 고객군은 발송 대신 사이트 내 개인화 전략 검토가 좋습니다",
            "다음 수신 동의 확보 캠페인 대상 여부를 분석하세요",
            "앱 유도, 배너 노출, 개인화 추천 전략을 검토하세요",
        ],
        "product": "사이트 개인화 / 앱 설치 유도 / 베스트 진입 동선",
        "note": "외부 발송보다 내 사이트 경험 개선이 우선입니다.",
    },
    "휴면복귀_고액": {
        "event": "고액 휴면 복귀",
        "objective": "구매력 높은 장기 휴면 재활성화",
        "media": ["SMS", "앱푸시"],
        "timing": "월 1회 이하, 화/수 오전 10~11시",
        "copy": [
            "예전처럼 다시 만족하실 대표 상품을 신중히 골랐습니다",
            "오랜만에 보셔도 미샵다운 상품만 먼저 추천드립니다",
            "다시 시작하기 좋은 베스트 코디를 확인해보세요",
        ],
        "product": "과거 주력 카테고리 추정 베스트 / 고정 판매 상품 / 세트 제안",
        "note": "반응 없는 고객에게 연속 발송하지 마세요.",
    },
    "휴면복귀_적립금": {
        "event": "적립금 기반 휴면 복귀",
        "objective": "적립금 사용을 계기로 재방문 유도",
        "media": ["SMS", "앱푸시"],
        "timing": "적립금 사용률이 높은 주간에 테스트",
        "copy": [
            "쌓아둔 적립금으로 지금 가볍게 시작하기 좋은 상품을 모았습니다",
            "적립금으로 부담 없이 다시 만나보실 대표 상품을 확인해보세요",
            "지금 사용하기 좋은 베스트 아이템을 추천드립니다",
        ],
        "product": "가격 진입 장벽 낮은 베스트 / 적립금 사용 쉬운 상품",
        "note": "쿠폰 남발보다 적립금 활용 메시지가 자연스럽습니다.",
    },
}


def bool_map(v):
    if pd.isna(v):
        return False
    s = str(v).strip().upper()
    return s in {"T", "TRUE", "Y", "YES", "1", "사용", "수신", "동의"}


def won(x):
    try:
        return f"{int(round(float(x))):,}원"
    except Exception:
        return "-"


def load_uploaded(file):
    name = file.name.lower()
    if name.endswith(".csv"):
        for enc in ["utf-8-sig", "utf-8", "cp949", "euc-kr"]:
            try:
                return pd.read_csv(file, encoding=enc, low_memory=False)
            except Exception:
                file.seek(0)
        raise ValueError("CSV 파일 인코딩을 읽지 못했습니다.")
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(file)
    raise ValueError("CSV 또는 XLSX 파일만 업로드할 수 있습니다.")


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in EXPECTED_COLS:
        if col not in out.columns:
            out[col] = np.nan

    for col in NUMERIC_COLS:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    for col in DATE_COLS:
        out[col] = pd.to_datetime(out[col], errors="coerce")

    for col in BOOL_COLS:
        out[col] = out[col].map(bool_map)

    if "불량회원" in out.columns:
        out["불량회원_raw"] = out["불량회원"].astype(str).fillna("")
        out["불량회원"] = out["불량회원_raw"].str.contains("불량|Y|T|TRUE|예|1", case=False, na=False)
    else:
        out["불량회원"] = False

    today = pd.Timestamp(datetime.now().date())
    out["회원키"] = out["아이디"].astype(str).str.strip()
    out["회원키"] = np.where(out["회원키"].isin(["", "nan", "None"]), out["휴대폰번호"].astype(str), out["회원키"])

    out["가입후경과일"] = (today - out["회원 가입일"]).dt.days
    out["마지막주문경과일"] = (today - out["최종주문일"]).dt.days
    out["마지막접속경과일"] = (today - out["최종접속일"]).dt.days

    out["구매회원"] = out["총 실주문건수"] > 0
    out["미구매회원"] = out["총 실주문건수"] <= 0
    out["최근30일접속"] = out["마지막접속경과일"].between(0, 30, inclusive="both")
    out["최근60일주문"] = out["마지막주문경과일"].between(0, 60, inclusive="both")
    out["최근90일주문없음"] = out["마지막주문경과일"] > 90
    out["최근180일주문없음"] = out["마지막주문경과일"] > 180
    out["최근365일주문없음"] = out["마지막주문경과일"] > 365
    out["최근3년접속없음"] = out["마지막접속경과일"] > 1095
    out["고액고객"] = out["총구매금액"] >= 300000
    out["초우수고객"] = out["총구매금액"] >= 500000
    out["방문많음"] = out["총 방문횟수(1년 내)"] >= 10
    out["적립금보유"] = out["사용가능 적립금"] > 0
    out["발송가능_SMS"] = out["SMS 수신여부"] & (~out["탈퇴여부"])
    out["발송가능_이메일"] = out["e메일 수신여부"] & (~out["탈퇴여부"])
    out["발송가능_앱"] = out["모바일앱 이용여부"] & (~out["탈퇴여부"])

    out["권장채널"] = np.select(
        [
            out["발송가능_SMS"],
            out["발송가능_앱"],
            out["발송가능_이메일"],
        ],
        ["SMS", "앱푸시", "이메일"],
        default="분석만"
    )

    # 휴면 복귀 점수
    dormant_score = (
        np.where(out["총구매금액"] >= 500000, 3, np.where(out["총구매금액"] >= 300000, 2, np.where(out["총구매금액"] > 0, 1, 0)))
        + np.where(out["총 실주문건수"] >= 5, 2, np.where(out["총 실주문건수"] >= 2, 1, 0))
        + np.where(out["발송가능_SMS"], 1, 0)
        + np.where(out["발송가능_앱"], 1, 0)
        + np.where(out["적립금보유"], 1, 0)
        - np.where(out["최근3년접속없음"], 2, 0)
        - np.where(out["최근365일주문없음"], 1, 0)
    )
    out["휴면복귀점수"] = dormant_score
    out["휴면복귀등급"] = pd.cut(
        out["휴면복귀점수"], bins=[-999, 3, 7, 999], labels=["낮음", "중간", "높음"]
    ).astype(str)

    out["휴면세부유형"] = np.select(
        [
            out["최근3년접속없음"] & out["고액고객"],
            out["최근3년접속없음"] & out["적립금보유"],
            out["최근3년접속없음"] & (out["총 실주문건수"] >= 1),
            out["최근3년접속없음"] & out["미구매회원"],
        ],
        [
            "휴면복귀_고액",
            "휴면복귀_적립금",
            "장기 휴면/이탈",
            "장기 휴면/이탈",
        ],
        default=""
    )

    out["세그먼트"] = build_segments(out)
    out["추천액션"] = out["세그먼트"].map(ACTION_TEXT).fillna("세그먼트 기준 액션 수동 검토")
    return out


def build_segments(df: pd.DataFrame) -> pd.Series:
    conds = [
        (df["미구매회원"]) & (df["가입후경과일"].fillna(9999) <= 30),
        (df["총 실주문건수"] == 1) & (df["마지막주문경과일"].fillna(9999) > 30),
        (df["미구매회원"]) & (df["최근30일접속"]),
        (df["총 실주문건수"] >= 2) & (df["최근60일주문"]) & (~df["고액고객"]),
        ((df["회원등급"].astype(str).str.contains("VIP|VVIP|골드|실버", na=False)) | df["고액고객"]) & (df["최근60일주문"]),
        (df["고액고객"]) & (df["최근90일주문없음"]),
        (df["최근180일주문없음"]) | (df["휴면처리일"].notna()) | (df["최근3년접속없음"]),
        ~(df["발송가능_SMS"] | df["발송가능_앱"] | df["발송가능_이메일"]),
    ]
    choices = [
        "신규 가입 후 미구매",
        "첫 구매 후 재구매 대기",
        "최근 방문·미구매",
        "활성 재구매 고객",
        "VIP/고액 활성 고객",
        "고액 이탈 위험",
        "장기 휴면/이탈",
        "비수신 분석 대상",
    ]
    return pd.Series(np.select(conds, choices, default="최근 방문·미구매"), index=df.index)


def calc_rfm(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    recency = out["마지막주문경과일"].fillna(9999)
    freq = out["총 실주문건수"].fillna(0)
    monetary = out["총구매금액"].fillna(0)

    out["R점수"] = pd.cut(
        recency, bins=[-1, 30, 60, 90, 180, np.inf], labels=[5, 4, 3, 2, 1]
    ).astype("Int64").fillna(1)
    out["F점수"] = pd.cut(
        freq, bins=[-1, 0, 1, 2, 4, np.inf], labels=[1, 2, 3, 4, 5]
    ).astype("Int64").fillna(1)
    out["M점수"] = pd.cut(
        monetary, bins=[-1, 0, 100000, 300000, 500000, np.inf], labels=[1, 2, 3, 4, 5]
    ).astype("Int64").fillna(1)
    out["RFM"] = out["R점수"].astype(str) + out["F점수"].astype(str) + out["M점수"].astype(str)
    return out


def summary_cards(df: pd.DataFrame) -> dict:
    active_buyers = df[(df["총 실주문건수"] > 0) & (df["마지막주문경과일"] <= 90)]
    return {
        "전체 고객수": len(df),
        "발송 가능 고객수": int((df["발송가능_SMS"] | df["발송가능_앱"] | df["발송가능_이메일"]).sum()),
        "불량회원": int(df["불량회원"].sum()),
        "첫 구매 후 재구매 대기": int((df["세그먼트"] == "첫 구매 후 재구매 대기").sum()),
        "고액 이탈 위험": int((df["세그먼트"] == "고액 이탈 위험").sum()),
        "3년 이상 미접속": int(df["최근3년접속없음"].sum()),
        "휴면복귀 고득점": int((df["휴면복귀등급"] == "높음").sum()),
        "최근 90일 활성 구매고객": len(active_buyers),
    }


def recommended_actions(df: pd.DataFrame) -> pd.DataFrame:
    base = df[~df["불량회원"]].copy()
    rows = []
    for seg in SEGMENT_ORDER:
        sub = base[base["세그먼트"] == seg]
        if len(sub) == 0:
            continue
        actionable = sub[sub["권장채널"] != "분석만"]
        rows.append({
            "우선순위 세그먼트": seg,
            "대상수": len(sub),
            "즉시 실행 가능수": len(actionable),
            "권장채널": actionable["권장채널"].mode().iloc[0] if len(actionable) else "분석만",
            "추천액션": ACTION_TEXT.get(seg, ""),
        })

    dorm = base[(base["최근3년접속없음"]) & (base["휴면복귀등급"] == "높음")]
    if len(dorm):
        actionable = dorm[dorm["권장채널"] != "분석만"]
        rows.append({
            "우선순위 세그먼트": "휴면복귀_고액",
            "대상수": len(dorm),
            "즉시 실행 가능수": len(actionable),
            "권장채널": actionable["권장채널"].mode().iloc[0] if len(actionable) else "분석만",
            "추천액션": "복귀 가능성 높은 장기휴면 우선 공략",
        })
    rec = pd.DataFrame(rows)
    if not rec.empty:
        rec = rec.sort_values(["즉시 실행 가능수", "대상수"], ascending=False)
    return rec


def save_snapshot(df: pd.DataFrame, snapshot_name: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in snapshot_name).strip("_")
    folder = SNAPSHOT_DIR / f"{ts}_{safe}"
    folder.mkdir(parents=True, exist_ok=True)
    df.to_parquet(folder / "data.parquet", index=False)
    meta = pd.DataFrame([{
        "snapshot_name": snapshot_name,
        "created_at": ts,
        "rows": len(df)
    }])
    meta.to_csv(folder / "meta.csv", index=False, encoding="utf-8-sig")
    return folder


def list_snapshots():
    items = []
    for p in sorted(SNAPSHOT_DIR.glob("*"), reverse=True):
        meta_file = p / "meta.csv"
        if meta_file.exists():
            try:
                meta = pd.read_csv(meta_file).iloc[0].to_dict()
            except Exception:
                meta = {"snapshot_name": p.name, "created_at": p.name[:15], "rows": None}
        else:
            meta = {"snapshot_name": p.name, "created_at": p.name[:15], "rows": None}
        meta["path"] = str(p)
        items.append(meta)
    return items


def read_snapshot(path: str) -> pd.DataFrame:
    p = Path(path)
    if (p / "data.parquet").exists():
        return pd.read_parquet(p / "data.parquet")
    raise FileNotFoundError("스냅샷 데이터 파일을 찾을 수 없습니다.")


def compare_snapshots(current: pd.DataFrame, previous: pd.DataFrame) -> pd.DataFrame:
    cols = ["회원키", "총구매금액", "총 실주문건수", "회원등급", "세그먼트", "권장채널", "불량회원"]
    a = current[cols].copy()
    b = previous[cols].copy()
    merged = a.merge(b, on="회원키", how="outer", suffixes=("_현재", "_이전"), indicator=True)
    merged["상태변화"] = np.select(
        [
            merged["_merge"] == "left_only",
            merged["_merge"] == "right_only",
            (merged["총 실주문건수_현재"].fillna(0) > merged["총 실주문건수_이전"].fillna(0)),
            (merged["총구매금액_현재"].fillna(0) > merged["총구매금액_이전"].fillna(0)),
            (merged["회원등급_현재"].astype(str) != merged["회원등급_이전"].astype(str)),
            (merged["세그먼트_현재"].astype(str) != merged["세그먼트_이전"].astype(str)),
            (merged["불량회원_현재"].fillna(False) != merged["불량회원_이전"].fillna(False)),
        ],
        [
            "신규 유입",
            "이전엔 있었으나 현재 없음",
            "주문 증가",
            "구매금액 증가",
            "회원등급 변경",
            "세그먼트 이동",
            "불량회원 상태 변경",
        ],
        default="변화 없음"
    )
    return merged


def dataframe_download(df: pd.DataFrame, label: str, filename: str):
    csv = df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(label, data=csv, file_name=filename, mime="text/csv")


def get_strategy_payload(seg_key: str) -> dict:
    return STRATEGY_LIBRARY.get(seg_key, STRATEGY_LIBRARY.get("비수신 분석 대상"))


@st.cache_data(show_spinner=False)
def build_campaign_output(seg_key: str, subset: pd.DataFrame):
    info = get_strategy_payload(seg_key)
    usable = subset[subset["권장채널"] != "분석만"] if "권장채널" in subset.columns else subset
    best_channel = usable["권장채널"].mode().iloc[0] if len(usable) else ", ".join(info["media"])
    return {
        "event": info["event"],
        "objective": info["objective"],
        "media": best_channel,
        "timing": info["timing"],
        "copy": info["copy"],
        "product": info["product"],
        "note": info["note"],
        "count": len(subset),
    }


def render_strategy_box(seg_key: str, subset: pd.DataFrame, title: str = "실행 전략 제안"):
    payload = build_campaign_output(seg_key, subset)
    st.markdown(f"### {title}")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**추천 이벤트**  \\n{payload['event']}")
        st.markdown(f"**핵심 목표**  \\n{payload['objective']}")
        st.markdown(f"**추천 매체**  \\n{payload['media']}")
    with c2:
        st.markdown(f"**추천 타이밍**  \\n{payload['timing']}")
        st.markdown(f"**추천 상품 방향**  \\n{payload['product']}")
        st.markdown(f"**주의사항**  \\n{payload['note']}")
    st.markdown("**추천 문구**")
    for idx, text in enumerate(payload["copy"], start=1):
        st.code(text, language=None)
        st.caption(f"문구 {idx}")


# ---------- UI ----------
st.title("📈 MISHARP CRM OS")
st.caption("엑셀 업로드 → 자동 세그먼트 → 실행 리스트 → 스냅샷 비교까지 한 번에 관리합니다.")

with st.sidebar:
    st.header("데이터 업로드")
    uploaded = st.file_uploader("회원 CSV / XLSX 업로드", type=["csv", "xlsx", "xls"])
    snapshot_name = st.text_input("이번 업로드 이름", value=datetime.now().strftime("%Y-%m-%d CRM"))
    save_now = st.button("스냅샷 저장")
    st.divider()
    st.subheader("저장된 스냅샷")
    snapshots = list_snapshots()
    snapshot_labels = [f"{s['created_at']} | {s['snapshot_name']} ({s['rows']} rows)" for s in snapshots]
    prev_choice = st.selectbox("비교할 이전 스냅샷", options=["선택 안 함"] + snapshot_labels)
    st.caption("카페24 실시간 연동이 없어도 주기적으로 파일 업로드하면 추세를 이어갈 수 있습니다.")

if uploaded is None:
    st.info("좌측 사이드바에서 회원 데이터를 업로드하세요. CSV와 XLSX 모두 지원합니다.")
    st.stop()

raw = load_uploaded(uploaded)
df = preprocess(raw)
df = calc_rfm(df)

if save_now:
    folder = save_snapshot(df, snapshot_name)
    st.success(f"스냅샷 저장 완료: {folder.name}")

cards = summary_cards(df)
cols = st.columns(4)
for i, (k, v) in enumerate(cards.items()):
    cols[i % 4].metric(k, f"{v:,}")

tabs = st.tabs([
    "CRM 대시보드", "고객 세그먼트", "실행 대상", "이탈 위험", "VIP 관리",
    "적립금 CRM", "캠페인 추천", "스냅샷 비교", "다운로드"
])

with tabs[0]:
    st.subheader("이번 주 액션 TOP")
    rec = recommended_actions(df)
    st.dataframe(rec, use_container_width=True, hide_index=True)
    seg_count = df[~df["불량회원"]]["세그먼트"].value_counts().reindex(SEGMENT_ORDER, fill_value=0).reset_index()
    seg_count.columns = ["세그먼트", "고객수"]
    st.bar_chart(seg_count.set_index("세그먼트"))
    channel_count = df[~df["불량회원"]]["권장채널"].value_counts().reset_index()
    channel_count.columns = ["채널", "고객수"]
    st.dataframe(channel_count, use_container_width=True, hide_index=True)

with tabs[1]:
    st.subheader("고객 세그먼트 자동 분류")
    exclude_bad = st.checkbox("불량회원 제외", value=True, key="seg_bad")
    seg = st.selectbox("세그먼트 선택", options=["전체"] + SEGMENT_ORDER, index=0)
    view = df.copy()
    if exclude_bad:
        view = view[~view["불량회원"]]
    if seg != "전체":
        view = view[view["세그먼트"] == seg]
    show_cols = ["이름", "아이디", "회원등급", "총구매금액", "총 실주문건수", "최종접속일", "최종주문일", "권장채널", "세그먼트", "추천액션"]
    st.dataframe(view[show_cols], use_container_width=True, hide_index=True)
    dataframe_download(view[show_cols], "현재 목록 CSV 다운로드", f"segment_{seg}.csv")
    if seg != "전체" and len(view):
        render_strategy_box(seg, view, "세그먼트 실행 전략")

with tabs[2]:
    st.subheader("발송 가능 실행 대상")
    channel = st.multiselect("채널", options=["SMS", "앱푸시", "이메일"], default=["SMS", "앱푸시", "이메일"])
    action_seg = st.multiselect("세그먼트", options=SEGMENT_ORDER, default=SEGMENT_ORDER[:5])
    exclude_bad = st.checkbox("불량회원 제외", value=True, key="action_bad")
    sub = df[df["권장채널"].isin(channel)]
    sub = sub[sub["권장채널"] != "분석만"]
    if exclude_bad:
        sub = sub[~sub["불량회원"]]
    if action_seg:
        sub = sub[sub["세그먼트"].isin(action_seg)]
    out_cols = ["이름", "휴대폰번호", "이메일", "회원등급", "총구매금액", "총 실주문건수", "권장채널", "세그먼트", "추천액션"]
    st.dataframe(sub[out_cols], use_container_width=True, hide_index=True)
    dataframe_download(sub[out_cols], "실행 대상 리스트 다운로드", "crm_action_list.csv")
    if len(action_seg) == 1 and len(sub):
        render_strategy_box(action_seg[0], sub, "발송 전략 및 문구 제안")
    elif len(sub):
        st.info("세그먼트를 1개만 선택하면 하단에 해당 고객군 전용 이벤트/매체/문구 제안이 함께 표시됩니다.")

with tabs[3]:
    st.subheader("이탈 위험 고객")
    exclude_bad = st.checkbox("불량회원 제외", value=True, key="risk_bad")
    risk = df[(df["고액고객"] & df["최근90일주문없음"]) | ((df["총 실주문건수"] >= 2) & df["최근180일주문없음"])]
    if exclude_bad:
        risk = risk[~risk["불량회원"]]
    risk = risk.sort_values(["총구매금액", "총 실주문건수"], ascending=[False, False])
    out_cols = ["이름", "아이디", "회원등급", "총구매금액", "총 실주문건수", "최종주문일", "권장채널", "세그먼트", "추천액션"]
    st.dataframe(risk[out_cols], use_container_width=True, hide_index=True)
    st.caption("고액 고객이거나 재구매 이력이 있었던 고객 중 최근 주문이 끊긴 대상을 우선 관리합니다.")
    dataframe_download(risk[out_cols], "이탈 위험 리스트 다운로드", "churn_risk_list.csv")
    if len(risk):
        render_strategy_box("고액 이탈 위험", risk, "이탈 방지 실행 전략")

with tabs[4]:
    st.subheader("VIP / 우수고객 관리")
    exclude_bad = st.checkbox("불량회원 제외", value=True, key="vip_bad")
    vip = df[
        (df["회원등급"].astype(str).str.contains("VIP|VVIP|골드|실버", na=False)) |
        (df["고액고객"])
    ].copy()
    if exclude_bad:
        vip = vip[~vip["불량회원"]]
    vip["활성상태"] = np.where(vip["최근60일주문"], "활성", np.where(vip["최근90일주문없음"], "주의", "보통"))
    vip = vip.sort_values(["총구매금액", "총 실주문건수"], ascending=False)
    out_cols = ["이름", "아이디", "회원등급", "총구매금액", "총 실주문건수", "최종주문일", "활성상태", "권장채널", "추천액션"]
    st.dataframe(vip[out_cols], use_container_width=True, hide_index=True)
    dataframe_download(vip[out_cols], "VIP 리스트 다운로드", "vip_list.csv")
    if len(vip):
        render_strategy_box("VIP/고액 활성 고객", vip, "VIP 케어 전략")

with tabs[5]:
    st.subheader("적립금 CRM")
    exclude_bad = st.checkbox("불량회원 제외", value=True, key="point_bad")
    point_df = df[df["사용가능 적립금"] > 0].copy()
    if exclude_bad:
        point_df = point_df[~point_df["불량회원"]]
    point_df["적립금구간"] = pd.cut(point_df["사용가능 적립금"], bins=[0, 1000, 5000, 10000, np.inf],
                                 labels=["1천원 이하", "1천~5천원", "5천~1만원", "1만원 초과"])
    st.dataframe(
        point_df[["이름", "아이디", "사용가능 적립금", "총구매금액", "최종주문일", "권장채널", "세그먼트"]],
        use_container_width=True, hide_index=True
    )
    st.bar_chart(point_df["적립금구간"].value_counts().sort_index())
    dataframe_download(point_df[["이름", "아이디", "휴대폰번호", "이메일", "사용가능 적립금", "권장채널", "세그먼트"]],
                       "적립금 대상 리스트 다운로드", "point_targets.csv")
    if len(point_df):
        seg_key = "휴면복귀_적립금" if (point_df["최근3년접속없음"].any()) else "활성 재구매 고객"
        render_strategy_box(seg_key, point_df, "적립금 활용 전략")

with tabs[6]:
    st.subheader("캠페인 추천")
    rec = recommended_actions(df)
    if rec.empty:
        st.warning("추천할 캠페인이 없습니다.")
    else:
        st.dataframe(rec, use_container_width=True, hide_index=True)
        selected = st.selectbox("캠페인 상세 보기", options=rec["우선순위 세그먼트"].tolist())
        if selected == "휴면복귀_고액":
            subset = df[(df["최근3년접속없음"]) & (df["휴면복귀등급"] == "높음") & (~df["불량회원"])]
        else:
            subset = df[(df["세그먼트"] == selected) & (~df["불량회원"])]
        st.dataframe(subset[["이름", "아이디", "회원등급", "총구매금액", "총 실주문건수", "권장채널"]], use_container_width=True, hide_index=True)
        render_strategy_box(selected, subset, "캠페인 실행 가이드")

        st.markdown("### 장기 휴면 복귀 센터")
        dormant = df[(df["최근3년접속없음"]) & (~df["불량회원"])].copy()
        if len(dormant) == 0:
            st.info("3년 이상 미접속 고객이 없습니다.")
        else:
            mode = st.selectbox("휴면 고객 보기", options=["전체", "복귀 가능성 높은 고객", "고액 휴면 고객", "적립금 보유 휴면 고객"])
            if mode == "복귀 가능성 높은 고객":
                dormant = dormant[dormant["휴면복귀등급"] == "높음"]
                seg_key = "휴면복귀_고액"
            elif mode == "고액 휴면 고객":
                dormant = dormant[dormant["고액고객"]]
                seg_key = "휴면복귀_고액"
            elif mode == "적립금 보유 휴면 고객":
                dormant = dormant[dormant["적립금보유"]]
                seg_key = "휴면복귀_적립금"
            else:
                seg_key = "장기 휴면/이탈"
            st.dataframe(dormant[["이름", "아이디", "총구매금액", "총 실주문건수", "마지막접속경과일", "휴면복귀점수", "휴면복귀등급", "권장채널"]], use_container_width=True, hide_index=True)
            dataframe_download(dormant[["이름", "아이디", "휴대폰번호", "이메일", "총구매금액", "총 실주문건수", "휴면복귀점수", "휴면복귀등급", "권장채널"]],
                               "휴면 복귀 리스트 다운로드", "dormant_reactivation.csv")
            render_strategy_box(seg_key, dormant, "휴면 복귀 실행 전략")

with tabs[7]:
    st.subheader("스냅샷 비교")
    if prev_choice == "선택 안 함":
        st.info("사이드바에서 비교할 이전 스냅샷을 선택하세요.")
    else:
        idx = snapshot_labels.index(prev_choice)
        prev = read_snapshot(snapshots[idx]["path"])
        comp = compare_snapshots(df, prev)
        summary = comp["상태변화"].value_counts().reset_index()
        summary.columns = ["상태변화", "건수"]
        c1, c2 = st.columns([1, 2])
        with c1:
            st.dataframe(summary, use_container_width=True, hide_index=True)
        with c2:
            st.bar_chart(summary.set_index("상태변화"))
        st.dataframe(comp[["회원키", "상태변화", "총 실주문건수_현재", "총 실주문건수_이전", "총구매금액_현재", "총구매금액_이전", "세그먼트_현재", "세그먼트_이전"]], use_container_width=True, hide_index=True)
        dataframe_download(comp, "비교 결과 다운로드", "snapshot_compare.csv")

with tabs[8]:
    st.subheader("한 번에 다운로드")
    bundles = {
        "신규 가입 후 미구매": df[(df["세그먼트"] == "신규 가입 후 미구매") & (~df["불량회원"])],
        "첫 구매 후 재구매 대기": df[(df["세그먼트"] == "첫 구매 후 재구매 대기") & (~df["불량회원"])],
        "고액 이탈 위험": df[(df["세그먼트"] == "고액 이탈 위험") & (~df["불량회원"])],
        "VIP/고액 활성 고객": df[(df["세그먼트"] == "VIP/고액 활성 고객") & (~df["불량회원"])],
        "SMS 발송 가능 전체": df[(df["권장채널"] == "SMS") & (~df["불량회원"])],
        "복귀 가능성 높은 장기휴면": df[(df["최근3년접속없음"]) & (df["휴면복귀등급"] == "높음") & (~df["불량회원"])],
    }
    for name, sub in bundles.items():
        st.markdown(f"**{name}** · {len(sub):,}명")
        dataframe_download(
            sub[["이름", "아이디", "휴대폰번호", "이메일", "회원등급", "총구매금액", "총 실주문건수", "권장채널", "세그먼트", "추천액션"]],
            f"{name} 다운로드",
            f"{name}.csv"
        )

st.divider()
st.caption("MISHARP CRM OS · 카페24 실시간 연동이 없더라도, 같은 형식의 파일을 정기 업로드하면 전략을 이어갈 수 있도록 설계했습니다.")


# Footer
import streamlit as st
st.markdown('---')
st.markdown('made by MISHARP, MIYAWA')

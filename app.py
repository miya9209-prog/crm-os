
import io
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

def bool_map(v):
    if pd.isna(v):
        return False
    s = str(v).strip().upper()
    return s in {"T", "TRUE", "Y", "YES", "1"}

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

    # 기본 정리
    out["회원키"] = out["아이디"].astype(str).str.strip()
    out["회원키"] = np.where(out["회원키"].isin(["", "nan", "None"]), out["휴대폰번호"].astype(str), out["회원키"])

    # 상태 플래그
    today = pd.Timestamp(datetime.now().date())
    out["가입후경과일"] = (today - out["회원 가입일"]).dt.days
    out["마지막주문경과일"] = (today - out["최종주문일"]).dt.days
    out["마지막접속경과일"] = (today - out["최종접속일"]).dt.days

    out["구매회원"] = out["총 실주문건수"] > 0
    out["미구매회원"] = out["총 실주문건수"] <= 0
    out["최근30일접속"] = out["마지막접속경과일"].between(0, 30, inclusive="both")
    out["최근60일주문"] = out["마지막주문경과일"].between(0, 60, inclusive="both")
    out["최근90일주문없음"] = out["마지막주문경과일"] > 90
    out["최근180일주문없음"] = out["마지막주문경과일"] > 180
    out["고액고객"] = out["총구매금액"] >= 300000
    out["초우수고객"] = out["총구매금액"] >= 500000
    out["방문많음"] = out["총 방문횟수(1년 내)"] >= 10
    out["적립금보유"] = out["사용가능 적립금"] > 0
    out["발송가능_SMS"] = out["SMS 수신여부"] & (~out["탈퇴여부"])
    out["발송가능_이메일"] = out["e메일 수신여부"] & (~out["탈퇴여부"])
    out["발송가능_앱"] = out["모바일앱 이용여부"] & (~out["탈퇴여부"])

    # 채널 우선순위
    out["권장채널"] = np.select(
        [
            out["발송가능_SMS"],
            out["발송가능_앱"],
            out["발송가능_이메일"],
        ],
        ["SMS", "앱푸시", "이메일"],
        default="분석만"
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
        (df["최근180일주문없음"]) | (df["휴면처리일"].notna()),
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

    # quantile 대신 실무형 컷
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
        "신규 가입 후 미구매": int((df["세그먼트"] == "신규 가입 후 미구매").sum()),
        "첫 구매 후 재구매 대기": int((df["세그먼트"] == "첫 구매 후 재구매 대기").sum()),
        "고액 이탈 위험": int((df["세그먼트"] == "고액 이탈 위험").sum()),
        "최근 90일 활성 구매고객": len(active_buyers),
        "사용가능 적립금 보유": int((df["사용가능 적립금"] > 0).sum()),
        "앱 이용 고객": int(df["모바일앱 이용여부"].sum()),
    }

def recommended_actions(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for seg in SEGMENT_ORDER:
        sub = df[df["세그먼트"] == seg]
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
    cols = ["회원키", "총구매금액", "총 실주문건수", "회원등급", "세그먼트", "권장채널"]
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
        ],
        [
            "신규 유입",
            "이전엔 있었으나 현재 없음",
            "주문 증가",
            "구매금액 증가",
            "회원등급 변경",
            "세그먼트 이동",
        ],
        default="변화 없음"
    )
    return merged

def dataframe_download(df: pd.DataFrame, label: str, filename: str):
    csv = df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(label, data=csv, file_name=filename, mime="text/csv")

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
    seg_count = df["세그먼트"].value_counts().reindex(SEGMENT_ORDER, fill_value=0).reset_index()
    seg_count.columns = ["세그먼트", "고객수"]
    st.bar_chart(seg_count.set_index("세그먼트"))
    channel_count = df["권장채널"].value_counts().reset_index()
    channel_count.columns = ["채널", "고객수"]
    st.dataframe(channel_count, use_container_width=True, hide_index=True)

with tabs[1]:
    st.subheader("고객 세그먼트 자동 분류")
    seg = st.selectbox("세그먼트 선택", options=["전체"] + SEGMENT_ORDER, index=0)
    view = df.copy()
    if seg != "전체":
        view = view[view["세그먼트"] == seg]
    show_cols = ["이름", "아이디", "회원등급", "총구매금액", "총 실주문건수", "최종접속일", "최종주문일", "권장채널", "세그먼트", "추천액션"]
    st.dataframe(view[show_cols], use_container_width=True, hide_index=True)
    dataframe_download(view[show_cols], "현재 목록 CSV 다운로드", f"segment_{seg}.csv")

with tabs[2]:
    st.subheader("발송 가능 실행 대상")
    channel = st.multiselect("채널", options=["SMS", "앱푸시", "이메일"], default=["SMS", "앱푸시", "이메일"])
    sub = df[df["권장채널"].isin(channel)]
    sub = sub[sub["권장채널"] != "분석만"]
    action_seg = st.multiselect("세그먼트", options=SEGMENT_ORDER, default=SEGMENT_ORDER[:5])
    if action_seg:
        sub = sub[sub["세그먼트"].isin(action_seg)]
    out_cols = ["이름", "휴대폰번호", "이메일", "회원등급", "총구매금액", "총 실주문건수", "권장채널", "세그먼트", "추천액션"]
    st.dataframe(sub[out_cols], use_container_width=True, hide_index=True)
    dataframe_download(sub[out_cols], "실행 대상 리스트 다운로드", "crm_action_list.csv")

with tabs[3]:
    st.subheader("이탈 위험 고객")
    risk = df[(df["고액고객"] & df["최근90일주문없음"]) | ((df["총 실주문건수"] >= 2) & df["최근180일주문없음"])]
    risk = risk.sort_values(["총구매금액", "총 실주문건수"], ascending=[False, False])
    out_cols = ["이름", "아이디", "회원등급", "총구매금액", "총 실주문건수", "최종주문일", "권장채널", "세그먼트", "추천액션"]
    st.dataframe(risk[out_cols], use_container_width=True, hide_index=True)
    st.caption("고액 고객이거나 재구매 이력이 있었던 고객 중 최근 주문이 끊긴 대상을 우선 관리합니다.")
    dataframe_download(risk[out_cols], "이탈 위험 리스트 다운로드", "churn_risk_list.csv")

with tabs[4]:
    st.subheader("VIP / 우수고객 관리")
    vip = df[
        (df["회원등급"].astype(str).str.contains("VIP|VVIP|골드|실버", na=False)) |
        (df["고액고객"])
    ].copy()
    vip["활성상태"] = np.where(vip["최근60일주문"], "활성", np.where(vip["최근90일주문없음"], "주의", "보통"))
    vip = vip.sort_values(["총구매금액", "총 실주문건수"], ascending=False)
    out_cols = ["이름", "아이디", "회원등급", "총구매금액", "총 실주문건수", "최종주문일", "활성상태", "권장채널", "추천액션"]
    st.dataframe(vip[out_cols], use_container_width=True, hide_index=True)
    dataframe_download(vip[out_cols], "VIP 리스트 다운로드", "vip_list.csv")

with tabs[5]:
    st.subheader("적립금 CRM")
    point_df = df[df["사용가능 적립금"] > 0].copy()
    point_df["적립금구간"] = pd.cut(point_df["사용가능 적립금"], bins=[0, 1000, 5000, 10000, np.inf],
                                 labels=["1천원 이하", "1천~5천원", "5천~1만원", "1만원 초과"])
    st.dataframe(
        point_df[["이름", "아이디", "사용가능 적립금", "총구매금액", "최종주문일", "권장채널", "세그먼트"]],
        use_container_width=True, hide_index=True
    )
    st.bar_chart(point_df["적립금구간"].value_counts().sort_index())
    dataframe_download(point_df[["이름", "아이디", "휴대폰번호", "이메일", "사용가능 적립금", "권장채널", "세그먼트"]],
                       "적립금 대상 리스트 다운로드", "point_targets.csv")

with tabs[6]:
    st.subheader("캠페인 추천")
    rec = recommended_actions(df)
    if rec.empty:
        st.warning("추천할 캠페인이 없습니다.")
    else:
        st.dataframe(rec, use_container_width=True, hide_index=True)
        selected = st.selectbox("캠페인 상세 보기", options=rec["우선순위 세그먼트"].tolist())
        subset = df[df["세그먼트"] == selected]
        st.markdown(f"**추천 액션:** {ACTION_TEXT[selected]}")
        st.markdown(f"**권장 채널:** {subset[subset['권장채널']!='분석만']['권장채널'].mode().iloc[0] if len(subset[subset['권장채널']!='분석만']) else '분석만'}")
        st.dataframe(subset[["이름","아이디","회원등급","총구매금액","총 실주문건수","권장채널"]], use_container_width=True, hide_index=True)

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
        st.dataframe(comp[["회원키","상태변화","총 실주문건수_현재","총 실주문건수_이전","총구매금액_현재","총구매금액_이전","세그먼트_현재","세그먼트_이전"]], use_container_width=True, hide_index=True)
        dataframe_download(comp, "비교 결과 다운로드", "snapshot_compare.csv")

with tabs[8]:
    st.subheader("한 번에 다운로드")
    bundles = {
        "신규 가입 후 미구매": df[df["세그먼트"] == "신규 가입 후 미구매"],
        "첫 구매 후 재구매 대기": df[df["세그먼트"] == "첫 구매 후 재구매 대기"],
        "고액 이탈 위험": df[df["세그먼트"] == "고액 이탈 위험"],
        "VIP/고액 활성 고객": df[df["세그먼트"] == "VIP/고액 활성 고객"],
        "SMS 발송 가능 전체": df[df["권장채널"] == "SMS"],
    }
    for name, sub in bundles.items():
        st.markdown(f"**{name}** · {len(sub):,}명")
        dataframe_download(
            sub[["이름","아이디","휴대폰번호","이메일","회원등급","총구매금액","총 실주문건수","권장채널","세그먼트","추천액션"]],
            f"{name} 다운로드",
            f"{name}.csv"
        )

st.divider()
st.caption("MISHARP CRM OS · 카페24 실시간 연동이 없더라도, 같은 형식의 파일을 정기 업로드하면 전략을 이어갈 수 있도록 설계했습니다.")

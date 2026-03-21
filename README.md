
# MISHARP CRM OS + Newsletter

## 실행
```bash
pip install -r requirements.txt
streamlit run app.py
```

## 주요 기능
- CRM 세그먼트 자동 분류
- 불량회원 제외 / 휴면복귀 분석
- 고객군별 전략·문구 추천
- 이메일 뉴스레터 HTML 자동 생성
- SendGrid 테스트 발송 / 즉시 발송 / 예약 큐 저장
- `mailer_worker.py`로 예약 큐 실행

## 예약 발송
예약 저장 후 아래 스크립트를 주기적으로 실행하세요.
```bash
python mailer_worker.py
```
Windows 작업 스케줄러 또는 cron에서 5분 간격으로 실행하면 됩니다.

## 환경변수 또는 Streamlit secrets
- SENDGRID_API_KEY
- SENDGRID_FROM_EMAIL
- SENDGRID_FROM_NAME
- SENDGRID_REPLY_TO

`.streamlit/secrets.toml.example`를 참고해 `.streamlit/secrets.toml`을 만드세요.

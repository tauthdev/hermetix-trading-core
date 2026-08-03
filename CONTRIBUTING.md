# 기여 가이드

hermetix-trading-core 는 증권사 모의투자 API(넥스트증권·한국투자·키움)로 자동매매 전략을 만드는 커뮤니티 프레임워크입니다. 기여를 환영합니다.

## 어디에 기여하나요?

| 하고 싶은 것 | 방법 |
|---|---|
| 내 전략을 만들고 싶다 | 이 레포가 아니라 [next-strategy-template](https://github.com/tauthdev/next-strategy-template) 으로 시작하세요. 코어 수정이 필요 없습니다 |
| 만든 전략을 공유하고 싶다 | 전략 레포를 Public 으로 만들고 [Strategy Share 이슈](../../issues/new?template=strategy-share.md)로 알려주세요. README 의 커뮤니티 전략 목록에 추가합니다 |
| 코어 버그를 찾았다 | [Bug Report 이슈](../../issues/new?template=bug_report.md) |
| 코어에 기능을 제안하고 싶다 | [Feature Request 이슈](../../issues/new?template=feature_request.md) 로 먼저 논의 후 PR |

## 개발 환경

- JDK 17, Kotlin 1.9 (Gradle Wrapper 포함 — 로컬 Gradle 설치 불필요)

```bash
git clone https://github.com/tauthdev/hermetix-trading-core.git
cd hermetix-trading-core
./gradlew build            # 컴파일 + 단위 테스트
./gradlew publishToMavenLocal   # 로컬에서 전략 레포와 함께 개발할 때
```

실서버 스모크 테스트(선택)는 모의투자 키가 있어야 합니다:

```bash
NEXT_CLIENT_ID=pk_test_... NEXT_CLIENT_SECRET=sk_test_... ./gradlew test --tests '*.RealApiSmokeTest'
```

## PR 규칙

1. **이슈 먼저**: 기능 추가는 이슈에서 방향을 합의한 뒤 PR 을 올려주세요 (버그 수정은 바로 PR 가능)
2. **테스트 포함**: 동작 변경에는 단위 테스트를 함께 주세요. 테스트를 비활성화해서 통과시키는 PR 은 받지 않습니다
3. **하위 호환**: 전략 SPI(`TradingStrategy`, `StrategyContext`, `Signal`)의 시그니처 변경은 모든 전략 레포를 깨뜨립니다. breaking change 는 이슈에서 충분히 논의합니다
4. **커밋 메시지**: `Imp:`(기능), `Fix:`(버그), `Docs:`(문서), `Chores:`(잡무) 프리픽스를 사용합니다
5. **키/시크릿 금지**: API 키가 포함된 커밋은 즉시 리젝됩니다. 키는 환경변수나 gitignore 된 `application-local.yml` 로만

## 릴리즈

- 버전은 `build.gradle.kts` 의 `version` + git 태그로 관리합니다 (JitPack 이 태그를 빌드)
- SPI 가 바뀌면 minor 버전을 올립니다 (0.x 대에서는 0.2.0 처럼)

## 행동 규칙

서로 존중하고, 전략의 수익률이 아니라 코드와 아이디어로 이야기합니다. 이 프로젝트의 모든 것은 **모의투자 학습용**이며 투자 조언이 아닙니다.

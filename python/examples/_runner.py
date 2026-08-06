"""예제 공용 실행기 - HERMETIX_BROKER 환경변수로 브로커 선택."""
import logging
import os
import sys

sys.path.insert(0, __file__.rsplit("/", 2)[0])

from hermetix import KisClient, KiwoomClient, NextClient, Strategy, StrategyEngine


def run(strategy: Strategy) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    broker_name = os.environ.get("HERMETIX_BROKER", "next")
    if broker_name == "next":
        broker = NextClient(os.environ["NEXT_CLIENT_ID"], os.environ["NEXT_CLIENT_SECRET"])
    elif broker_name == "kis":
        broker = KisClient(os.environ["KIS_APPKEY"], os.environ["KIS_APPSECRET"], os.environ["KIS_CANO"])
    elif broker_name == "kiwoom":
        broker = KiwoomClient(os.environ["KIWOOM_APPKEY"], os.environ["KIWOOM_SECRETKEY"])
    else:
        raise SystemExit(f"unknown HERMETIX_BROKER: {broker_name}")
    StrategyEngine(broker, [strategy]).run()

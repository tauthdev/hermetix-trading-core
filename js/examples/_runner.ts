/** 예제 공용 실행기 - HERMETIX_BROKER 환경변수로 브로커 선택. */
import { KisClient, KiwoomClient, NextClient, StrategyEngine } from "../src/index.js";
import type { BrokerClient, Strategy } from "../src/index.js";

export async function run(strategy: Strategy): Promise<void> {
  const env = process.env;
  const name = env.HERMETIX_BROKER ?? "next";
  let broker: BrokerClient;
  if (name === "next") broker = new NextClient(env.NEXT_CLIENT_ID!, env.NEXT_CLIENT_SECRET!);
  else if (name === "kis") broker = new KisClient(env.KIS_APPKEY!, env.KIS_APPSECRET!, env.KIS_CANO!);
  else if (name === "kiwoom") broker = new KiwoomClient(env.KIWOOM_APPKEY!, env.KIWOOM_SECRETKEY!);
  else throw new Error(`unknown HERMETIX_BROKER: ${name}`);
  await new StrategyEngine(broker, [strategy]).run();
}

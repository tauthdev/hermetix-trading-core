/**
 * 실서버 스모크 (수동 실행 전용):
 *   node dist/tests/smoke.js next|kis|kiwoom   (환경변수로 키 주입)
 */
import { Decimal } from "decimal.js";
import { BrokerApiError, KisClient, KiwoomClient, MarketCalendar, NextClient, pnlReport } from "../src/index.js";
import type { BrokerClient } from "../src/index.js";

function makeClient(name: string): [BrokerClient, string, Decimal | null] {
  const env = process.env;
  if (name === "next") return [new NextClient(env.NEXT_CLIENT_ID!, env.NEXT_CLIENT_SECRET!), "AAPL", new Decimal(150)];
  if (name === "kis") return [new KisClient(env.KIS_APPKEY!, env.KIS_APPSECRET!, env.KIS_CANO!), "005930", null];
  if (name === "kiwoom") return [new KiwoomClient(env.KIWOOM_APPKEY!, env.KIWOOM_SECRETKEY!), "005930", null];
  throw new Error(`unknown broker: ${name}`);
}

async function main(name: string): Promise<void> {
  const [client, symbol, farPriceFixed] = makeClient(name);
  console.log(`== ${name} (${client.capabilities.market}) ==`);

  const [quote] = await client.getQuotes([symbol]);
  if (!quote.price.gt(0)) throw new Error("price <= 0");
  console.log(`현재가 ${symbol}: ${quote.price} / 등락률 ${quote.changeRate}`);

  const candles = await client.getCandles(symbol, "1d", 30);
  if (candles.length < 20 || candles[0].timestamp >= candles[candles.length - 1].timestamp) throw new Error("candles bad");
  console.log(`일봉 ${candles.length}개 / 최신 종가 ${candles[candles.length - 1].close}`);

  console.log(`정규장 open = ${await new MarketCalendar(client).isRegularOpen()}`);

  const account = await client.getAccount();
  if (!account.cash.gt(0)) throw new Error("cash <= 0");
  console.log(`예수금 ${account.cash} / 총평가 ${account.portfolioValue}`);

  const holdings = await client.getHoldings();
  console.log(`보유 ${holdings.length}종목: ${holdings.slice(0, 5).map((h) => `${h.symbol}x${h.quantity}`).join(", ")}`);

  const power = await client.getBuyingPower();
  if (!power.gt(0)) throw new Error("buying power <= 0");
  console.log(`주문가능 ${power}`);

  await client.getOrders();
  await client.getFills();

  const farPrice = farPriceFixed ?? quote.price.mul("0.8");
  try {
    const order = await client.createOrder({
      symbol, side: "BUY", orderType: "LIMIT", quantity: new Decimal(1), limitPrice: farPrice,
    });
    console.log(`주문 접수: ${order.orderId}`);
    console.log(`주문 조회: ${(await client.getOrder(order.orderId)).status}`);
    console.log(`주문 취소: ${(await client.cancelOrder(order.orderId)).status}`);
  } catch (e) {
    if (e instanceof BrokerApiError) console.log(`주문 스킵: ${e.message}`);
    else throw e;
  }

  const report = await pnlReport(client);
  console.log(`PnL: portfolio=${report.portfolioValue} unrealized=${report.totalUnrealizedPnl}`);
  console.log("SMOKE OK");
}

main(process.argv[2] ?? "next").catch((e) => {
  console.error("SMOKE FAILED:", e);
  process.exit(1);
});

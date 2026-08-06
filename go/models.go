// Package hermetix - 증권사 모의투자 통합 트레이딩 프레임워크 (Go).
//
// 금액/수량은 전부 decimal.Decimal - float 를 절대 섞지 말 것.
package hermetix

import (
	"time"

	"github.com/shopspring/decimal"
)

type CandleInterval string

const (
	Min1  CandleInterval = "1m"
	Min5  CandleInterval = "5m"
	Hour1 CandleInterval = "1h"
	Day1  CandleInterval = "1d"
)

type OrderSide string

const (
	Buy  OrderSide = "BUY"
	Sell OrderSide = "SELL"
)

type OrderType string

const (
	Market OrderType = "MARKET"
	Limit  OrderType = "LIMIT"
)

type TimeInForce string

const (
	Day TimeInForce = "DAY"
	GTC TimeInForce = "GTC"
)

type OrderStatus string

const (
	Submitted       OrderStatus = "SUBMITTED"
	PartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	Filled          OrderStatus = "FILLED"
	Canceled        OrderStatus = "CANCELED"
	Rejected        OrderStatus = "REJECTED"
	Expired         OrderStatus = "EXPIRED"
	Unknown         OrderStatus = "UNKNOWN"
)

func (s OrderStatus) IsOpen() bool {
	return s == Submitted || s == PartiallyFilled
}

type Quote struct {
	Symbol     string
	Price      decimal.Decimal
	BidPrice   *decimal.Decimal
	AskPrice   *decimal.Decimal
	Volume     int64
	Change     *decimal.Decimal
	ChangeRate *decimal.Decimal
	Timestamp  time.Time
}

type Candle struct {
	Timestamp time.Time
	Open      decimal.Decimal
	High      decimal.Decimal
	Low       decimal.Decimal
	Close     decimal.Decimal
	Volume    int64
}

type SessionHours struct {
	Start string // "09:00"
	End   string // "15:30"
}

type MarketDay struct {
	Date     string // "2026-08-06"
	Open     bool
	Regular  *SessionHours
	Timezone string
	Holiday  string
}

type Account struct {
	AccountID      string
	Currency       string
	Cash           decimal.Decimal
	PortfolioValue decimal.Decimal
	Status         string
}

type Holding struct {
	Symbol            string
	Quantity          decimal.Decimal
	AvgEntryPrice     decimal.Decimal
	CurrentPrice      *decimal.Decimal
	MarketValue       *decimal.Decimal
	UnrealizedPnl     *decimal.Decimal
	UnrealizedPnlRate *decimal.Decimal
}

type CreateOrderRequest struct {
	Symbol        string
	Side          OrderSide
	OrderType     OrderType
	Quantity      decimal.Decimal
	LimitPrice    *decimal.Decimal
	TimeInForce   TimeInForce
	ClientOrderID string
}

type Order struct {
	OrderID        string
	Status         OrderStatus
	Symbol         string
	Side           OrderSide
	OrderType      OrderType
	Quantity       *decimal.Decimal
	LimitPrice     *decimal.Decimal
	FilledQuantity *decimal.Decimal
	AvgFillPrice   *decimal.Decimal
	ClientOrderID  string
	SubmittedAt    *time.Time
	CanceledAt     *time.Time
}

type Fill struct {
	FillID   string
	OrderID  string
	Symbol   string
	Side     OrderSide
	Quantity *decimal.Decimal
	Price    *decimal.Decimal
}

// BrokerCapabilities - 브로커가 지원하는 기능의 코드 선언. 실측으로 확인한 것만 true.
type BrokerCapabilities struct {
	BrokerID         string
	Market           string // "US" | "KRX"
	Currency         string
	CandleIntervals  map[CandleInterval]bool
	ClientOrderID    bool
	NativeBracket    bool
	FractionalShares bool
	// false 면 어댑터가 메모리 추적 (재시작 시 추적 소실)
	ServerOpenOrders bool
}

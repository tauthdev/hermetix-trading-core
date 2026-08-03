package com.tripleauth.nexttrading.pnl

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

/**
 * 수익률 조회 엔드포인트. 전략 앱이 웹 앱일 때 자동 등록된다.
 *
 * `GET /pnl` → 계좌 총평가/현금/평가손익/종목별 손익 JSON
 */
@RestController
class PnlController(
    private val pnlService: PnlService,
) {

    @GetMapping("/pnl")
    fun pnl(): PnlReport = pnlService.report()
}

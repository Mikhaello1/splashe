package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MoyuFunding/exchange-pb/go/pkg/mexc"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// ==========================================
// НАСТРОЙКИ ФИЛЬТРАЦИИ И ПОИСКА СПЛЕШЕЙ
// ==========================================
const (
	IntervalSeconds  = 1     // Интервал проверки (в секундах)
	ThresholdPercent = 0.1   // Минимальное движение цены (в %)
	MinVolumeUSD     = 200.0 // Минимальный объем торгов внутри интервала (в USD)
)

const (
	WSEndpoint      = "wss://wbs-api.mexc.com/ws"
	RestAPIEndpoint = "https://api.mexc.com/api/v3/defaultSymbols"
	MaxSubsPerConn  = 30 // Лимит MEXC Spot: до 30 подписок на одно WS-соединение
	DealChannelFmt  = "spot@public.aggre.deals.v3.api.pb@100ms@%s"
)

type TickerStats struct {
	Prices []float64
	Volume float64
}

type DefaultSymbolsResponse struct {
	Code int      `json:"code"`
	Data []string `json:"data"`
	Msg  *string  `json:"msg"`
}

var (
	marketData = make(map[string]*TickerStats)
	dataMu     sync.Mutex
)

func main() {
	log.Println("🔄 Шаг 1: Запрос списка всех спотовых пар с MEXC...")
	allSymbols, err := fetchAllSpotSymbols()
	if err != nil {
		log.Fatalf("❌ Не удалось получить список пар: %v", err)
	}
	log.Printf("✅ Найдено активных спотовых пар для мониторинга: %d", len(allSymbols))

	log.Println("📊 Шаг 2: Запуск фонового анализатора рынка...")
	go startAnalyzer()

	log.Println("🔌 Шаг 3: Запуск пула WebSocket соединений...")
	workers := (len(allSymbols) + MaxSubsPerConn - 1) / MaxSubsPerConn
	log.Printf("📡 Будет создано %d WebSocket-соединений (до %d пар на каждое)", workers, MaxSubsPerConn)

	for i := 0; i < len(allSymbols); i += MaxSubsPerConn {
		end := i + MaxSubsPerConn
		if end > len(allSymbols) {
			end = len(allSymbols)
		}
		chunk := allSymbols[i:end]

		go startWebSocketWorker(chunk)
		time.Sleep(500 * time.Millisecond)
	}

	select {}
}

func fetchAllSpotSymbols() ([]string, error) {
	resp, err := http.Get(RestAPIEndpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResult DefaultSymbolsResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResult); err != nil {
		return nil, err
	}
	if len(apiResult.Data) == 0 {
		return nil, fmt.Errorf("пустой ответ API (code=%d)", apiResult.Code)
	}

	symbols := make([]string, 0, len(apiResult.Data))
	for _, symbol := range apiResult.Data {
		symbols = append(symbols, strings.ToUpper(symbol))
	}
	return symbols, nil
}

func startWebSocketWorker(symbols []string) {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(WSEndpoint, nil)
		if err != nil {
			log.Printf("❌ Ошибка подключения группы %v: %v. Повтор через 5 сек...", symbols[0], err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("🚀 WS подключен. Подписка на группу из %d пар (от %s)...", len(symbols), symbols[0])

		params := make([]string, len(symbols))
		for i, symbol := range symbols {
			params[i] = fmt.Sprintf(DealChannelFmt, symbol)
		}
		subMessage := map[string]interface{}{
			"method": "SUBSCRIPTION",
			"params": params,
		}
		msgBytes, _ := json.Marshal(subMessage)
		if err := conn.WriteMessage(websocket.TextMessage, msgBytes); err != nil {
			log.Printf("⚠️ Не удалось подписаться на группу %s: %v", symbols[0], err)
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		go func(c *websocket.Conn) {
			ticker := time.NewTicker(20 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				ping := map[string]interface{}{"method": "PING"}
				pingBytes, _ := json.Marshal(ping)
				if err := c.WriteMessage(websocket.TextMessage, pingBytes); err != nil {
					return
				}
			}
		}(conn)

		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ Соединение разорвано на группе %s. Переподключение...", symbols[0])
				_ = conn.Close()
				break
			}

			if messageType == websocket.TextMessage {
				continue
			}

			wrapper := &mexc.PushDataV3ApiWrapper{}
			if err := proto.Unmarshal(message, wrapper); err != nil {
				continue
			}

			deals := wrapper.GetPublicAggreDeals()
			if deals == nil || len(deals.GetDeals()) == 0 {
				continue
			}

			symbol := wrapper.GetSymbol()
			if symbol == "" {
				continue
			}

			dataMu.Lock()
			if _, exists := marketData[symbol]; !exists {
				marketData[symbol] = &TickerStats{}
			}
			stats := marketData[symbol]

			for _, deal := range deals.GetDeals() {
				price, err := strconv.ParseFloat(deal.GetPrice(), 64)
				if err != nil {
					continue
				}
				quantity, err := strconv.ParseFloat(deal.GetQuantity(), 64)
				if err != nil {
					continue
				}
				stats.Prices = append(stats.Prices, price)
				stats.Volume += price * quantity
			}
			dataMu.Unlock()
		}
		time.Sleep(2 * time.Second)
	}
}

func startAnalyzer() {
	ticker := time.NewTicker(IntervalSeconds * time.Second)
	for range ticker.C {
		dataMu.Lock()

		for symbol, stats := range marketData {
			if len(stats.Prices) < 2 {
				continue
			}

			startPrice := stats.Prices[0]
			endPrice := stats.Prices[len(stats.Prices)-1]
			totalVolume := stats.Volume

			priceDiff := ((endPrice - startPrice) / startPrice) * 100

			absDiff := priceDiff
			if absDiff < 0 {
				absDiff = -absDiff
			}

			if absDiff >= ThresholdPercent && totalVolume >= MinVolumeUSD {
				spotLink := spotExchangeLink(symbol)

				fmt.Println(strings.Repeat("-", 60))
				fmt.Printf("🔥 СПЛЕШ НА МОНЕТЕ: %s\n", symbol)
				fmt.Printf("📊 Движение: %.2f%% за %d сек.\n", priceDiff, IntervalSeconds)
				fmt.Printf("💰 Объем за интервал: $%.2f\n", totalVolume)
				fmt.Printf("🔗 Ссылка: %s\n", spotLink)
				fmt.Println(strings.Repeat("-", 60))
			}

			stats.Prices = nil
			stats.Volume = 0.0
		}

		dataMu.Unlock()
	}
}

func spotExchangeLink(symbol string) string {
	for _, quote := range []string{"USDT", "USDC", "USD1", "USDE", "BTC", "ETH"} {
		if strings.HasSuffix(symbol, quote) {
			base := strings.TrimSuffix(symbol, quote)
			return fmt.Sprintf("https://www.mexc.com/exchange/%s_%s", base, quote)
		}
	}
	return "https://www.mexc.com/exchange/" + symbol
}

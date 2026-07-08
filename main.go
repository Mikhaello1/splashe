package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
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
	AlertCooldownSec = 30    // Не слать повторный алерт по той же паре чаще, чем раз в N секунд
)

// ==========================================
// TELEGRAM
// ==========================================
// Токен бота задается переменной окружения TELEGRAM_BOT_TOKEN.
// Запуск: TELEGRAM_BOT_TOKEN="123456:ABC..." ./splash
const chatsFile = "tg_chats.json" // сюда сохраняются chat_id подписчиков

// Котировки, приравненные к доллару: для них объем price*quantity — это USD.
var usdQuotes = []string{"USDT", "USDC"}

// ==========================================
// ОБЩИЕ СТРУКТУРЫ
// ==========================================

type TickerStats struct {
	Prices []float64
	Volume float64
}

type marketKey struct {
	Exchange string // "MEXC", "KUCOIN", "GATE", "BITGET"
	Symbol   string // символ в нативном формате биржи
}

var (
	marketData = make(map[marketKey]*TickerStats)
	dataMu     sync.Mutex

	lastAlert   = make(map[marketKey]time.Time)
	lastAlertMu sync.Mutex
)

func recordTrade(exchange, symbol string, price, quantity float64) {
	key := marketKey{Exchange: exchange, Symbol: symbol}
	dataMu.Lock()
	stats, exists := marketData[key]
	if !exists {
		stats = &TickerStats{}
		marketData[key] = stats
	}
	stats.Prices = append(stats.Prices, price)
	stats.Volume += price * quantity
	dataMu.Unlock()
}

func main() {
	log.Println("🤖 Запуск Telegram-бота...")
	tg := newTelegramBot()
	go tg.pollUpdates()
	go tg.senderLoop()

	log.Println("📊 Запуск фонового анализатора рынка...")
	go startAnalyzer(tg)

	log.Println("🔌 Запуск воркеров бирж...")
	go startMEXC()
	go startKuCoin()
	go startGate()
	go startBitget()

	select {}
}

// ==========================================
// АНАЛИЗАТОР
// ==========================================

func startAnalyzer(tg *TelegramBot) {
	ticker := time.NewTicker(IntervalSeconds * time.Second)
	for range ticker.C {
		dataMu.Lock()
		for key, stats := range marketData {
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

			if absDiff >= ThresholdPercent && totalVolume >= MinVolumeUSD && shouldAlert(key) {
				link := exchangeLink(key)

				fmt.Println(strings.Repeat("-", 60))
				fmt.Printf("🔥 СПЛЕШ [%s]: %s\n", key.Exchange, key.Symbol)
				fmt.Printf("📊 Движение: %.2f%% за %d сек.\n", priceDiff, IntervalSeconds)
				fmt.Printf("💰 Объем за интервал: $%.2f\n", totalVolume)
				fmt.Printf("🔗 Ссылка: %s\n", link)
				fmt.Println(strings.Repeat("-", 60))

				arrow := "📈"
				if priceDiff < 0 {
					arrow = "📉"
				}
				text := fmt.Sprintf(
					"🔥 <b>%s</b> — %s\n%s Движение: <b>%+.2f%%</b> за %d сек.\n💰 Объем: $%.0f\n🔗 %s",
					key.Symbol, key.Exchange, arrow, priceDiff, IntervalSeconds, totalVolume, link,
				)
				tg.enqueue(text)
			}

			stats.Prices = nil
			stats.Volume = 0.0
		}
		dataMu.Unlock()
	}
}

func shouldAlert(key marketKey) bool {
	lastAlertMu.Lock()
	defer lastAlertMu.Unlock()
	if t, ok := lastAlert[key]; ok && time.Since(t) < AlertCooldownSec*time.Second {
		return false
	}
	lastAlert[key] = time.Now()
	return true
}

func exchangeLink(key marketKey) string {
	switch key.Exchange {
	case "MEXC":
		for _, quote := range usdQuotes {
			if strings.HasSuffix(key.Symbol, quote) {
				base := strings.TrimSuffix(key.Symbol, quote)
				return fmt.Sprintf("https://www.mexc.com/exchange/%s_%s", base, quote)
			}
		}
		return "https://www.mexc.com/exchange/" + key.Symbol
	case "KUCOIN":
		return "https://www.kucoin.com/trade/" + key.Symbol // формат BTC-USDT
	case "GATE":
		return "https://www.gate.io/trade/" + key.Symbol // формат BTC_USDT
	case "BITGET":
		return "https://www.bitget.com/spot/" + key.Symbol // формат BTCUSDT
	}
	return ""
}

func isUSDQuote(quote string) bool {
	for _, q := range usdQuotes {
		if strings.EqualFold(quote, q) {
			return true
		}
	}
	return false
}

// ==========================================
// TELEGRAM BOT
// ==========================================

type TelegramBot struct {
	token  string
	chats  map[int64]bool
	chatMu sync.Mutex
	queue  chan string
}

func newTelegramBot() *TelegramBot {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		log.Fatal("❌ Не задана переменная окружения TELEGRAM_BOT_TOKEN")
	}
	tg := &TelegramBot{
		token: token,
		chats: make(map[int64]bool),
		queue: make(chan string, 1000),
	}
	tg.loadChats()
	return tg
}

func (tg *TelegramBot) api(method string) string {
	return "https://api.telegram.org/bot" + tg.token + "/" + method
}

func (tg *TelegramBot) loadChats() {
	data, err := os.ReadFile(chatsFile)
	if err != nil {
		return
	}
	var ids []int64
	if err := json.Unmarshal(data, &ids); err != nil {
		return
	}
	for _, id := range ids {
		tg.chats[id] = true
	}
	log.Printf("✉️ Загружено подписчиков Telegram: %d", len(ids))
}

func (tg *TelegramBot) saveChats() {
	ids := make([]int64, 0, len(tg.chats))
	for id := range tg.chats {
		ids = append(ids, id)
	}
	data, _ := json.Marshal(ids)
	_ = os.WriteFile(chatsFile, data, 0600)
}

// pollUpdates слушает входящие сообщения боту: любой, кто напишет боту
// (например /start), подписывается на алерты.
func (tg *TelegramBot) pollUpdates() {
	offset := 0
	client := &http.Client{Timeout: 40 * time.Second}
	for {
		resp, err := client.Get(fmt.Sprintf("%s?timeout=30&offset=%d", tg.api("getUpdates"), offset))
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		var result struct {
			OK     bool `json:"ok"`
			Result []struct {
				UpdateID int `json:"update_id"`
				Message  *struct {
					Chat struct {
						ID int64 `json:"id"`
					} `json:"chat"`
					Text string `json:"text"`
				} `json:"message"`
			} `json:"result"`
		}
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil || !result.OK {
			time.Sleep(5 * time.Second)
			continue
		}
		for _, upd := range result.Result {
			offset = upd.UpdateID + 1
			if upd.Message == nil {
				continue
			}
			chatID := upd.Message.Chat.ID
			tg.chatMu.Lock()
			isNew := !tg.chats[chatID]
			if isNew {
				tg.chats[chatID] = true
				tg.saveChats()
			}
			tg.chatMu.Unlock()
			if isNew {
				log.Printf("✉️ Новый подписчик Telegram: %d", chatID)
				tg.sendTo(chatID, "✅ Оповещения о сплешах включены (MEXC, KuCoin, Gate, Bitget).")
			}
		}
	}
}

func (tg *TelegramBot) enqueue(text string) {
	select {
	case tg.queue <- text:
	default:
		// очередь переполнена — молча отбрасываем, чтобы не блокировать анализатор
	}
}

func (tg *TelegramBot) senderLoop() {
	for text := range tg.queue {
		tg.chatMu.Lock()
		ids := make([]int64, 0, len(tg.chats))
		for id := range tg.chats {
			ids = append(ids, id)
		}
		tg.chatMu.Unlock()

		for _, id := range ids {
			tg.sendTo(id, text)
			time.Sleep(50 * time.Millisecond) // лимиты Telegram: ~30 сообщений/сек
		}
	}
}

func (tg *TelegramBot) sendTo(chatID int64, text string) {
	form := url.Values{}
	form.Set("chat_id", strconv.FormatInt(chatID, 10))
	form.Set("text", text)
	form.Set("parse_mode", "HTML")
	form.Set("disable_web_page_preview", "true")

	resp, err := http.PostForm(tg.api("sendMessage"), form)
	if err != nil {
		log.Printf("⚠️ Telegram: ошибка отправки в %d: %v", chatID, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		time.Sleep(2 * time.Second)
	}
}

// ==========================================
// MEXC
// ==========================================

const (
	mexcWSEndpoint     = "wss://wbs-api.mexc.com/ws"
	mexcSymbolsAPI     = "https://api.mexc.com/api/v3/exchangeInfo"
	mexcMaxSubsPerConn = 30 // Лимит MEXC Spot: до 30 подписок на одно WS-соединение
	mexcDealChannelFmt = "spot@public.aggre.deals.v3.api.pb@100ms@%s"
)

func startMEXC() {
	symbols, err := fetchMEXCSymbols()
	if err != nil {
		log.Printf("❌ MEXC: не удалось получить список пар: %v", err)
		return
	}
	log.Printf("✅ MEXC: пар для мониторинга: %d", len(symbols))

	for i := 0; i < len(symbols); i += mexcMaxSubsPerConn {
		end := i + mexcMaxSubsPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		go mexcWorker(symbols[i:end])
		time.Sleep(500 * time.Millisecond)
	}
}

func fetchMEXCSymbols() ([]string, error) {
	resp, err := http.Get(mexcSymbolsAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResult struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			QuoteAsset string `json:"quoteAsset"`
		} `json:"symbols"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResult); err != nil {
		return nil, err
	}

	symbols := make([]string, 0, len(apiResult.Symbols))
	for _, s := range apiResult.Symbols {
		if s.Status != "1" { // "1" = ENABLED (торговля активна)
			continue
		}
		if !isUSDQuote(s.QuoteAsset) {
			continue
		}
		symbols = append(symbols, strings.ToUpper(s.Symbol))
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("пустой список пар")
	}
	return symbols, nil
}

func mexcWorker(symbols []string) {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(mexcWSEndpoint, nil)
		if err != nil {
			log.Printf("❌ MEXC: ошибка подключения группы %s: %v. Повтор через 5 сек...", symbols[0], err)
			time.Sleep(5 * time.Second)
			continue
		}

		params := make([]string, len(symbols))
		for i, symbol := range symbols {
			params[i] = fmt.Sprintf(mexcDealChannelFmt, symbol)
		}
		subMessage := map[string]interface{}{
			"method": "SUBSCRIPTION",
			"params": params,
		}
		if err := conn.WriteJSON(subMessage); err != nil {
			log.Printf("⚠️ MEXC: не удалось подписаться на группу %s: %v", symbols[0], err)
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		stopPing := startJSONPing(conn, 20*time.Second, map[string]interface{}{"method": "PING"})

		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ MEXC: соединение разорвано (группа %s). Переподключение...", symbols[0])
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
			for _, deal := range deals.GetDeals() {
				price, err1 := strconv.ParseFloat(deal.GetPrice(), 64)
				quantity, err2 := strconv.ParseFloat(deal.GetQuantity(), 64)
				if err1 != nil || err2 != nil {
					continue
				}
				recordTrade("MEXC", symbol, price, quantity)
			}
		}
		close(stopPing)
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}
}

// ==========================================
// KUCOIN
// ==========================================

const (
	kucoinSymbolsAPI     = "https://api.kucoin.com/api/v2/symbols"
	kucoinBulletAPI      = "https://api.kucoin.com/api/v1/bullet-public"
	kucoinSymbolsPerConn = 300 // до 300 топиков на соединение
	kucoinSymbolsPerSub  = 100 // до 100 символов в одном subscribe-сообщении
)

func startKuCoin() {
	symbols, err := fetchKuCoinSymbols()
	if err != nil {
		log.Printf("❌ KuCoin: не удалось получить список пар: %v", err)
		return
	}
	log.Printf("✅ KuCoin: пар для мониторинга: %d", len(symbols))

	for i := 0; i < len(symbols); i += kucoinSymbolsPerConn {
		end := i + kucoinSymbolsPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		go kucoinWorker(symbols[i:end])
		time.Sleep(500 * time.Millisecond)
	}
}

func fetchKuCoinSymbols() ([]string, error) {
	resp, err := http.Get(kucoinSymbolsAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResult struct {
		Data []struct {
			Symbol        string `json:"symbol"`
			QuoteCurrency string `json:"quoteCurrency"`
			EnableTrading bool   `json:"enableTrading"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResult); err != nil {
		return nil, err
	}

	symbols := make([]string, 0, len(apiResult.Data))
	for _, s := range apiResult.Data {
		if !s.EnableTrading || !isUSDQuote(s.QuoteCurrency) {
			continue
		}
		symbols = append(symbols, s.Symbol) // формат BTC-USDT
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("пустой список пар")
	}
	return symbols, nil
}

func kucoinGetWSEndpoint() (string, int, error) {
	resp, err := http.Post(kucoinBulletAPI, "application/json", nil)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			Token           string `json:"token"`
			InstanceServers []struct {
				Endpoint     string `json:"endpoint"`
				PingInterval int    `json:"pingInterval"`
			} `json:"instanceServers"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, err
	}
	if result.Data.Token == "" || len(result.Data.InstanceServers) == 0 {
		return "", 0, fmt.Errorf("пустой ответ bullet-public")
	}
	srv := result.Data.InstanceServers[0]
	wsURL := fmt.Sprintf("%s?token=%s&connectId=%d", srv.Endpoint, result.Data.Token, time.Now().UnixNano())
	pingMs := srv.PingInterval
	if pingMs <= 0 {
		pingMs = 18000
	}
	return wsURL, pingMs, nil
}

func kucoinWorker(symbols []string) {
	for {
		wsURL, pingMs, err := kucoinGetWSEndpoint()
		if err != nil {
			log.Printf("❌ KuCoin: ошибка bullet-public: %v. Повтор через 5 сек...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("❌ KuCoin: ошибка подключения группы %s: %v. Повтор через 5 сек...", symbols[0], err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Подписка на трейды пачками символов в одном топике /market/match
		subOK := true
		for i := 0; i < len(symbols); i += kucoinSymbolsPerSub {
			end := i + kucoinSymbolsPerSub
			if end > len(symbols) {
				end = len(symbols)
			}
			sub := map[string]interface{}{
				"id":             fmt.Sprintf("sub-%d", time.Now().UnixNano()),
				"type":           "subscribe",
				"topic":          "/market/match:" + strings.Join(symbols[i:end], ","),
				"privateChannel": false,
				"response":       false,
			}
			if err := conn.WriteJSON(sub); err != nil {
				subOK = false
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !subOK {
			log.Printf("⚠️ KuCoin: не удалось подписаться на группу %s", symbols[0])
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		stopPing := startJSONPing(conn, time.Duration(pingMs)*time.Millisecond, map[string]interface{}{
			"id":   strconv.FormatInt(time.Now().UnixNano(), 10),
			"type": "ping",
		})

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ KuCoin: соединение разорвано (группа %s). Переподключение...", symbols[0])
				break
			}

			var msg struct {
				Type  string `json:"type"`
				Topic string `json:"topic"`
				Data  struct {
					Symbol string `json:"symbol"`
					Price  string `json:"price"`
					Size   string `json:"size"`
				} `json:"data"`
			}
			if err := json.Unmarshal(message, &msg); err != nil {
				continue
			}
			if msg.Type != "message" || !strings.HasPrefix(msg.Topic, "/market/match") {
				continue
			}
			price, err1 := strconv.ParseFloat(msg.Data.Price, 64)
			quantity, err2 := strconv.ParseFloat(msg.Data.Size, 64)
			if err1 != nil || err2 != nil || msg.Data.Symbol == "" {
				continue
			}
			recordTrade("KUCOIN", msg.Data.Symbol, price, quantity)
		}
		close(stopPing)
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}
}

// ==========================================
// GATE.IO
// ==========================================

const (
	gateWSEndpoint     = "wss://api.gateio.ws/ws/v4/"
	gateSymbolsAPI     = "https://api.gateio.ws/api/v4/spot/currency_pairs"
	gateSymbolsPerConn = 200
	gateSymbolsPerSub  = 50
)

func startGate() {
	symbols, err := fetchGateSymbols()
	if err != nil {
		log.Printf("❌ Gate: не удалось получить список пар: %v", err)
		return
	}
	log.Printf("✅ Gate: пар для мониторинга: %d", len(symbols))

	for i := 0; i < len(symbols); i += gateSymbolsPerConn {
		end := i + gateSymbolsPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		go gateWorker(symbols[i:end])
		time.Sleep(500 * time.Millisecond)
	}
}

func fetchGateSymbols() ([]string, error) {
	resp, err := http.Get(gateSymbolsAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairs []struct {
		ID          string `json:"id"`
		Quote       string `json:"quote"`
		TradeStatus string `json:"trade_status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pairs); err != nil {
		return nil, err
	}

	symbols := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if p.TradeStatus != "tradable" || !isUSDQuote(p.Quote) {
			continue
		}
		symbols = append(symbols, p.ID) // формат BTC_USDT
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("пустой список пар")
	}
	return symbols, nil
}

func gateWorker(symbols []string) {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(gateWSEndpoint, nil)
		if err != nil {
			log.Printf("❌ Gate: ошибка подключения группы %s: %v. Повтор через 5 сек...", symbols[0], err)
			time.Sleep(5 * time.Second)
			continue
		}

		subOK := true
		for i := 0; i < len(symbols); i += gateSymbolsPerSub {
			end := i + gateSymbolsPerSub
			if end > len(symbols) {
				end = len(symbols)
			}
			sub := map[string]interface{}{
				"time":    time.Now().Unix(),
				"channel": "spot.trades",
				"event":   "subscribe",
				"payload": symbols[i:end],
			}
			if err := conn.WriteJSON(sub); err != nil {
				subOK = false
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !subOK {
			log.Printf("⚠️ Gate: не удалось подписаться на группу %s", symbols[0])
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		stopPing := startJSONPing(conn, 20*time.Second, map[string]interface{}{
			"time":    time.Now().Unix(),
			"channel": "spot.ping",
		})

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ Gate: соединение разорвано (группа %s). Переподключение...", symbols[0])
				break
			}

			var msg struct {
				Channel string `json:"channel"`
				Event   string `json:"event"`
				Result  struct {
					CurrencyPair string `json:"currency_pair"`
					Price        string `json:"price"`
					Amount       string `json:"amount"`
				} `json:"result"`
			}
			if err := json.Unmarshal(message, &msg); err != nil {
				continue
			}
			if msg.Channel != "spot.trades" || msg.Event != "update" {
				continue
			}
			price, err1 := strconv.ParseFloat(msg.Result.Price, 64)
			quantity, err2 := strconv.ParseFloat(msg.Result.Amount, 64)
			if err1 != nil || err2 != nil || msg.Result.CurrencyPair == "" {
				continue
			}
			recordTrade("GATE", msg.Result.CurrencyPair, price, quantity)
		}
		close(stopPing)
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}
}

// ==========================================
// BITGET
// ==========================================

const (
	bitgetWSEndpoint     = "wss://ws.bitget.com/v2/ws/public"
	bitgetSymbolsAPI     = "https://api.bitget.com/api/v2/spot/public/symbols"
	bitgetSymbolsPerConn = 200
	bitgetSymbolsPerSub  = 40 // лимит длины сообщения 4096 байт
)

func startBitget() {
	symbols, err := fetchBitgetSymbols()
	if err != nil {
		log.Printf("❌ Bitget: не удалось получить список пар: %v", err)
		return
	}
	log.Printf("✅ Bitget: пар для мониторинга: %d", len(symbols))

	for i := 0; i < len(symbols); i += bitgetSymbolsPerConn {
		end := i + bitgetSymbolsPerConn
		if end > len(symbols) {
			end = len(symbols)
		}
		go bitgetWorker(symbols[i:end])
		time.Sleep(500 * time.Millisecond)
	}
}

func fetchBitgetSymbols() ([]string, error) {
	resp, err := http.Get(bitgetSymbolsAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResult struct {
		Code string `json:"code"`
		Data []struct {
			Symbol    string `json:"symbol"`
			QuoteCoin string `json:"quoteCoin"`
			Status    string `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResult); err != nil {
		return nil, err
	}

	symbols := make([]string, 0, len(apiResult.Data))
	for _, s := range apiResult.Data {
		if s.Status != "online" || !isUSDQuote(s.QuoteCoin) {
			continue
		}
		symbols = append(symbols, s.Symbol) // формат BTCUSDT
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("пустой список пар (code=%s)", apiResult.Code)
	}
	return symbols, nil
}

func bitgetWorker(symbols []string) {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(bitgetWSEndpoint, nil)
		if err != nil {
			log.Printf("❌ Bitget: ошибка подключения группы %s: %v. Повтор через 5 сек...", symbols[0], err)
			time.Sleep(5 * time.Second)
			continue
		}

		subOK := true
		for i := 0; i < len(symbols); i += bitgetSymbolsPerSub {
			end := i + bitgetSymbolsPerSub
			if end > len(symbols) {
				end = len(symbols)
			}
			args := make([]map[string]string, 0, end-i)
			for _, s := range symbols[i:end] {
				args = append(args, map[string]string{
					"instType": "SPOT",
					"channel":  "trade",
					"instId":   s,
				})
			}
			sub := map[string]interface{}{"op": "subscribe", "args": args}
			if err := conn.WriteJSON(sub); err != nil {
				subOK = false
				break
			}
			time.Sleep(200 * time.Millisecond) // лимит Bitget: 10 сообщений/сек
		}
		if !subOK {
			log.Printf("⚠️ Bitget: не удалось подписаться на группу %s", symbols[0])
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// Bitget требует текстовый "ping" каждые ~30 сек
		stopPing := make(chan struct{})
		go func(c *websocket.Conn) {
			ticker := time.NewTicker(25 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopPing:
					return
				case <-ticker.C:
					if err := c.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
						return
					}
				}
			}
		}(conn)

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("⚠️ Bitget: соединение разорвано (группа %s). Переподключение...", symbols[0])
				break
			}
			if bytes.Equal(message, []byte("pong")) {
				continue
			}

			var msg struct {
				Action string `json:"action"`
				Arg    struct {
					Channel string `json:"channel"`
					InstID  string `json:"instId"`
				} `json:"arg"`
				Data []struct {
					Price string `json:"price"`
					Size  string `json:"size"`
				} `json:"data"`
			}
			if err := json.Unmarshal(message, &msg); err != nil {
				continue
			}
			if msg.Arg.Channel != "trade" || msg.Arg.InstID == "" {
				continue
			}
			// action "snapshot" — история при подписке, берем только "update"
			if msg.Action != "update" {
				continue
			}
			for _, d := range msg.Data {
				price, err1 := strconv.ParseFloat(d.Price, 64)
				quantity, err2 := strconv.ParseFloat(d.Size, 64)
				if err1 != nil || err2 != nil {
					continue
				}
				recordTrade("BITGET", msg.Arg.InstID, price, quantity)
			}
		}
		close(stopPing)
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}
}

// ==========================================
// ВСПОМОГАТЕЛЬНОЕ
// ==========================================

// startJSONPing шлет JSON-пинг с заданным интервалом, пока соединение живо.
// Возвращенный канал нужно закрыть при разрыве соединения.
func startJSONPing(conn *websocket.Conn, interval time.Duration, payload map[string]interface{}) chan struct{} {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := conn.WriteJSON(payload); err != nil {
					return
				}
			}
		}
	}()
	return stop
}

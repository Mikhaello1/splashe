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
// КОНФИГ (настройки фильтрации и поиска сплешей)
// ==========================================
// Все настройки читаются из config.json рядом с программой.
// Если файла нет — он создается автоматически со значениями по умолчанию.
const configFile = "config.json"

type ExchangeToggles struct {
	MEXC   bool `json:"mexc"`
	KuCoin bool `json:"kucoin"`
	Gate   bool `json:"gate"`
	Bitget bool `json:"bitget"`
}

type Config struct {
	IntervalSeconds  int             `json:"interval_seconds"`   // Интервал проверки (в секундах)
	ThresholdPercent float64         `json:"threshold_percent"`  // Минимальное движение цены (в %)
	MinVolumeUSD     float64         `json:"min_volume_usd"`     // Минимальный объем торгов внутри интервала (в USD)
	AlertCooldownSec int             `json:"alert_cooldown_sec"` // Не слать повторный алерт по паре чаще, чем раз в N секунд
	Quotes           []string        `json:"quotes"`             // Котировки-доллары: для них объем price*quantity считается как USD
	Exchanges        ExchangeToggles `json:"exchanges"`          // Какие биржи мониторить
}

var (
	cfg   Config
	cfgMu sync.RWMutex // защищает поля cfg, которые редактируются на лету через бота
)

func defaultConfig() Config {
	return Config{
		IntervalSeconds:  5,
		ThresholdPercent: 2,
		MinVolumeUSD:     2000.0,
		AlertCooldownSec: 30,
		Quotes:           []string{"USDT", "USDC"},
		Exchanges:        ExchangeToggles{MEXC: true, KuCoin: true, Gate: true, Bitget: true},
	}
}

// loadConfig читает config.json. Если файла нет — создает его с дефолтами.
func loadConfig() {
	cfg = defaultConfig()

	data, err := os.ReadFile(configFile)
	if err != nil {
		out, _ := json.MarshalIndent(cfg, "", "  ")
		if werr := os.WriteFile(configFile, out, 0644); werr != nil {
			log.Printf("⚠️ Не удалось создать %s: %v (использую настройки по умолчанию)", configFile, werr)
		} else {
			log.Printf("📝 Создан %s с настройками по умолчанию — отредактируйте под себя", configFile)
		}
		return
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("❌ Ошибка чтения %s: %v", configFile, err)
	}
	if len(cfg.Quotes) == 0 {
		cfg.Quotes = defaultConfig().Quotes
	}
	if cfg.IntervalSeconds < 1 {
		cfg.IntervalSeconds = 1
	}
	log.Printf("⚙️ Настройки загружены из %s: порог=%.2f%%, объем=$%.0f, интервал=%dс, кулдаун=%dс",
		configFile, cfg.ThresholdPercent, cfg.MinVolumeUSD, cfg.IntervalSeconds, cfg.AlertCooldownSec)
}

// saveConfig записывает текущий cfg обратно в config.json.
func saveConfig() {
	cfgMu.RLock()
	out, _ := json.MarshalIndent(cfg, "", "  ")
	cfgMu.RUnlock()
	if err := os.WriteFile(configFile, out, 0644); err != nil {
		log.Printf("⚠️ Не удалось сохранить %s: %v", configFile, err)
	}
}

// filterSnapshot возвращает согласованный снимок фильтров под RLock.
func filterSnapshot() (threshold, minVol float64, interval, cooldown int) {
	cfgMu.RLock()
	defer cfgMu.RUnlock()
	return cfg.ThresholdPercent, cfg.MinVolumeUSD, cfg.IntervalSeconds, cfg.AlertCooldownSec
}

// ==========================================
// .ENV (секреты и параметры окружения)
// ==========================================
// Файл .env лежит рядом с программой. Формат — KEY=VALUE построчно.
// Поддерживаемые ключи:
//   TELEGRAM_BOT_TOKEN  — токен бота от @BotFather
//   ADMIN_IDS           — Telegram ID админов через запятую (напр. 111,222)
//   ALERT_CHAT_ID       — ID форум-супергруппы, куда шлются алерты (напр. -1001234567890)
//   TOPIC_MEXC          — message_thread_id топика MEXC
//   TOPIC_KUCOIN        — message_thread_id топика KuCoin
//   TOPIC_GATE          — message_thread_id топика Gate
//   TOPIC_BITGET        — message_thread_id топика Bitget

type EnvConfig struct {
	Token  string
	Admins map[int64]bool
	ChatID int64
	Topics map[string]int64 // exchange -> message_thread_id
}

// loadEnvFile читает .env в map. Отсутствие файла — не ошибка (можно задать
// значения через настоящие переменные окружения).
func loadEnvFile(path string) map[string]string {
	m := make(map[string]string)
	data, err := os.ReadFile(path)
	if err != nil {
		return m
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		val = strings.Trim(val, `"'`) // снимаем обрамляющие кавычки
		m[key] = val
	}
	return m
}

func loadEnv() EnvConfig {
	file := loadEnvFile(".env")
	// Значение из .env приоритетнее; если его нет — берём из окружения ОС.
	get := func(key string) string {
		if v, ok := file[key]; ok && v != "" {
			return v
		}
		return os.Getenv(key)
	}

	env := EnvConfig{
		Admins: make(map[int64]bool),
		Topics: make(map[string]int64),
	}

	env.Token = get("TELEGRAM_BOT_TOKEN")
	if env.Token == "" {
		log.Fatal("❌ Не задан TELEGRAM_BOT_TOKEN (в .env или переменной окружения)")
	}

	for _, part := range strings.Split(get("ADMIN_IDS"), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			log.Printf("⚠️ .env: пропускаю некорректный ADMIN_IDS элемент %q", part)
			continue
		}
		env.Admins[id] = true
	}
	if len(env.Admins) == 0 {
		log.Println("⚠️ .env: ADMIN_IDS пуст — управлять ботом никто не сможет")
	}

	if v := get("ALERT_CHAT_ID"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			log.Fatalf("❌ .env: ALERT_CHAT_ID некорректен: %v", err)
		}
		env.ChatID = id
	} else {
		log.Println("⚠️ .env: ALERT_CHAT_ID не задан — алерты в канал отправляться не будут")
	}

	for exch, key := range map[string]string{
		"MEXC": "TOPIC_MEXC", "KUCOIN": "TOPIC_KUCOIN",
		"GATE": "TOPIC_GATE", "BITGET": "TOPIC_BITGET",
	} {
		if v := get(key); v != "" {
			id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.Printf("⚠️ .env: %s некорректен (%v) — топик пропущен", key, err)
				continue
			}
			env.Topics[exch] = id
		}
	}

	log.Printf("🔐 .env загружен: админов=%d, chat_id=%d, топиков=%d",
		len(env.Admins), env.ChatID, len(env.Topics))
	return env
}

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
	loadConfig()
	env := loadEnv()

	log.Println("🤖 Запуск Telegram-бота...")
	tg := newTelegramBot(env)
	go tg.pollUpdates()
	go tg.senderLoop()

	log.Println("📊 Запуск фонового анализатора рынка...")
	go startAnalyzer(tg)

	log.Println("🔌 Запуск воркеров бирж...")
	if cfg.Exchanges.MEXC {
		go startMEXC()
	}
	if cfg.Exchanges.KuCoin {
		go startKuCoin()
	}
	if cfg.Exchanges.Gate {
		go startGate()
	}
	if cfg.Exchanges.Bitget {
		go startBitget()
	}

	select {}
}

// ==========================================
// АНАЛИЗАТОР
// ==========================================

func startAnalyzer(tg *TelegramBot) {
	_, _, interval, _ := filterSnapshot()
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		threshold, minVol, curInterval, cooldown := filterSnapshot()
		// Интервал могли изменить через бота — перезапускаем тикер на лету.
		if curInterval != interval {
			interval = curInterval
			ticker.Reset(time.Duration(interval) * time.Second)
		}

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

			if absDiff >= threshold && totalVolume >= minVol && shouldAlert(key, cooldown) {
				link := exchangeLink(key)

				fmt.Println(strings.Repeat("-", 60))
				fmt.Printf("🔥 СПЛЕШ [%s]: %s\n", key.Exchange, key.Symbol)
				fmt.Printf("📊 Движение: %.2f%% за %d сек.\n", priceDiff, interval)
				fmt.Printf("💰 Объем за интервал: $%.2f\n", totalVolume)
				fmt.Printf("🔗 Ссылка: %s\n", link)
				fmt.Println(strings.Repeat("-", 60))

				arrow := "📈"
				if priceDiff < 0 {
					arrow = "📉"
				}
				text := fmt.Sprintf(
					"🔥 <code>%s</code> — %s\n%s Движение: <b>%+.2f%%</b> за %d сек.\n💰 Объем: $%.0f",
					key.Symbol, key.Exchange, arrow, priceDiff, interval, totalVolume,
				)
				tg.enqueue(alertMsg{Exchange: key.Exchange, Text: text, URL: link})
			}

			stats.Prices = nil
			stats.Volume = 0.0
		}
		dataMu.Unlock()
	}
}

func shouldAlert(key marketKey, cooldownSec int) bool {
	lastAlertMu.Lock()
	defer lastAlertMu.Unlock()
	if t, ok := lastAlert[key]; ok && time.Since(t) < time.Duration(cooldownSec)*time.Second {
		return false
	}
	lastAlert[key] = time.Now()
	return true
}

func exchangeLink(key marketKey) string {
	switch key.Exchange {
	case "MEXC":
		for _, quote := range cfg.Quotes {
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
	for _, q := range cfg.Quotes {
		if strings.EqualFold(quote, q) {
			return true
		}
	}
	return false
}

// ==========================================
// TELEGRAM BOT
// ==========================================

type alertMsg struct {
	Exchange string
	Text     string
	URL      string
}

type TelegramBot struct {
	token  string
	admins map[int64]bool
	chatID int64            // форум-супергруппа для алертов
	topics map[string]int64 // exchange -> message_thread_id
	queue  chan alertMsg

	editing map[int64]string // adminID -> редактируемое поле ("interval"/"threshold"/"volume")
	editMu  sync.Mutex
}

func newTelegramBot(env EnvConfig) *TelegramBot {
	return &TelegramBot{
		token:   env.Token,
		admins:  env.Admins,
		chatID:  env.ChatID,
		topics:  env.Topics,
		queue:   make(chan alertMsg, 1000),
		editing: make(map[int64]string),
	}
}

func (tg *TelegramBot) api(method string) string {
	return "https://api.telegram.org/bot" + tg.token + "/" + method
}

func (tg *TelegramBot) isAdmin(id int64) bool {
	return tg.admins[id]
}

// post отправляет форму методу Bot API. Тело ответа нам не нужно.
func (tg *TelegramBot) post(method string, form url.Values) {
	resp, err := http.PostForm(tg.api(method), form)
	if err != nil {
		log.Printf("⚠️ Telegram %s: %v", method, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		time.Sleep(2 * time.Second)
	}
}

func (tg *TelegramBot) sendText(chatID int64, text, replyMarkup string) {
	form := url.Values{}
	form.Set("chat_id", strconv.FormatInt(chatID, 10))
	form.Set("text", text)
	form.Set("parse_mode", "HTML")
	form.Set("disable_web_page_preview", "true")
	if replyMarkup != "" {
		form.Set("reply_markup", replyMarkup)
	}
	tg.post("sendMessage", form)
}

func (tg *TelegramBot) answerCallback(callbackID, text string) {
	form := url.Values{}
	form.Set("callback_query_id", callbackID)
	if text != "" {
		form.Set("text", text)
	}
	tg.post("answerCallbackQuery", form)
}

// enqueue кладёт алерт в очередь на отправку в канал.
func (tg *TelegramBot) enqueue(msg alertMsg) {
	select {
	case tg.queue <- msg:
	default:
		// очередь переполнена — молча отбрасываем, чтобы не блокировать анализатор
	}
}

func (tg *TelegramBot) senderLoop() {
	for msg := range tg.queue {
		if tg.chatID == 0 {
			continue // ALERT_CHAT_ID не задан — слать некуда
		}
		tg.sendAlert(tg.topics[msg.Exchange], msg.Text, msg.URL)
		time.Sleep(50 * time.Millisecond) // лимиты Telegram: ~30 сообщений/сек
	}
}

// sendAlert шлёт алерт в нужный топик канала с кнопкой «Открыть график».
func (tg *TelegramBot) sendAlert(threadID int64, text, chartURL string) {
	markup, _ := json.Marshal(map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "📊 Открыть график", "url": chartURL}},
		},
	})

	form := url.Values{}
	form.Set("chat_id", strconv.FormatInt(tg.chatID, 10))
	if threadID != 0 {
		form.Set("message_thread_id", strconv.FormatInt(threadID, 10))
	}
	form.Set("text", text)
	form.Set("parse_mode", "HTML")
	form.Set("disable_web_page_preview", "true")
	form.Set("reply_markup", string(markup))
	tg.post("sendMessage", form)
}

// settingsMenu возвращает текст и inline-клавиатуру меню настроек.
func (tg *TelegramBot) settingsMenu() (string, string) {
	threshold, minVol, interval, cooldown := filterSnapshot()
	text := fmt.Sprintf(
		"⚙️ <b>Настройки фильтров</b>\n\n"+
			"⏱ Интервал: <b>%d сек</b>\n"+
			"📈 Порог движения: <b>%.2f%%</b>\n"+
			"💰 Мин. объём: <b>%.0f USDT</b>\n"+
			"🔁 Кулдаун: <b>%d сек</b>\n\n"+
			"Выберите, что изменить:",
		interval, threshold, minVol, cooldown,
	)
	markup, _ := json.Marshal(map[string]interface{}{
		"inline_keyboard": [][]map[string]string{
			{{"text": "⏱ Интервал", "callback_data": "edit_interval"}},
			{{"text": "📈 % движения", "callback_data": "edit_threshold"}},
			{{"text": "💰 Объём USDT", "callback_data": "edit_volume"}},
		},
	})
	return text, string(markup)
}

// pollUpdates слушает апдейты. Реагирует только на админов из .env.
func (tg *TelegramBot) pollUpdates() {
	offset := 0
	client := &http.Client{Timeout: 40 * time.Second}
	for {
		u := fmt.Sprintf("%s?timeout=30&offset=%d&allowed_updates=[\"message\",\"callback_query\"]",
			tg.api("getUpdates"), offset)
		resp, err := client.Get(u)
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
					From struct {
						ID int64 `json:"id"`
					} `json:"from"`
					Text string `json:"text"`
				} `json:"message"`
				CallbackQuery *struct {
					ID   string `json:"id"`
					Data string `json:"data"`
					From struct {
						ID int64 `json:"id"`
					} `json:"from"`
					Message struct {
						Chat struct {
							ID int64 `json:"id"`
						} `json:"chat"`
					} `json:"message"`
				} `json:"callback_query"`
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
			switch {
			case upd.CallbackQuery != nil:
				tg.handleCallback(upd.CallbackQuery.ID, upd.CallbackQuery.From.ID,
					upd.CallbackQuery.Message.Chat.ID, upd.CallbackQuery.Data)
			case upd.Message != nil:
				tg.handleMessage(upd.Message.From.ID, upd.Message.Chat.ID, upd.Message.Text)
			}
		}
	}
}

func (tg *TelegramBot) handleMessage(userID, chatID int64, text string) {
	if !tg.isAdmin(userID) {
		return // бот отвечает только админам
	}
	text = strings.TrimSpace(text)

	// Если админ в режиме ввода значения — принимаем введённое число.
	tg.editMu.Lock()
	field, editing := tg.editing[userID]
	tg.editMu.Unlock()
	if editing {
		tg.applyEdit(userID, chatID, field, text)
		return
	}

	switch {
	case strings.HasPrefix(text, "/start"), strings.HasPrefix(text, "/settings"):
		menu, markup := tg.settingsMenu()
		tg.sendText(chatID, menu, markup)
	default:
		tg.sendText(chatID, "Команды: /settings — настройки фильтров.", "")
	}
}

func (tg *TelegramBot) handleCallback(callbackID string, userID, chatID int64, data string) {
	if !tg.isAdmin(userID) {
		tg.answerCallback(callbackID, "Нет доступа")
		return
	}
	var field, prompt string
	switch data {
	case "edit_interval":
		field, prompt = "interval", "⏱ Введите интервал в секундах (целое число ≥ 1):"
	case "edit_threshold":
		field, prompt = "threshold", "📈 Введите порог движения в % (например 0.5):"
	case "edit_volume":
		field, prompt = "volume", "💰 Введите минимальный объём в USDT (например 500):"
	default:
		tg.answerCallback(callbackID, "")
		return
	}
	tg.editMu.Lock()
	tg.editing[userID] = field
	tg.editMu.Unlock()
	tg.answerCallback(callbackID, "")
	tg.sendText(chatID, prompt, "")
}

// applyEdit применяет введённое админом значение к соответствующему полю cfg.
func (tg *TelegramBot) applyEdit(userID, chatID int64, field, text string) {
	num := strings.Replace(strings.TrimSpace(text), ",", ".", 1)

	switch field {
	case "interval":
		v, err := strconv.Atoi(num)
		if err != nil || v < 1 {
			tg.sendText(chatID, "❌ Нужно целое число ≥ 1. Попробуйте ещё раз или /settings для отмены.", "")
			return
		}
		cfgMu.Lock()
		cfg.IntervalSeconds = v
		cfgMu.Unlock()
	case "threshold":
		v, err := strconv.ParseFloat(num, 64)
		if err != nil || v <= 0 {
			tg.sendText(chatID, "❌ Нужно положительное число (например 0.5). Попробуйте ещё раз.", "")
			return
		}
		cfgMu.Lock()
		cfg.ThresholdPercent = v
		cfgMu.Unlock()
	case "volume":
		v, err := strconv.ParseFloat(num, 64)
		if err != nil || v <= 0 {
			tg.sendText(chatID, "❌ Нужно положительное число (например 500). Попробуйте ещё раз.", "")
			return
		}
		cfgMu.Lock()
		cfg.MinVolumeUSD = v
		cfgMu.Unlock()
	default:
		tg.clearEdit(userID)
		return
	}

	tg.clearEdit(userID)
	saveConfig()
	menu, markup := tg.settingsMenu()
	tg.sendText(chatID, "✅ Сохранено.\n\n"+menu, markup)
}

func (tg *TelegramBot) clearEdit(userID int64) {
	tg.editMu.Lock()
	delete(tg.editing, userID)
	tg.editMu.Unlock()
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

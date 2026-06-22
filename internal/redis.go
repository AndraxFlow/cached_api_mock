package redis

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client
var ctx = context.Background()

func generateRequestID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(bytes)
}

func JSONLoggerMiddleware() gin.HandlerFunc {
	logger := func(c *gin.Context) {
		startTime := time.Now()

		// 1. Генерируем или берем из апстрима Request ID
		requestID := c.GetHeader("X-Request-Id")
		if requestID == "" {
			requestID = generateRequestID()
		}

		c.Set("request_id", requestID)
		c.Header("X-Request-Id", requestID)

		c.Next()

		// 2. Собираем метрики после выполнения запроса
		latency := time.Since(startTime)
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()
		method := c.Request.Method

		if method == "" {
			method = c.Request.Method
		}
		path := c.Request.URL.Path

		// Формируем структурированный лог по результатам запроса
		attributes := []slog.Attr{
			slog.String("request_id", requestID),
			slog.Int("status", statusCode),
			slog.String("method", method),
			slog.String("path", path),
			slog.String("ip", clientIP),
			slog.Duration("duration", latency),
		}

		// Выбираем уровень лога в зависимости от HTTP-статуса
		if statusCode >= 500 {
			slog.LogAttrs(c.Request.Context(), slog.LevelError, "HTTP Request Failed", attributes...)
		} else if statusCode >= 400 {
			slog.LogAttrs(c.Request.Context(), slog.LevelWarn, "HTTP Request Client Error", attributes...)
		} else {
			slog.LogAttrs(c.Request.Context(), slog.LevelInfo, "HTTP Request Success", attributes...)
		}

	}

	return logger
}

func InitializeRedis() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, // Минимальный уровень для вывода (DEBUG, INFO, WARN, ERROR)
	}))
	slog.SetDefault(logger)

	slog.Info("Starting weather API service...")

	rdb = redis.NewClient(&redis.Options{
		Addr:         "redis_db:6379",
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Отключаем дефолтный консольный логгер Gin, так как у нас свой JSON-логгер
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery()) // Защита от паники (panic) внутри кода

	// Подключаем наш кастомный логгер
	r.Use(JSONLoggerMiddleware())

	r.GET("/weather/:city", getWeather)
	r.GET("/user/history", getHistory)
	r.GET("/top-cities", getTopCities)

	r.Run(":8000")

	slog.Info("Server is running on port :8000")
	if err := r.Run(":8000"); err != nil {
		slog.Error("Failed to start server", "error", err.Error())
	}
}

func getWeather(c *gin.Context) {
	reqID := c.GetString("request_id")
	ctx := c.Request.Context()

	city := strings.ToLower(c.Param("city"))
	userID := c.GetHeader("User-Id")

	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing User-Id header"})
		return
	}

	// 1. RATE LIMITER

	rateKey := fmt.Sprintf("rate:%s", userID)

	// 1. Создаем транзакционный конвейер
	pipe := rdb.TxPipeline()

	// 2. Нанизываем команды на конвейер
	incrCmd := pipe.Incr(c.Request.Context(), rateKey)
	pipe.Expire(c.Request.Context(), rateKey, time.Minute)

	// 3. Выполняем весь пайплайн за один сетевой запрос
	_, err := pipe.Exec(ctx)
	if err != nil {
		// КРИТИЧЕСКАЯ ОШИБКА: пишем ERROR, прикрепляем трейс и ошибку
		slog.Error("Redis transaction failed in rate limiter",
			"request_id", reqID, "user_id", userID, "error", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	// 4. Достаем результат инкремента из выполненной команды
	request := incrCmd.Val()

	if incrCmd.Val() > 5 {
		// ПРЕДУПРЕЖДЕНИЕ: пользователь превысил лимит. Это WARN.
		slog.Warn("Rate limit exceeded", "request_id", reqID, "user_id", userID, "count", incrCmd.Val())
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "Too many requests"})
		return
	}

	if err == nil && request == 1 {
		// Устанавливаем время жизни ключа только в момент его создания (когда счётчик равен 1)
		rdb.Expire(ctx, rateKey, time.Minute)
	}

	// 2. КЭШИРОВАНИЕ ДАННЫХ

	cacheKey := fmt.Sprintf("weather:%s", city)
	cachedWeather, err := rdb.Get(ctx, cacheKey).Result()

	if err == nil {
		// ШТАТНАЯ СИТУАЦИЯ: Данные из кэша. Пишем INFO.
		slog.Info("Cache HIT", "request_id", reqID, "city", city)
		c.JSON(http.StatusOK, gin.H{"source": "cache", "data": cachedWeather})
		return
	}

	if err != redis.Nil {
		// Ошибка Redis (не считая отсутствия ключа) — это плохо, но не смертельно для бизнес-логики. Log & Fallback.
		slog.Error("Redis GET failed", "request_id", reqID, "key", cacheKey, "error", err.Error())
	}

	// Сюда доходим только при Cache MISS
	slog.Info("Cache MISS. Fetching from remote API...", "request_id", reqID, "city", city)

	// Эмуляция "тяжёлого" запроса к внешнему источнику (2 секунды)
	time.Sleep(2 * time.Second)
	fakeTemp := fmt.Sprintf("%d°C", rand.Intn(45)-10) // от -10 до +35

	// Записываем в кэш асинхронно, чтобы не тормозить клиента
	go func(id string) {
		// Для горутины создаем чистый бэкграунд контекст, так как контекст запроса c.Request.Context() умрет после ответа клиенту
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := rdb.Set(bgCtx, cacheKey, fakeTemp, 30*time.Second).Err(); err != nil {
			slog.Error("Failed to write to cache", "request_id", id, "key", cacheKey, "error", err.Error())
		} else {
			slog.Info("Cache successfully populated", "request_id", id, "key", cacheKey)
		}
	}(reqID)

	c.JSON(http.StatusOK, gin.H{"city": city, "temperature": fakeTemp, "source": "api"})
}

func getHistory(c *gin.Context) {
	userID := c.GetHeader("User-Id")

	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing User-Id header"})
		return
	}

	// Получаем историю поиска из Redis
	history, err := rdb.LRange(ctx, fmt.Sprintf("history:%s", userID), 0, -1).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Redis error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"history": history})
}

func getTopCities(c *gin.Context) {
	// Получаем топ-3 популярных городов из Redis
	topCities, err := rdb.ZRevRangeWithScores(ctx, "popular_cities", 0, 2).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Redis error"})
		return
	}

	type CityScore struct {
		City  string `json:"city"`
		Score int    `json:"score"`
	}
	var results []CityScore
	for _, z := range topCities {
		fmt.Print(z)
		results = append(results, CityScore{
			City:  z.Member.(string),
			Score: int(z.Score),
		})
	}

	c.JSON(http.StatusOK, gin.H{"top_cities": results})
}

package redis

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client
var ctx = context.Background()

func InitializeRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "redis_db:6379",
	})

	r := gin.Default()

	r.GET("/weather/:city", getWeather)
	r.GET("/user/history", getHistory)
	r.GET("/top-cities", getTopCities)

	r.Run(":8000")
}

func getWeather(c *gin.Context) {
	city := strings.ToLower(c.Param("city"))
	userID := c.GetHeader("User-Id")

	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing User-Id header"})
		return
	}

	// 1. RATE LIMITER

	rateKey := fmt.Sprintf("rate:%s", userID)
	request, err := rdb.Incr(ctx, rateKey).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Redis error"})
		return
	}

	if request > 5 {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "Too many requests. Please try again later."})
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
		c.JSON(http.StatusOK, gin.H{"source": "cache", "data": cachedWeather})
		return
	}

	// Эмуляция "тяжёлого" запроса к внешнему источнику (2 секунды)
	time.Sleep(2 * time.Second)
	fakeTemp := fmt.Sprintf("%d°C", rand.Intn(45)-10) // от -10 до +35

	// Сохраняем в кэш со временем жизни (TTL) 30 секунд
	rdb.Set(ctx, cacheKey, fakeTemp, 30*time.Second)

	// 3. ИСТОРИЯ ПОИСКА
	historyKey := fmt.Sprintf("history:%s", userID)
	rdb.LPush(ctx, historyKey, city)
	rdb.LTrim(ctx, historyKey, 0, 4)

	// 4. ТОП ПОПУЛЯРНЫХ ГОРОДОВ
	rdb.ZIncrBy(ctx, "popular_cities", 1, city)

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

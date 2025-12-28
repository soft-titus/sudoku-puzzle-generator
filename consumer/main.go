package main

import log "github.com/sirupsen/logrus"

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	maxRetries           int
	baseBackoffSeconds   int
	backoffMultiplier    int
	maxBackoffSeconds    int
	kafkaBroker          string
	puzzleTopic          string
	puzzleDLQTopic       string
	retriableTopic       string
	imageTopic           string
	consumerGroup        string
	kafkaPoolingInterval time.Duration

	mongoClient     *mongo.Client
	mongoCollection *mongo.Collection
)

func init() {
	var err error

	levelStr := os.Getenv("LOG_LEVEL")
	level, err := log.ParseLevel(levelStr)
	if err != nil {
		level = log.InfoLevel
	}
	log.SetLevel(level)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	log.SetOutput(os.Stdout)

	maxRetries, err = strconv.Atoi(os.Getenv("TASK_MAX_RETRIES"))
	if err != nil {
		log.Fatalf("Invalid TASK_MAX_RETRIES: %v", err)
	}
	baseBackoffSeconds, err = strconv.Atoi(os.Getenv("TASK_BASE_BACKOFF_SECONDS"))
	if err != nil {
		log.Fatalf("Invalid TASK_BASE_BACKOFF_SECONDS: %v", err)
	}
	backoffMultiplier, err = strconv.Atoi(os.Getenv("TASK_BASE_BACKOFF_MULTIPLIER"))
	if err != nil {
		log.Fatalf("Invalid TASK_BASE_BACKOFF_MULTIPLIER: %v", err)
	}
	maxBackoffSeconds, err = strconv.Atoi(os.Getenv("TASK_MAX_BACKOFF_SECONDS"))
	if err != nil {
		log.Fatalf("Invalid TASK_MAX_BACKOFF_SECONDS: %v", err)
	}

	kafkaBroker = fmt.Sprintf("%s:%s", os.Getenv("KAFKA_BROKER_HOST"), os.Getenv("KAFKA_BROKER_PORT"))
	puzzleTopic = os.Getenv("KAFKA_PUZZLE_TOPIC")
	puzzleDLQTopic = os.Getenv("KAFKA_PUZZLE_DLQ_TOPIC")
	retriableTopic = os.Getenv("KAFKA_RETRIABLE_TOPIC")
	imageTopic = os.Getenv("KAFKA_IMAGE_TOPIC")
	consumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP_NAME")

	intervalSeconds, err := strconv.Atoi(os.Getenv("KAFKA_POOLING_INTERVAL_SECONDS"))
	if err != nil {
		log.Infof("Invalid KAFKA_POOLING_INTERVAL_SECONDS, defaulting to 15s: %v", err)
		intervalSeconds = 15
	}
	kafkaPoolingInterval = time.Duration(intervalSeconds) * time.Second

	// Initialize MongoDB
	mongoURI := fmt.Sprintf(
		"mongodb://%s:%s@%s:%s/%s",
		os.Getenv("MONGO_USER"),
		os.Getenv("MONGO_PASSWORD"),
		os.Getenv("MONGO_HOST"),
		os.Getenv("MONGO_PORT"),
		os.Getenv("MONGO_DB"),
	)

	mongoOptions := os.Getenv("MONGO_OPTIONS")
	if mongoOptions != "" {
		mongoURI = fmt.Sprintf("%s?%s", mongoURI, mongoOptions)
	}
	mongoClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	mongoCollection = mongoClient.Database(os.Getenv("MONGO_DB")).Collection(os.Getenv("MONGO_COLLECTION_NAME"))
}

func main() {
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			log.Errorf("failed to disconnect mongo client: %v", err)
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		Topic:          puzzleTopic,
		GroupID:        consumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // disable auto-commit
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Errorf("failed to close kafka reader: %v", err)
		}
	}()

	wImage := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   imageTopic,
	})
	defer func() {
		if err := wImage.Close(); err != nil {
			log.Errorf("failed to close Image kafka writer: %v", err)
		}
	}()

	wRetry := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   retriableTopic,
	})
	defer func() {
		if err := wRetry.Close(); err != nil {
			log.Errorf("failed to close Retry kafka writer: %v", err)
		}
	}()

	wDLQ := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   puzzleDLQTopic,
	})
	defer func() {
		if err := wDLQ.Close(); err != nil {
			log.Errorf("failed to close DLQ kafka writer: %v", err)
		}
	}()

	log.Info("Starting sudoku puzzle generator consumer...")

	for {
		ctx, cancel := context.WithTimeout(context.Background(), kafkaPoolingInterval)
		msg, err := r.FetchMessage(ctx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			log.Infof("Error fetching message: %v", err)
			continue
		}

		log.Infof("Received message: %s", string(msg.Value))

		// Quick heartbeat check
		var raw map[string]interface{}
		if err := json.Unmarshal(msg.Value, &raw); err == nil {
			if msgType, ok := raw["type"].(string); ok && msgType == "heartbeat" {
				log.Infof("Heartbeat message received, skipping processing for partition %d offset %d", msg.Partition, msg.Offset)
				commitMessage(context.Background(), r, msg)
				continue
			}
		}

		// Parse payload as JSON
		var payload map[string]interface{}
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			pushToDLQ(context.Background(), msg, wDLQ, r, "invalid JSON payload")
			continue
		}

		// Check puzzleId exists
		puzzleID, ok := payload["puzzleId"].(string)
		if !ok || puzzleID == "" {
			pushToDLQ(context.Background(), msg, wDLQ, r, "missing puzzleId on payload")
			continue
		}

		// Check if puzzle exists and validate fields
		var puzzleDoc struct {
			PuzzleSize int    `bson:"puzzleSize"`
			Level      string `bson:"level"`
			Status     string `bson:"status"`
		}

		err = mongoCollection.FindOne(context.Background(), bson.M{"puzzleId": puzzleID}).Decode(&puzzleDoc)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				pushToDLQ(context.Background(), msg, wDLQ, r, "puzzleId does not exist in MongoDB")
			} else {
				log.Info("Failed to query MongoDB:", err)
				handleRetry(context.Background(), msg, wRetry, r)
			}
			continue
		}

		// Extract retry count
		retryCount := 0
		for _, h := range msg.Headers {
			if h.Key == "retry-count" {
				retryCount, _ = strconv.Atoi(string(h.Value))
				break
			}
		}

		// DLQ if max retries reached
		if retryCount >= maxRetries {
			pushToDLQ(context.Background(), msg, wDLQ, r, fmt.Sprintf("retry exceeded (%d)", retryCount))
			continue
		}

		// Validate puzzleSize
		validSizes := map[int]bool{4: true, 9: true, 16: true}
		if !validSizes[puzzleDoc.PuzzleSize] {
			pushToDLQ(context.Background(), msg, wDLQ, r, fmt.Sprintf("invalid puzzleSize: %d", puzzleDoc.PuzzleSize))
			continue
		}

		// Validate level
		validLevels := map[string]bool{"EASY": true, "MEDIUM": true, "HARD": true}
		if !validLevels[puzzleDoc.Level] {
			pushToDLQ(context.Background(), msg, wDLQ, r, fmt.Sprintf("invalid level: %s", puzzleDoc.Level))
			continue
		}

		// Validate status
		if puzzleDoc.Status != "GENERATING_PUZZLE" {
			pushToDLQ(context.Background(), msg, wDLQ, r, fmt.Sprintf("invalid status: %s", puzzleDoc.Status))
			continue
		}

		// Process message
		if err := processPuzzle(rng, context.Background(), puzzleID, puzzleDoc.PuzzleSize, puzzleDoc.Level); err != nil {
			log.Info("Processing failed:", err)
			handleRetry(context.Background(), msg, wRetry, r)
		} else {
			if err := wImage.WriteMessages(context.Background(), kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
			}); err != nil {
				log.Infof("Failed to write to topic %s: %v", imageTopic, err)
			} else {
				log.Infof("Message processed successfully and sent to topic %s", imageTopic)
				commitMessage(context.Background(), r, msg)
			}
		}
	}
}

func handleRetry(ctx context.Context, msg kafka.Message, wRetry *kafka.Writer, r *kafka.Reader) {
	retryCount := 0
	for _, h := range msg.Headers {
		if h.Key == "retry-count" {
			retryCount, _ = strconv.Atoi(string(h.Value))
			break
		}
	}

	retryCount++
	backoff := time.Duration(baseBackoffSeconds) * time.Second * time.Duration(int(math.Pow(float64(backoffMultiplier), float64(retryCount-1))))
	if backoff > time.Duration(maxBackoffSeconds)*time.Second {
		backoff = time.Duration(maxBackoffSeconds) * time.Second
	}
	processAfter := time.Now().Add(backoff)

	newHeaders := []kafka.Header{
		{Key: "retry-count", Value: []byte(strconv.Itoa(retryCount))},
		{Key: "process-after", Value: []byte(processAfter.Format(time.RFC3339))},
		{Key: "origin-topic", Value: []byte(puzzleTopic)},
	}

	if err := wRetry.WriteMessages(ctx, kafka.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: newHeaders,
	}); err != nil {
		log.Infof("Failed to write to topic %s: %v", retriableTopic, err)
	} else {
		log.Infof("Message sent to topic %s with retry-count=%d, process-after=%s",
			retriableTopic, retryCount, processAfter.Format(time.RFC3339))

		commitMessage(ctx, r, msg)
	}
}

func pushToDLQ(ctx context.Context, msg kafka.Message, wDLQ *kafka.Writer, r *kafka.Reader, reason string) {
	failedAt := time.Now().UTC()

	// Parse payload or wrap invalid payload
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		payload = map[string]interface{}{
			"originalPayload": string(msg.Value),
		}
	}

	puzzleID, _ := payload["puzzleId"].(string)
	markPuzzleFailed(ctx, puzzleID, reason, failedAt)

	// Add failedReason and failedAt
	payload["failedReason"] = reason
	payload["failedAt"] = failedAt.Format(time.RFC3339)

	// Marshal payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Info("Failed to marshal payload for DLQ:", err)
		return
	}

	// Filter out retry headers
	var newHeaders []kafka.Header
	for _, h := range msg.Headers {
		if h.Key != "retry-count" && h.Key != "process-after" && h.Key != "origin-topic" {
			newHeaders = append(newHeaders, h)
		}
	}

	if err := wDLQ.WriteMessages(ctx, kafka.Message{
		Key:     msg.Key,
		Value:   payloadBytes,
		Headers: newHeaders,
	}); err != nil {
		log.Infof("Failed to write to topic %s: %v", puzzleDLQTopic, err)
	} else {
		log.Infof("Message sent to topic %s: %s", puzzleDLQTopic, reason)

		commitMessage(ctx, r, msg)
	}
}

func commitMessage(ctx context.Context, r *kafka.Reader, msg kafka.Message) {
	if err := r.CommitMessages(ctx, msg); err != nil {
		log.Infof("Failed to commit message: %v", err)
	}
}

func markPuzzleFailed(ctx context.Context, puzzleID string, reason string, failedAt time.Time) {
	if puzzleID == "" {
		return
	}

	update := bson.M{
		"$set": bson.M{
			"status":       "FAILED",
			"failedAt":     failedAt,
			"failedReason": reason,
			"updatedAt":    time.Now().UTC(),
		},
	}

	res, err := mongoCollection.UpdateOne(
		ctx,
		bson.M{"puzzleId": puzzleID},
		update,
	)

	if err != nil {
		log.Warnf("Failed to update puzzle %s as FAILED: %v", puzzleID, err)
		return
	}

	if res.MatchedCount == 0 {
		log.Infof("No MongoDB document found for puzzleId=%s, skipping FAILED update", puzzleID)
	}
}

func processPuzzle(rng *rand.Rand, ctx context.Context, puzzleId string, puzzleSize int, level string) error {
	solution := generateSudokuSolution(rng, puzzleSize)
	puzzle := generatePuzzleWithUniqueSolution(rng, solution, puzzleSize, level)

	log.Debug("Generated sudoku solution:")
	printSudoku(solution)
	printSudoku(puzzle)

	// Save generated solution to MongoDB
	_, err := mongoCollection.UpdateOne(
		ctx,
		bson.M{"puzzleId": puzzleId},
		bson.M{"$set": bson.M{"solution": solution, "puzzle": puzzle, "status": "GENERATING_IMAGE", "updatedAt": time.Now().UTC()}},
	)
	if err != nil {
		return fmt.Errorf("failed to update puzzle in MongoDB: %v", err)
	}

	return nil
}

func generateSudokuSolution(rng *rand.Rand, size int) [][]int {
	const maxAttempts = 100

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		board := makeBoard(size)
		if fillBoard(rng, board, size) {
			return board
		}
		log.Infof("Solution generation failed, retrying (%d/%d)", attempt, maxAttempts)
	}

	panic("failed to generate full sudoku solution")
}

func makeBoard(size int) [][]int {
	board := make([][]int, size)
	for i := range board {
		board[i] = make([]int, size)
	}
	return board
}

func fillBoard(rng *rand.Rand, board [][]int, size int) bool {
	// Find the empty cell with the fewest valid candidates
	row, col, candidates, deadEnd := findMRVCell(board, size)

	if deadEnd {
		return false
	}

	if row == -1 {
		// No empty cells left - solved
		return true
	}

	// Try candidates (randomized to keep board variety)
	rng.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	for _, val := range candidates {
		board[row][col] = val
		if fillBoard(rng, board, size) {
			return true
		}
		board[row][col] = 0 // backtrack
	}

	return false
}

func findMRVCell(board [][]int, size int) (row, col int, candidates []int, deadEnd bool) {
	minCandidates := size + 1
	bestRow, bestCol := -1, -1
	var bestCandidates []int

	for r := 0; r < size; r++ {
		for c := 0; c < size; c++ {
			if board[r][c] != 0 {
				continue
			}

			cands := getCandidates(board, size, r, c)
			if len(cands) == 0 {
				return r, c, nil, true // DEAD END
			}

			if len(cands) < minCandidates {
				minCandidates = len(cands)
				bestRow = r
				bestCol = c
				bestCandidates = cands
				if minCandidates == 1 {
					return bestRow, bestCol, bestCandidates, false
				}
			}
		}
	}

	if bestRow == -1 {
		return -1, -1, nil, false // SOLVED
	}

	return bestRow, bestCol, bestCandidates, false
}

func getCandidates(board [][]int, size, row, col int) []int {
	candidates := make([]int, 0, size)
	for num := 1; num <= size; num++ {
		if isValid(board, size, row, col, num) {
			candidates = append(candidates, num)
		}
	}
	return candidates
}

func isValid(board [][]int, size, row, col, num int) bool {
	block := intSqrt(size)

	for i := 0; i < size; i++ {
		if board[row][i] == num || board[i][col] == num {
			return false
		}
	}

	startRow := row / block * block
	startCol := col / block * block

	for r := 0; r < block; r++ {
		for c := 0; c < block; c++ {
			if board[startRow+r][startCol+c] == num {
				return false
			}
		}
	}

	return true
}

func intSqrt(n int) int {
	for i := 1; i*i <= n; i++ {
		if i*i == n {
			return i
		}
	}
	panic("invalid sudoku size")
}

func removalAttempts(size int, level string) int {
	switch level {
	case "EASY":
		switch size {
		case 4:
			return 10
		case 9:
			return 46
		case 16:
			return 120
		default:
			panic("unknown size")
		}
	case "MEDIUM":
		switch size {
		case 4:
			return 11
		case 9:
			return 52
		case 16:
			return 135
		default:
			panic("unknown size")
		}
	case "HARD":
		switch size {
		case 4:
			return 12
		case 9:
			return 58
		case 16:
			return 150
		default:
			panic("unknown size")
		}
	default:
		panic("unknown level")
	}
}

func minRemovalRequired(size int, level string) int {
	switch level {
	case "EASY":
		switch size {
		case 4:
			return 8
		case 9:
			return 42
		case 16:
			return 115
		default:
			panic("unknown size")
		}
	case "MEDIUM":
		switch size {
		case 4:
			return 9
		case 9:
			return 46
		case 16:
			return 125
		default:
			panic("unknown size")
		}
	case "HARD":
		switch size {
		case 4:
			return 10
		case 9:
			return 50
		case 16:
			return 135
		default:
			panic("unknown size")
		}
	default:
		panic("unknown level")
	}
}

func generatePuzzleWithUniqueSolution(rng *rand.Rand,
	solution [][]int,
	size int,
	level string,
) [][]int {

	const maxRetries = 20

	minRemoved := minRemovalRequired(size, level)
	attempts := removalAttempts(size, level)

	var puzzle [][]int // define outside

	for retry := 1; retry <= maxRetries; retry++ {
		puzzle = deepCopy(solution)

		type cell struct{ r, c int }
		var cells []cell
		for r := 0; r < size; r++ {
			for c := 0; c < size; c++ {
				cells = append(cells, cell{r, c})
			}
		}

		rng.Shuffle(len(cells), func(i, j int) {
			cells[i], cells[j] = cells[j], cells[i]
		})

		for i := 0; i < attempts && i < len(cells); i++ {
			r, c := cells[i].r, cells[i].c

			if puzzle[r][c] == 0 {
				continue
			}

			backup := puzzle[r][c]
			puzzle[r][c] = 0

			testBoard := deepCopy(puzzle)
			if countSolutions(rng, testBoard, size, 2) != 1 {
				puzzle[r][c] = backup
			}
		}

		removed := countRemovedCells(puzzle)
		if removed >= minRemoved {
			log.Infof(
				"Puzzle generated after %d attempt(s), removed=%d (min=%d)",
				retry, removed, minRemoved,
			)
			return puzzle
		}

		log.Infof(
			"Removal too low (%d < %d), retrying puzzle generation (%d/%d)",
			removed, minRemoved, retry, maxRetries,
		)
	}

	log.Infof(
		"WARNING: Max retries reached, returning best-effort puzzle",
	)

	// fallback: return last generated puzzle
	return puzzle
}

func countSolutions(rng *rand.Rand, board [][]int, size, limit int) int {
	count := 0
	var solve func() bool

	solve = func() bool {
		// Find the empty cell with the fewest candidates (MRV)
		row, col, candidates, deadEnd := findMRVCell(board, size)
		if deadEnd {
			return false
		}
		if row == -1 {
			// Solved one solution
			count++
			return count >= limit
		}

		// Randomize candidates to reduce bias (optional)
		rng.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})

		for _, num := range candidates {
			board[row][col] = num
			if solve() {
				board[row][col] = 0
				return true // early exit if over limit
			}
			board[row][col] = 0
		}

		return false
	}

	solve()

	if count > limit {
		return limit + 1
	}
	return count
}

func deepCopy(src [][]int) [][]int {
	dst := make([][]int, len(src))
	for i := range src {
		dst[i] = append([]int(nil), src[i]...)
	}
	return dst
}

func printSudoku(board [][]int) {
	size := len(board)
	block := intSqrt(size)

	for r := 0; r < size; r++ {
		line := ""
		if r > 0 && r%block == 0 {
			log.Debug("\n") // block separator
		}
		for c := 0; c < size; c++ {
			if c > 0 && c%block == 0 {
				line += " |"
			}
			if board[r][c] == 0 {
				line += " . "
			} else {
				line += fmt.Sprintf("%2d ", board[r][c])
			}
		}
		log.Debug(line)
	}
	log.Debug("\n")
}

func countRemovedCells(board [][]int) int {
	count := 0
	for r := range board {
		for c := range board[r] {
			if board[r][c] == 0 {
				count++
			}
		}
	}
	return count
}

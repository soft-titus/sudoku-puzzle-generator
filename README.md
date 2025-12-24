# Sudoku Puzzle Generator

Worker to generate Sudoku puzzles (both solution and the puzzle itself).

This worker is intended to run inside a Kubernetes cluster with KEDA.

It will:
- Pull messages from Kafka
- Generate Sudoku solution and puzzle
- Store the result in MongoDB for further processing

---

## Features
- Can generate different puzzle sizes (4 | 9 | 16) with multiple difficulty
  levels (EASY | MEDIUM | HARD)
- Uses MRV (Minimum Remaining Values) algorithm for fast and efficient
  puzzle generation

---

## Requirements
- Go (tested with version 1.25.5)
- golangci-lint (tested with version 2.7.2)
- Docker & Docker Compose (for local development)

---

## Setup

1. Clone the repository:

```bash
git clone https://github.com/soft-titus/sudoku-puzzle-generator.git
cd sudoku-puzzle-generator
```

2. Install Go dependencies:

```bash
go mod tidy
```

3. Install Git pre-commit hooks:

```bash
cp hooks/pre-commit .git/hooks/
```

4. Run the application locally with Docker:

```bash
docker compose build
docker compose up -d
```

5. Feed data to MongoDB:

```bash
docker compose run --rm ingester \
  --puzzle-id pz-001 --puzzle-size 9 --level HARD --status GENERATING_PUZZLE
```

| Parameter   | Required | Notes |
|-------------|----------|-------|
| puzzle-id   | yes      | Any string value |
| puzzle-size | no       | Default 9, valid: 4 / 9 / 16 |
| level       | no       | Default "EASY", valid: "EASY" / "MEDIUM" / "HARD" |
| status      | no       | Default "GENERATING_PUZZLE", must be "GENERATING_PUZZLE" |

6. Send Kafka messages:

```bash
docker compose run --rm producer --puzzle-id pz-001 --retry-count 0
```

| Parameter   | Required | Notes |
|-------------|----------|-------|
| puzzle-id   | yes      | Must exist in MongoDB or worker will fail |
| retry-count | no       | Default 0, must be less than TASK_MAX_RETRIES in `.env` |

---

## Check Worker Logs

```bash
docker compose logs consumer -f
```

---

## Code Formatting and Linting

```bash
go fmt ./...
golangci-lint run ./...
```

---

## GitHub Actions

- `ci.yaml` : Runs linting and tests on all branches and PRs
- `build-and-publish-branch-docker-image.yaml` : Builds Docker images for
  branches
- `build-and-publish-production-docker-image.yaml` : Builds production
  images on `main`

---

### Branch Builds

Branch-specific Docker images are built with timestamped tags.  

Example: `1.0.0-dev-1762431`

---

### Production Builds

Merges to `main` trigger a production Docker image build.  
Versioning follows semantic versioning based on commit messages:

- `BREAKING CHANGE:` : major version bump
- `feat:` : minor version bump
- `fix:` : patch version bump

Example: `1.0.0`

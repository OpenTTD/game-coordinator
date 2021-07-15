# Game Coordinator

[![GitHub License](https://img.shields.io/github/license/OpenTTD/game-coordinator)](https://github.com/OpenTTD/game-coordinator/blob/main/LICENSE)
[![GitHub Tag](https://img.shields.io/github/v/tag/OpenTTD/game-coordinator?include_prereleases&label=stable)](https://github.com/OpenTTD/game-coordinator/releases)
[![GitHub commits since latest release](https://img.shields.io/github/commits-since/OpenTTD/game-coordinator/latest/main)](https://github.com/OpenTTD/game-coordinator/commits/main)

[![GitHub Workflow Status (Testing)](https://img.shields.io/github/workflow/status/OpenTTD/game-coordinator/Testing/main?label=main)](https://github.com/OpenTTD/game-coordinator/actions?query=workflow%3ATesting)
[![GitHub Workflow Status (Publish Image)](https://img.shields.io/github/workflow/status/OpenTTD/game-coordinator/Publish%20image?label=publish)](https://github.com/OpenTTD/game-coordinator/actions?query=workflow%3A%22Publish+image%22)
[![GitHub Workflow Status (Deployments)](https://img.shields.io/github/workflow/status/OpenTTD/game-coordinator/Deployment?label=deployment)](https://github.com/OpenTTD/game-coordinator/actions?query=workflow%3A%22Deployment%22)

[![GitHub deployments (Staging)](https://img.shields.io/github/deployments/OpenTTD/game-coordinator/staging?label=staging)](https://github.com/OpenTTD/game-coordinator/deployments)
[![GitHub deployments (Production)](https://img.shields.io/github/deployments/OpenTTD/game-coordinator/production?label=production)](https://github.com/OpenTTD/game-coordinator/deployments)

This is the Game Coordinator / STUN server to assist in OpenTTD players to play together.

## Development

This server is written in Python 3.8 with aiohttp, and makes strong use of asyncio.

### Running a local server

#### Dependencies

- Python3.8 or higher.
- Redis

#### Preparing your venv

To start it, you are advised to first create a virtualenv:

```bash
python3 -m venv .env
.env/bin/pip install -r requirements.txt
```

#### Preparing redis

Make sure you have a local redis running. For example via Docker:

```bash
docker run --rm -p 6379:6379 redis
```

#### Starting a local server (Game Coordinator)

You can start the Game Coordinator server by running:

```bash
.env/bin/python -m game_coordinator --db redis --app coordinator --shared-secret test --web-port 12345
```

#### Starting a local server (STUN Server)

You can start the STUN server by running:

```bash
.env/bin/python -m game_coordinator --db redis --app stun --web-port 12346
```

### Running via docker (Game Coordinator)

```bash
docker build -t openttd/game-coordinator:local .
docker run --rm -p 127.0.0.1:3976:3976 openttd/game-coordinator:local
```

### Running via docker (STUN server)

```bash
docker build -t openttd/game-coordinator:local .
docker run --rm -p 127.0.0.1:3975:3975 openttd/game-coordinator:local --app stun --bind 0.0.0.0 --db redis --redis-url redis://redis
```

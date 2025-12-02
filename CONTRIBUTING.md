# Contributing

Thanks for considering a contribution! This project is a compact demo, so contributions focus on clarity and stability.

## Getting started
- Install dependencies: `make venv && make install`
- Start services: `make up`
- Run quick checks: `make test` (lightweight pytest suite)

## Guidelines
- Keep README and docs accurate with the Make targets and CLI options.
- Favor small, focused pull requests with concise descriptions.
- Add or update documentation when modifying workflows or commands.
- Follow existing code style; avoid wrapping imports in try/except.

## Reporting issues
- Include the command you ran, expected vs. actual behavior, and environment details (OS, Python version).
- For Kafka/Ignite connectivity issues, share relevant `docker compose logs` snippets.

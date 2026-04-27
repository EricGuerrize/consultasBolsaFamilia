#!/bin/bash
# Sobe API Python + frontend dev em paralelo

ROOT="$(cd "$(dirname "$0")" && pwd)"

# Libera porta 8000 se já estiver em uso
lsof -ti:8000 | xargs kill -9 2>/dev/null

echo "▶ Iniciando API Python em localhost:8000..."
python "$ROOT/api/index.py" &
API_PID=$!

echo "▶ Iniciando frontend em localhost:5173..."
cd "$ROOT/frontend" && npm run dev &
FRONT_PID=$!

trap "kill $API_PID $FRONT_PID 2>/dev/null; echo '✓ Encerrado.'" INT TERM

echo ""
echo "  API:      http://localhost:8000"
echo "  Frontend: http://localhost:5173"
echo ""
echo "  Ctrl+C para encerrar tudo."
echo ""

wait

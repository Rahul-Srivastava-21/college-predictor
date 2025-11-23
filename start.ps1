# College Predictor - Quick Start Script
# Run this script to start both backend and frontend servers

Write-Host "🎓 Starting College Predictor Application..." -ForegroundColor Cyan
Write-Host ""

# Start Backend
Write-Host "📡 Starting Backend (FastAPI)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd 'D:\Major Project\college-predictor\app'; & 'D:\Major Project\college-predictor\ml-gpu\Scripts\Activate.ps1'; uvicorn main:app --reload"

Start-Sleep -Seconds 3

# Start Frontend
Write-Host "⚛️  Starting Frontend (React + Vite)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd 'D:\Major Project\college-predictor\frontend'; npm run dev"

Write-Host ""
Write-Host "✅ Both servers are starting..." -ForegroundColor Green
Write-Host ""
Write-Host "Backend API: http://localhost:8000" -ForegroundColor Cyan
Write-Host "Frontend UI: http://localhost:5173" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press any key to stop all servers and exit..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

# Cleanup (stop all uvicorn and vite processes)
Get-Process | Where-Object {$_.Name -eq "uvicorn" -or $_.ProcessName -eq "node"} | Stop-Process -Force
Write-Host "Servers stopped." -ForegroundColor Red
